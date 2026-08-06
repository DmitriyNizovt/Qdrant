import time
import uuid
from settings.logger import setup_logger
from typing import List
from datetime import datetime, timezone

import numpy as np
from qdrant_client import QdrantClient, models
from postgres_logger import insert_log

from settings import constance
from clickhouse import (
    ensure_state_table,
    get_last_sync_time,
    update_last_sync_time,
    get_updated_users,
    get_users_with_clusters,
    fetch_clusters_for_users,
    fetch_all_clusters,
    get_max_updated_at,
)
# Инициализация логгера (файл + консоль)
logger = setup_logger(name="qdrant_scheduler")

class QdrantUploader:
    """
    Класс для синхронизации данных из ClickHouse в Qdrant.
    Поддерживает историческую заливку (все данные) и инкрементальную (только изменения).
    """

    def __init__(
        self,
        collection_name: str = constance.COLLECTION_NAME,
        vector_size: int = constance.VECTOR_SIZE,
        batch_size: int = constance.BATCH_SIZE,
        ch_block_size: int = constance.CH_BLOCK_SIZE,
        retries: int = constance.RETRIES,
        batch_users: int = constance.BATCH_USERS,
    ):
        """
        Инициализация загрузчика.

        Параметры:
            collection_name: имя коллекции в Qdrant
            vector_size: размерность векторов
            batch_size: количество точек в одном батче для Qdrant
            ch_block_size: количество строк, читаемых из ClickHouse за один раз
            retries: число повторных попыток при ошибках сети/сервера
            batch_users: размер группы пользователей при массовом удалении
        """
        self.collection_name = collection_name
        self.vector_size = vector_size
        self.batch_size = batch_size
        self.ch_block_size = ch_block_size
        self.retries = retries
        self.batch_users = batch_users

        # Инициализация клиента Qdrant (по умолчанию REST, можно переключить на gRPC)
        self.qdrant_client = QdrantClient(url=constance.QDRANT_URL, timeout=120)

        # Проверяем/создаём таблицу состояния синхронизации
        ensure_state_table()

    def ensure_collection(self, force_recreate: bool = False) -> None:
        """
        Создаёт коллекцию в Qdrant, если её нет.
        Если force_recreate=True, удаляет существующую и создаёт заново.

        Это используется при исторической заливке, чтобы гарантировать чистоту данных.
        При создании коллекции задаются:
            - размер вектора (384),
            - метрика косинусного расстояния,
            - параметры HNSW для баланса скорости/точности,
            - индексы для полей payload (user_id, cluster_id, updated_at).
        """
        start = time.perf_counter()
        exists = self.qdrant_client.collection_exists(self.collection_name)
        if force_recreate and exists:
            self.qdrant_client.delete_collection(self.collection_name)
            logger.info(f"Коллекция {self.collection_name} удалена")
            exists = False
        if not exists:
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_size,
                    distance=models.Distance.COSINE,
                    # m=16 — стандартное значение для баланса скорости вставки и поиска. Больше m -> быстрее поиск, но медленнее вставка и больше память.
                    # ef_construct=100 — стандартное, влияет на качество построения графа. Больше -> точнее поиск, но медленнее построение индекса.
                    hnsw_config=models.HnswConfigDiff(m=16, ef_construct=100)
                ),
                on_disk_payload=True,
            )
            # Индексы для ускорения фильтрации по payload (может понадобиться)
            self.qdrant_client.create_payload_index(
                self.collection_name,
                "user_id",
                models.PayloadSchemaType.KEYWORD
            )

            self.qdrant_client.create_payload_index(
                self.collection_name,
                "cluster_id",
                models.PayloadSchemaType.INTEGER
            )

            self.qdrant_client.create_payload_index(
                self.collection_name,
                "updated_at",
                models.PayloadSchemaType.DATETIME
            )

        elapsed = time.perf_counter() - start
        logger.info(f"Коллекция {self.collection_name} создана за {elapsed:.2f} сек")

    @staticmethod
    def _make_point(user_id: str, cluster_id: int, centroid: np.ndarray, updated_at: datetime) -> models.PointStruct:
        """
        Формирует объект PointStruct для Qdrant из сырых данных.

        ID точки генерируется как стабильный UUID на основе user_id и cluster_id.
        Это гарантирует, что при повторной заливке той же пары точка будет обновлена,
        а не создана дубликат.
        """
        point_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{user_id}_{cluster_id}")
        return models.PointStruct(
            id=str(point_id),
            vector=centroid.tolist(),
            payload={
                "user_id": user_id,
                "cluster_id": cluster_id,
                "updated_at": updated_at.isoformat(),
            }
        )

    def _upload_points_streaming(self, rows_generator, max_points: int = None) -> int:
        """
        Основной метод потоковой загрузки точек из генератора строк в Qdrant.

        Строки ожидаются в формате (user_id, cluster_id, centroid, updated_at).
        Загрузка выполняется батчами (размер задаётся в self.batch_size).

        Для отладки и профилирования ведётся детальная статистика времени по этапам:
            - получение из генератора,
            - преобразование centroid,
            - генерация UUID,
            - создание PointStruct,
            - отправка в Qdrant (Upsert).
        """
        start_total = time.perf_counter()

        batch_buffer = []
        total = 0

        # Детальная статистика для профилирования
        t_generator = 0.0
        t_numpy = 0.0
        t_point = 0.0
        t_append = 0.0
        t_upsert = 0.0

        while True:
            # Получение строки из auction_vectors (ожидание данных из ClickHouse)
            t0 = time.perf_counter()
            try:
                row = next(rows_generator)
            except StopIteration:
                break
            t_generator += time.perf_counter() - t0

            user_id, cluster_id, centroid, updated_at = row

            # Преобразование в numpy-массив для удобства (но потом снова в list)
            t0 = time.perf_counter()
            centroid = np.array(centroid)
            t_numpy += time.perf_counter() - t0

            # Создание точки через _make_point (он сам сделает tolist() и заполнит payload)
            t0 = time.perf_counter()
            point = self._make_point(user_id, cluster_id, centroid, updated_at)
            t_point += time.perf_counter() - t0

            # Добавление в буфер
            t0 = time.perf_counter()
            batch_buffer.append(point)
            t_append += time.perf_counter() - t0

            total += 1

            # Если буфер полон — отправляем батч
            if len(batch_buffer) >= self.batch_size:
                t0 = time.perf_counter()
                self._upsert_with_retry(batch_buffer)
                t_upsert += time.perf_counter() - t0

                batch_buffer.clear()

                if total % 5000 == 0:
                    logger.info(f"Загружено {total} точек...")

            if max_points is not None and total >= max_points:
                break

        # Отправка остатка
        if batch_buffer:
            t0 = time.perf_counter()
            self._upsert_with_retry(batch_buffer)
            t_upsert += time.perf_counter() - t0

        elapsed = time.perf_counter() - start_total

        # Вывод детальной статистики
        logger.info(
            "\n================ ПРОФИЛИРОВАНИЕ ================\n"
            f"Всего точек               : {total}\n"
            f"Общее время               : {elapsed:.2f} сек\n"
            f"\n"
            f"Получение из генератора   : {t_generator:.2f} сек\n"
            f"np.array                  : {t_numpy:.2f} сек\n"
            f"PointStruct               : {t_point:.2f} сек\n"
            f"append                    : {t_append:.2f} сек\n"
            f"Upsert                    : {t_upsert:.2f} сек\n"
            f"\n"
            f"Неучтенное время          : "
            f"{elapsed - (t_generator + t_numpy + t_point + t_append + t_upsert):.2f} сек\n"
            "================================================"
        )

        return total

    def _upsert_with_retry(self, points: List[models.PointStruct]) -> None:
        """
        Отправляет батч точек в Qdrant с повторными попытками при ошибках.

        При сбое выполняет экспоненциальную задержку между попытками.
        """
        for attempt in range(self.retries):
            try:
                self.qdrant_client.upsert(
                    collection_name=self.collection_name,
                    points=points,
                    wait=True
                )
                return
            except Exception as e:
                if attempt == self.retries - 1:
                    logger.error(f"Не удалось загрузить батч после {self.retries} попыток: {e}")
                    raise
                wait = 2 ** attempt
                logger.warning(f"Ошибка заливки, повтор {attempt+2}/{self.retries} через {wait} сек: {e}")
                time.sleep(wait)

    def _delete_users_batch(self, user_ids: List[str]) -> None:
        """
        Массовое удаление всех точек для списка пользователей из Qdrant.

        Использует фильтр MatchAny по user_id, что позволяет удалить все кластеры
        пользователей за один запрос (вместо отдельных запросов на каждого пользователя).
        """
        if not user_ids:
            return
        start = time.perf_counter()
        filter_cond = models.Filter(
            must=[models.FieldCondition(
                key="user_id",
                match=models.MatchAny(any=user_ids)
            )]
        )
        self.qdrant_client.delete(
            collection_name=self.collection_name,
            points_selector=filter_cond
        )
        elapsed = time.perf_counter() - start
        logger.debug(f"Удалены точки для {len(user_ids)} пользователей за {elapsed:.2f} сек")

    def historical_upload(self) -> int:
        start_time = datetime.now(timezone.utc)
        logger.info("Начинаем историческую заливку...")
        data_json = {
            "operation": "historical",
            "start_time": start_time.isoformat(),
            "total_points": 0,
            "load_time": 0,
            "total_time": 0,
            "max_updated": None,
            "status": "started"
        }
        try:
            self.ensure_collection(force_recreate=True)
            start_load = time.perf_counter()
            rows_stream = fetch_all_clusters(block_size=self.ch_block_size)
            total = self._upload_points_streaming(rows_stream)
            load_time = time.perf_counter() - start_load

            max_updated = get_max_updated_at()
            if max_updated:
                update_last_sync_time(self.collection_name, max_updated)

            end_time = datetime.now(timezone.utc)
            data_json.update({
                "total_points": total,
                "load_time": load_time,
                "total_time": (end_time - start_time).total_seconds(),
                "max_updated": max_updated.isoformat() if max_updated else None,
                "status": "success"
            })
            insert_log(start_time, end_time, data_json)
            logger.info(f"Историческая заливка завершена. Загружено {total} точек.")
            return total
        except Exception as e:
            end_time = datetime.now(timezone.utc)
            data_json["status"] = "failed"
            insert_log(start_time, end_time, data_json, errors=str(e))
            logger.error(f"Историческая заливка завершилась ошибкой: {e}")
            raise

    def incremental_upload(self) -> int:
        """
        Инкрементальная синхронизация: обновляет только изменившихся пользователей.

        Логика:
        1. Получить last_sync_time из таблицы состояния.
        2. Найти всех пользователей, у которых есть записи с updated_at > last_sync_time.
        3. Для каждого батча пользователей:
           - Проверить, есть ли у них кластеры в auction_vectors.
           - Если кластеров нет – удалить пользователя из Qdrant.
           - Если кластеры есть – удалить старые точки и залить актуальные.
        4. Обновить last_sync_time до максимального updated_at среди обработанных.
        5. Записать детали выполнения в таблицу логов (PostgreSQL).

        Возвращает количество загруженных точек.
        """
        # ----- Начало инкремента. Фиксируем время старта -----
        start_time = datetime.now(timezone.utc)

        # ----- 1. Получаем время последней успешной синхронизации -----
        last_sync = get_last_sync_time(self.collection_name)

        # Подготавливаем словарь для логирования в БД (заполнится по ходу работы)
        data_json = {
            "operation": "incremental",
            "start_time": start_time.isoformat(),
            "last_sync": last_sync.isoformat() if last_sync else None,
            "users_found": 0,
            "users_processed": 0,
            "total_points": 0,
            "find_time": 0,
            "delete_time": 0,
            "load_time": 0,
            "update_time": 0,
            "total_time": 0,
            "status": "started"
        }

        try:
            # Если last_sync нет → значит это первый запуск → переключаемся на историческую заливку
            if last_sync is None:
                logger.info("Нет сохранённого last_sync_time, выполняем историческую заливку...")
                return self.historical_upload()

            logger.info(f"Инкрементальная заливка с last_sync_time = {last_sync}")

            # ----- 2. Находим всех пользователей с обновлениями (updated_at > last_sync) -----
            start_find = time.perf_counter()
            updated_users = get_updated_users(last_sync)  # возвращает список (user_id, max_updated_at)
            find_time = time.perf_counter() - start_find
            data_json["find_time"] = find_time

            # Если таких пользователей нет – завершаем без действий
            if not updated_users:
                logger.info("Нет пользователей с обновлениями, пропускаем")
                data_json["status"] = "success"
                data_json["users_found"] = 0
                data_json["total_time"] = (datetime.now(timezone.utc) - start_time).total_seconds()
                insert_log(start_time, datetime.now(timezone.utc), data_json)
                return 0

            user_ids = [uid for uid, _ in updated_users]
            data_json["users_found"] = len(user_ids)
            logger.info(f"Найдено {len(user_ids)} пользователей за {find_time:.2f} сек")

            # ----- 3. Инициализация счётчиков и переменных -----
            total_uploaded = 0          # всего загружено точек
            total_delete_time = 0       # суммарное время на удаление
            total_load_time = 0         # суммарное время на загрузку
            # Максимальный updated_at среди всех обработанных пользователей (нужен для обновления last_sync_time)
            max_updated_from_all_users = max(updated_at for _, updated_at in updated_users)

            # Разбиваем пользователей на батчи (чтобы не перегружать Qdrant и контролировать память)
            for i in range(0, len(user_ids), self.batch_users):
                batch = user_ids[i:i + self.batch_users]

                start_load = time.perf_counter()
                delete_elapsed_batch = 0.0

                # ----- 4. Определяем, у кого из батча есть кластеры -----
                # Это нужно, чтобы отделить пользователей, которые полностью потеряли кластеры
                returned_users = set(get_users_with_clusters(batch))
                deleted_users = set(batch) - returned_users

                # ----- 5. Удаляем пользователей без кластеров -----
                # Если у пользователя больше нет кластеров в ClickHouse, удаляем его из Qdrant,
                # чтобы не хранить мёртвые точки.
                if deleted_users:
                    start_delete = time.perf_counter()
                    self._delete_users_batch(list(deleted_users))
                    elapsed_delete = time.perf_counter() - start_delete
                    delete_elapsed_batch += elapsed_delete
                    total_delete_time += elapsed_delete
                    logger.debug(f"Удалены точки для {len(deleted_users)} пользователей без кластеров")

                # ----- 6. Обновляем пользователей с кластерами -----
                # Для этих пользователей удаляем старые точки и заливаем новые (актуальные кластеры)
                if returned_users:
                    # Удаляем старые точки этого пользователя из Qdrant
                    start_delete = time.perf_counter()
                    self._delete_users_batch(list(returned_users))
                    elapsed_delete = time.perf_counter() - start_delete
                    delete_elapsed_batch += elapsed_delete
                    total_delete_time += elapsed_delete

                    # Загружаем актуальные кластеры из ClickHouse
                    rows_stream = fetch_clusters_for_users(
                        list(returned_users),
                        block_size=self.ch_block_size
                    )
                    uploaded = self._upload_points_streaming(rows_stream)
                    total_uploaded += uploaded
                else:
                    uploaded = 0

                # Замеряем время обработки батча
                load_elapsed = time.perf_counter() - start_load
                total_load_time += load_elapsed

                logger.info(
                    f"Батч {i // self.batch_users + 1}: "
                    f"обработано пользователей {len(batch)}, "
                    f"обновлено кластеров пользователей {len(returned_users)}, "
                    f"полностью удалено пользователей {len(deleted_users)}, "
                    f"загружено точек {uploaded}, "
                    f"удаление {delete_elapsed_batch:.2f} сек, "
                    f"общее время {load_elapsed:.2f} сек"
                )

            # ----- 7. Обновляем last_sync_time на основе максимального updated_at среди всех обработанных -----
            # Это гарантирует, что даже пользователи без кластеров не будут обрабатываться снова.
            start_update = time.perf_counter()
            if max_updated_from_all_users > last_sync:
                update_last_sync_time(self.collection_name, max_updated_from_all_users)
            else:
                # Теоретически такого не должно быть, но если произойдёт – оставляем без изменений
                logger.warning(
                    f"max_updated_from_all_users ({max_updated_from_all_users}) не превышает last_sync ({last_sync})")
            update_time = time.perf_counter() - start_update

            # ----- 8. Фиксируем завершение и логируем результат в БД -----
            end_time = datetime.now(timezone.utc)
            data_json.update({
                "users_processed": len(user_ids),
                "total_points": total_uploaded,
                "delete_time": total_delete_time,
                "load_time": total_load_time,
                "update_time": update_time,
                "total_time": (end_time - start_time).total_seconds(),
                "max_updated": max_updated_from_all_users.isoformat() if max_updated_from_all_users else None,
                "status": "success"
            })
            insert_log(start_time, end_time, data_json)

            logger.info(
                f"Инкремент завершён. Обработано {len(user_ids)} пользователей, "
                f"загружено {total_uploaded} точек. "
                f"Время: поиск {find_time:.2f} сек, удаление {total_delete_time:.2f} сек, "
                f"загрузка {total_load_time:.2f} сек, обновление состояния {update_time:.2f} сек, "
                f"итого {(end_time - start_time).total_seconds():.2f} сек."
            )
            return total_uploaded

        # ----- 9. Обработка ошибок: логируем сбой и перевыбрасываем исключение -----
        except Exception as e:
            end_time = datetime.now(timezone.utc)
            data_json["status"] = "failed"
            data_json["total_time"] = (end_time - start_time).total_seconds()
            insert_log(start_time, end_time, data_json, errors=str(e))
            logger.error(f"Инкрементальная заливка завершилась ошибкой: {e}", exc_info=True)
            raise

    def sync(self) -> int:
        """
        Главный метод синхронизации: определяет, нужна ли историческая или инкрементальная заливка.

        Вызывается в раннере. Логика:
            - если коллекции нет → историческая заливка,
            - если коллекция существует, но пуста → историческая заливка,
            - иначе → инкрементальная заливка.

        Возвращает количество загруженных/обновлённых точек.
        """
        start = time.perf_counter()
        exists = self.qdrant_client.collection_exists(self.collection_name)
        if not exists:
            total = self.historical_upload()
        else:
            # Проверяем, есть ли хотя бы одна точка
            points, _ = self.qdrant_client.scroll(
                collection_name=self.collection_name,
                limit=1,
                with_vectors=False,
                with_payload=False
            )
            if not points:
                total = self.historical_upload()
            else:
                total = self.incremental_upload()
        elapsed = time.perf_counter() - start
        logger.info(f"Синхронизация завершена, общее время {elapsed:.2f} сек, загружено/обновлено {total} точек.")
        return total
"""
Скрипт для потоковой загрузки данных из ClickHouse в Qdrant.
Использует параллельную заливку батчами, gRPC (если доступен) и повторные попытки при ошибках.
"""

import time
import logging
import uuid
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from clickhouse_driver import Client
from qdrant_client import QdrantClient, models

# ---------- НАСТРОЙКИ ----------
# Параметры подключения к ClickHouse (источник данных)
CH_HOST = '91.202.205.16'
CH_PORT = 5542
CH_DB = 'look_a_like'
CH_USER = 'look_user'
CH_PASS = 'iusdboinc239sckqdaksHFeoi3rs'

# Параметры подключения к Qdrant (векторная база данных)
QDRANT_URL = 'http://localhost:6333'
QDRANT_GRPC_PORT = 6334               # Порт для gRPC (быстрее, чем REST)
COLLECTION_NAME = 'user_clusters'     # Имя коллекции в Qdrant
VECTOR_SIZE = 384                     # Размерность вектора (соответствует модели all-MiniLM-L12-v2)

# Настройки производительности
PARALLEL_WORKERS = 4                  # Количество потоков для параллельной заливки
BATCH_SIZE = 2000                     # Количество точек в одном батче для Qdrant
CH_BLOCK_SIZE = 10000                 # Сколько строк читать из ClickHouse за один раз
RETRIES = 3                           # Сколько раз повторять попытку при ошибке
MAX_POINTS = 100000                   # Максимальное число точек для загрузки (None = все данные)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ---------- ПОДКЛЮЧЕНИЕ К CLICKHOUSE ----------
def get_ch_client() -> Client:
    """
    Создаёт и возвращает клиент для подключения к ClickHouse.
    Все параметры подключения берутся из глобальных констант.
    """
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASS,
        connect_timeout=10,
        send_receive_timeout=30,
    )


# ---------- ГЕНЕРАТОР СТРОК ИЗ CLICKHOUSE ----------
def fetch_rows_from_ch(limit: int = None):
    """
    Генератор, который читает данные из ClickHouse порциями (батчами) и выдаёт по одной строке.

    Аргументы:
        limit (int, optional): Максимальное количество строк, которое нужно прочитать.
                                Если None, читаются все данные.
    """
    ch = get_ch_client()
    # Запрос выбирает уникальные комбинации (user_id, cluster_id) с их центроидом.
    # Группировка нужна, чтобы избежать дублирования, если в таблице есть несколько записей для одного кластера.
    query = """
        SELECT user_id, cluster_id, centroid
        FROM auction_vectors
        GROUP BY user_id, cluster_id, centroid
        ORDER BY user_id, cluster_id
    """
    if limit is not None:
        query += f" LIMIT {limit}"

    logger.info("Начинаем потоковую загрузку из ClickHouse...")
    # execute_iter возвращает итератор, который выдаёт строки по мере поступления с сервера.
    rows_stream = ch.execute_iter(query, settings={'max_block_size': CH_BLOCK_SIZE})
    total = 0
    for row in rows_stream:
        yield row
        total += 1
        # Логируем прогресс каждые 100 000 строк, чтобы не засорять лог.
        if total % 100000 == 0:
            logger.info(f"Прочитано {total} строк из ClickHouse")
    ch.disconnect_connection()
    logger.info(f"Всего прочитано {total} строк")


# ---------- ПРЕОБРАЗОВАНИЕ СТРОКИ В ТОЧКУ QDRANT ----------
def make_point(row):
    """
    Преобразует строку из ClickHouse в объект PointStruct для Qdrant.

    Аргументы:
        row (tuple): Кортеж (user_id, cluster_id, centroid), где centroid — список чисел.
    """
    user_id, cluster_id, centroid = row
    # Преобразуем centroid из списка в numpy-массив.
    centroid = np.array(centroid)
    # Генерируем стабильный UUID на основе user_id и cluster_id,
    # чтобы одна и та же пара всегда давала одинаковый ID (это важно для обновлений).
    point_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{user_id}_{cluster_id}")
    return models.PointStruct(
        id=str(point_id),
        vector=centroid.tolist(),
        payload={
            "user_id": user_id,
            "cluster_id": int(cluster_id),
        }
    )


# ---------- ФУНКЦИЯ ЗАЛИВКИ ОДНОГО БАТЧА (С ПОВТОРАМИ) ----------
def upload_batch(client, batch_points, retries=RETRIES):
    """
    Отправляет один батч точек в Qdrant с повторными попытками при ошибках.

    Аргументы:
        client (QdrantClient): Клиент Qdrant.
        batch_points (list): Список объектов PointStruct.
        retries (int): Количество попыток.
    """
    for attempt in range(retries):
        try:
            # wait=False означает, что мы не ждём завершения индексации,
            # это значительно ускоряет заливку, но поиск может быть не сразу доступен.
            client.upsert(
                collection_name=COLLECTION_NAME,
                points=batch_points,
                wait=False
            )
            return
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Не удалось загрузить батч после {retries} попыток: {e}")
                raise
            # Экспоненциальная задержка между попытками (2, 4, 8 секунд).
            wait = 2 ** attempt
            logger.warning(f"Ошибка заливки, повтор {attempt+2}/{retries} через {wait} сек: {e}")
            time.sleep(wait)


# ---------- ПАРАЛЛЕЛЬНАЯ ЗАЛИВКА ----------
def upload_parallel(rows_generator, client, batch_size=BATCH_SIZE, workers=PARALLEL_WORKERS):
    """
    Параллельно заливает точки из генератора rows_generator в Qdrant.

    Аргументы:
        rows_generator (generator): Генератор строк из ClickHouse.
        client (QdrantClient): Клиент Qdrant.
        batch_size (int): Размер батча точек.
        workers (int): Количество потоков.
    """
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = []
    total_points = 0
    batch_buffer = []

    for row in rows_generator:
        point = make_point(row)
        batch_buffer.append(point)
        total_points += 1

        # Когда буфер заполнен, отправляем батч в отдельный поток.
        if len(batch_buffer) >= batch_size:
            future = executor.submit(upload_batch, client, batch_buffer.copy())
            futures.append(future)
            batch_buffer.clear()

            # Если накопилось слишком много задач, дожидаемся завершения нескольких,
            # чтобы не перегружать память.
            if len(futures) >= workers * 2:
                for f in as_completed(futures[:workers]):
                    try:
                        f.result()
                    except Exception as e:
                        logger.error(f"Ошибка в задаче: {e}")
                        raise
                # Удаляем завершённые задачи из списка.
                futures = [f for f in futures if not f.done()]

    # Заливаем остаток точек, если он есть.
    if batch_buffer:
        executor.submit(upload_batch, client, batch_buffer.copy())

    # Ждём завершения всех оставшихся задач.
    for f in as_completed(futures):
        try:
            f.result()
        except Exception as e:
            logger.error(f"Ошибка в задаче: {e}")
            raise

    executor.shutdown(wait=True)
    logger.info(f"Всего загружено {total_points} точек")


# ---------- СОЗДАНИЕ КОЛЛЕКЦИИ ----------
def create_collection_if_not_exists(client):
    """
    Создаёт коллекцию в Qdrant, если она ещё не существует.

    Аргументы:
        client (QdrantClient): Клиент Qdrant.
    """
    if client.collection_exists(COLLECTION_NAME):
        logger.info(f"Коллекция {COLLECTION_NAME} уже существует")
        return

    logger.info(f"Создаём коллекцию {COLLECTION_NAME}...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=VECTOR_SIZE,
            distance=models.Distance.COSINE,
            hnsw_config=models.HnswConfigDiff(
                m=16,               # Количество соединений на узел
                ef_construct=100    # Количество ближайших соседей, которые рассматриваются на каждом шаге построения графа.
            )
        ),
        on_disk_payload=True,       # Хранить payload на диске, чтобы экономить ОЗУ.
    )
    # Индексы для быстрой фильтрации по user_id и cluster_id.
    client.create_payload_index(COLLECTION_NAME, "user_id", models.PayloadSchemaType.KEYWORD)
    client.create_payload_index(COLLECTION_NAME, "cluster_id", models.PayloadSchemaType.INTEGER)
    logger.info("Коллекция создана")


# ---------- ОСНОВНАЯ ФУНКЦИЯ ----------
def main():
    """
    Основная точка входа.
    Подключается к Qdrant, создаёт коллекцию (если нужно), читает данные из ClickHouse
    и запускает параллельную заливку.
    """
    # 1. Создаём клиент Qdrant с поддержкой gRPC для максимальной скорости.
    client = QdrantClient(
        url=QDRANT_URL,
        port=QDRANT_GRPC_PORT,
        prefer_grpc=True,
        timeout=120
    )

    # 2. Убеждаемся, что коллекция существует.
    create_collection_if_not_exists(client)

    # 3. Получаем генератор строк из ClickHouse.
    rows_gen = fetch_rows_from_ch(limit=MAX_POINTS)

    # 4. Запускаем параллельную заливку и замеряем время.
    start_time = time.time()
    upload_parallel(rows_gen, client)

    elapsed = time.time() - start_time
    logger.info(f"Общее время заливки: {elapsed:.2f} сек")
    logger.info("Проверьте дашборд: http://localhost:6333/dashboard#/collections")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
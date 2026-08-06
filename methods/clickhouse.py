from settings.logger import setup_logger
import time
from datetime import datetime, timedelta
from typing import List, Optional

from clickhouse_driver import Client
from settings import constance

# Инициализация логгера (файл + консоль)
logger = setup_logger(name="qdrant_scheduler")


def get_ch_client() -> Client:
    """Возвращает подключение к ClickHouse для Qdrant-сервиса."""
    return Client(
        host=constance.CH_HOST,
        port=constance.CH_PORT,
        database=constance.CH_DB,
        user=constance.CH_USER,
        password=constance.CH_PASS,
        connect_timeout=120,
        send_receive_timeout=600,
        sync_request_timeout=600
    )


def _log_query_time(query_name: str, start: float) -> None:
    """Логирует время выполнения запроса (для профилирования)."""
    elapsed = time.perf_counter() - start
    logger.info(f"Query '{query_name}' executed in {elapsed:.4f} sec")

def get_updated_users(last_sync: datetime) -> List[tuple]:
    """
    Возвращает список кортежей (user_id, updated_at) для пользователей,
    у которых есть записи с updated_at > last_sync.
    """
    ch = get_ch_client()
    query = """
        SELECT user_id, max(updated_at) AS max_updated
        FROM auction_vectors
        WHERE updated_at > %(last_sync)s
        GROUP BY user_id
    """
    rows = ch.execute(query, {'last_sync': last_sync})
    ch.disconnect_connection()
    return [(row[0], row[1]) for row in rows]


def fetch_clusters_for_users(user_ids: List[str], block_size: int = 50000):
    """
    Потоково (через execute_iter) загружает все кластеры (user_id, cluster_id, centroid, updated_at)
    для переданного списка пользователей.
    Используется в инкрементальном режиме для обновления конкретных пользователей.
    Размер блока (block_size) регулирует количество строк, передаваемых за один раз по сети,
    что позволяет балансировать между памятью и скоростью.
    """
    start = time.perf_counter()
    if not user_ids:
        return iter([])
    ch = get_ch_client()
    query = """
        SELECT user_id, cluster_id, centroid, updated_at
        FROM auction_vectors
        WHERE user_id IN %(user_ids)s
        GROUP BY user_id, cluster_id, centroid, updated_at
        ORDER BY user_id, cluster_id
    """
    result = ch.execute_iter(
        query,
        {'user_ids': tuple(user_ids)},
        settings={'max_block_size': block_size}
    )
    _log_query_time("fetch_clusters_for_users", start)
    return result


def fetch_all_clusters(limit: Optional[int] = None, block_size: int = 50000):
    """
    Потоково загружает все кластеры из auction_vectors для исторической заливки.
    Используется при первом запуске или при полной перезаливке.
    Группировка по (user_id, cluster_id, centroid, updated_at) гарантирует уникальность кластеров.
    Параметр limit позволяет ограничить количество точек (для тестирования, при выпуске в прод параметр убирается).
    """
    start = time.perf_counter()
    ch = get_ch_client()
    query = """
        SELECT user_id, cluster_id, centroid, updated_at
        FROM auction_vectors
        GROUP BY user_id, cluster_id, centroid, updated_at
        ORDER BY user_id, cluster_id
    """
    if limit is not None:
        query += f" LIMIT {limit}"
    result = ch.execute_iter(query, settings={'max_block_size': block_size})
    _log_query_time("fetch_all_clusters", start)
    return result


def get_max_updated_at() -> Optional[datetime]:
    """
    Возвращает максимальное значение updated_at из таблицы auction_vectors.
    Используется при исторической заливке для установки last_sync_time.
    Это гарантирует, что после исторической заливки инкремент не будет обрабатывать уже загруженные данные.
    """
    start = time.perf_counter()
    ch = get_ch_client()
    row = ch.execute("SELECT max(updated_at) FROM auction_vectors")
    ch.disconnect_connection()
    _log_query_time("get_max_updated_at", start)
    if row and row[0][0] is not None:
        return row[0][0]
    return None

def get_users_with_clusters(user_ids: list[str]) -> set[str]:
    """
    Возвращает пользователей, у которых сейчас есть кластеры в auction_vectors.

    Используется перед удалением из Qdrant, чтобы отличить:
    - пользователь реально потерял все кластеры;
    - пользователь просто не попал в выборку из-за ошибки.
    """
    ch = get_ch_client()
    query = """
        SELECT DISTINCT user_id
        FROM auction_vectors
        WHERE user_id IN %(user_ids)s
    """
    rows = ch.execute(
        query,
        {"user_ids": user_ids}
    )
    return {
        row[0]
        for row in rows
    }
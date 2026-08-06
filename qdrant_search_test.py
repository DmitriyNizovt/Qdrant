"""
Скрипт для поиска похожих пользователей через Qdrant REST API.
Использует усреднение центроидов пользователя и поиск ближайших кластеров.
"""

import logging
import time
from collections import Counter
from typing import List, Optional
import requests
import numpy as np

# ---------- НАСТРОЙКИ ----------
QDRANT_URL = 'http://localhost:6333'
COLLECTION_NAME = 'user_clusters'

TOP_K = 10
SEARCH_LIMIT = 100
SCORE_THRESHOLD = 0.9

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ---------- ФУНКЦИИ ДЛЯ РАБОТЫ С REST API ----------
def scroll_points(user_id: str, limit: int = 1000, offset=None):
    """
    Получает точки пользователя через REST API с фильтром по user_id.
    """
    url = f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points/scroll"
    payload = {
        "filter": {
            "must": [{"key": "user_id", "match": {"value": user_id}}]
        },
        "limit": limit,
        "with_vector": True,
        "with_payload": True
    }
    if offset:
        payload["offset"] = offset

    response = requests.post(url, json=payload)
    response.raise_for_status()
    data = response.json()["result"]
    return data["points"], data.get("next_page_offset")


def get_user_vector_from_rest(user_id: str) -> Optional[np.ndarray]:
    """
    Получает усреднённый вектор пользователя через REST API.
    """
    all_points = []
    offset = None
    while True:
        points, offset = scroll_points(user_id, offset=offset)
        all_points.extend(points)
        if not offset:
            break

    if not all_points:
        return None

    vectors = [np.array(p["vector"]) for p in all_points]
    return np.mean(vectors, axis=0)


def search_similar_vectors(query_vector: np.ndarray, limit: int = SEARCH_LIMIT, threshold: float = SCORE_THRESHOLD):
    """
    Выполняет поиск ближайших векторов через REST API.
    """
    url = f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points/search"
    payload = {
        "vector": query_vector.tolist(),
        "limit": limit,
        "score_threshold": threshold,
        "with_payload": True
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]


# ---------- ОСНОВНАЯ ЛОГИКА ПОИСКА ----------
def find_similar_users(user_id: str, top_k: int = TOP_K) -> List[str]:
    """
    Находит топ похожих пользователей для заданного user_id через REST API.
    """
    # 1. Получаем вектор пользователя
    user_vector = get_user_vector_from_rest(user_id)
    if user_vector is None:
        logger.warning(f"Пользователь {user_id} не найден")
        return []

    # 2. Поиск ближайших кластеров
    search_result = search_similar_vectors(user_vector, limit=SEARCH_LIMIT, threshold=SCORE_THRESHOLD)

    if not search_result:
        logger.info(f"Не найдено кластеров, похожих на пользователя {user_id}")
        return []

    # 3. Агрегация по user_id
    counter = Counter()
    for point in search_result:
        uid = point["payload"]["user_id"]
        if uid != user_id:
            counter[uid] += 1

    # 4. Топ пользователей
    return [uid for uid, _ in counter.most_common(top_k)]


# ---------- ДЕМОНСТРАЦИЯ ----------
def demo_search(user_id: str):
    logger.info(f"🔍 Поиск похожих пользователей для: {user_id}")

    start_time = time.time()
    similar_users = find_similar_users(user_id)
    elapsed = time.time() - start_time

    if not similar_users:
        logger.info("❌ Похожие пользователи не найдены.")
        return

    logger.info(f"✅ Найдено {len(similar_users)} пользователей за {elapsed:.3f} сек:")
    for i, uid in enumerate(similar_users, 1):
        logger.info(f"  {i}. {uid}")


if __name__ == "__main__":
    TEST_USER_ID = "13931592217100659274"
    demo_search(TEST_USER_ID)
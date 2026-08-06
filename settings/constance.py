"""
Конфигурация Qdrant-сервиса.
Все параметры берутся из переменных окружения (.env).
"""

import os
from dotenv import load_dotenv

# Загружаем переменные из .env (файл должен лежать в корне проекта)
load_dotenv()

# ---------- ClickHouse (источник данных: auction_vectors) ----------
CH_HOST = os.getenv('CH_HOST', None)
CH_PORT = int(os.getenv('CH_PORT', 0))
CH_DB = os.getenv('CH_DB', None)
CH_USER = os.getenv('CH_USER', None)
CH_PASS = os.getenv('CH_PASS', None)

# ---------- Qdrant ----------
QDRANT_HOST=os.getenv('QDRANT_HOST', None)
QDRANT_PORT = int(os.getenv('QDRANT_PORT', 0))
COLLECTION_NAME = os.getenv('QDRANT_COLLECTION_NAME', None)
VECTOR_SIZE = int(os.getenv('QDRANT_VECTOR_SIZE', 0))

# ---------- PostgreSQL ----------
PG_LOG_HOST = os.getenv('PG_LOG_HOST', None)
PG_LOG_PORT = int(os.getenv('PG_LOG_PORT', 0))
PG_LOG_DB = os.getenv('PG_LOG_DB', None)
PG_LOG_USER = os.getenv('PG_LOG_USER', None)
PG_LOG_PASS = os.getenv('PG_LOG_PASS', None)


# ---------- Настройки синхронизации ----------
SYNC_INTERVAL_MINUTES = int(os.getenv('QDRANT_SYNC_INTERVAL_MINUTES', 60))
BATCH_SIZE = int(os.getenv('QDRANT_BATCH_SIZE', 2000))
CH_BLOCK_SIZE = int(os.getenv('QDRANT_CH_BLOCK_SIZE', 100000))
RETRIES = int(os.getenv('QDRANT_RETRIES', 3))
BATCH_USERS = int(os.getenv('QDRANT_BATCH_USERS', 100))

# ---------- Таблица состояния ----------
SYNC_STATE_TABLE = os.getenv('QDRANT_SYNC_STATE_TABLE', 'qdrant_sync_state')
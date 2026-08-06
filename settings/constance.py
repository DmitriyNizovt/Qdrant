"""
Конфигурация Qdrant-сервиса.
Все параметры берутся из переменных окружения (.env).
"""

import os
from dotenv import load_dotenv

# Загружаем переменные из .env (файл должен лежать в корне проекта)
load_dotenv()

# ---------- ClickHouse (источник данных: auction_vectors) ----------
CH_HOST = os.getenv('CH_HOST', '91.202.205.16')
CH_PORT = int(os.getenv('CH_PORT', 5542))
CH_DB = os.getenv('CH_DB', 'look_a_like')
CH_USER = os.getenv('CH_USER', 'look_user')
CH_PASS = os.getenv('CH_PASS', 'iusdboinc239sckqdaksHFeoi3rs')

# ---------- Qdrant ----------
QDRANT_URL = os.getenv('QDRANT_URL', 'http://localhost:6333')
COLLECTION_NAME = os.getenv('QDRANT_COLLECTION_NAME', 'user_clusters')
VECTOR_SIZE = int(os.getenv('QDRANT_VECTOR_SIZE', 384))

# ---------- PostgreSQL ----------
PG_LOG_HOST = 'localhost'
PG_LOG_PORT = 5433
PG_LOG_DB = 'migration_logs'
PG_LOG_USER = 'log_user'
PG_LOG_PASS = 'log_pass_123'

# ---------- Настройки синхронизации ----------
SYNC_INTERVAL_MINUTES = int(os.getenv('QDRANT_SYNC_INTERVAL_MINUTES', 60))
BATCH_SIZE = int(os.getenv('QDRANT_BATCH_SIZE', 2000))
CH_BLOCK_SIZE = int(os.getenv('QDRANT_CH_BLOCK_SIZE', 100000))
RETRIES = int(os.getenv('QDRANT_RETRIES', 3))
BATCH_USERS = int(os.getenv('QDRANT_BATCH_USERS', 100))

# ---------- Таблица состояния ----------
SYNC_STATE_TABLE = os.getenv('QDRANT_SYNC_STATE_TABLE', 'qdrant_sync_state')
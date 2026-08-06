"""
Модуль для логирования операций Qdrant-синхронизации в PostgreSQL.
Использует SQLAlchemy Core с прямыми SQL-запросами.
"""

import json
from settings.logger import setup_logger
from datetime import datetime
from typing import Optional
import psycopg2
from sqlalchemy import create_engine, text
from settings import constance

# Инициализация логгера (как в миграции)
logger = setup_logger(name="qdrant_scheduler")

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{constance.PG_LOG_USER}:{constance.PG_LOG_PASS}@"
    f"{constance.PG_LOG_HOST}:{constance.PG_LOG_PORT}/"
    f"{constance.PG_LOG_DB}"
)

engine = create_engine(DATABASE_URL, echo=False)


def get_pg_connection():
    """Возвращает соединение с PostgreSQL через SQLAlchemy."""
    return engine.connect()


def init_log_table():
    """Создаёт таблицу qdrant_sync_log, если она не существует."""
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS qdrant_sync_log (
            id SERIAL PRIMARY KEY,
            runner_start TIMESTAMP NOT NULL,
            runner_end TIMESTAMP,
            data_json JSONB NOT NULL,
            errors TEXT
        )
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    logger.info("Таблица qdrant_sync_log создана/проверена")


def insert_log(
    start_time: datetime,
    end_time: Optional[datetime],
    data_json: dict,
    errors: str = ""
):
    """Вставляет запись о выполнении синхронизации в таблицу логов."""
    insert_sql = """
        INSERT INTO qdrant_sync_log (runner_start, runner_end, data_json, errors)
        VALUES (:start_time, :end_time, :data_json, :errors)
    """
    with engine.connect() as conn:
        conn.execute(
            text(insert_sql),
            {
                "start_time": start_time,
                "end_time": end_time,
                "data_json": json.dumps(data_json, default=str),
                "errors": errors,
            }
        )
        conn.commit()
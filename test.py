from methods.postgres_logger import get_pg_connection
from sqlalchemy import text
import json

with get_pg_connection() as conn:
    result = conn.execute(text("""
        SELECT id, runner_start, runner_end, data_json, errors
        FROM qdrant_sync_log
        ORDER BY id DESC
        LIMIT 5
    """))
    for row in result:
        print(f"ID: {row[0]}")
        print(f"Старт: {row[1]}")
        print(f"Конец: {row[2]}")
        print(f"Ошибка: {row[4] if row[4] else 'Нет'}")
        print("Данные (JSON):")
        print(json.dumps(row[3], indent=2, default=str))
        print("-" * 50)
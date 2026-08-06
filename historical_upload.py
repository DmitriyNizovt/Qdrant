"""
Скрипт для принудительной исторической заливки всех данных в Qdrant.
Удаляет существующую коллекцию и создаёт заново.
"""

import logging
from methods.uploader import QdrantUploader
from methods.postgres_logger import init_log_table
from settings.logger import setup_logger

# Инициализация таблицы логов (PostgreSQL)
init_log_table()

# Настройка логгера (как в сервисе миграции)
logger = setup_logger(name="qdrant_scheduler")

# Отключаем логгирование HTTP-запросов от внешних библиотек
logging.getLogger("qdrant_client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

if __name__ == "__main__":
    logger.info("Запуск исторической заливки...")
    uploader = QdrantUploader()
    total = uploader.historical_upload()
    logger.info(f"Историческая заливка завершена. Загружено {total} точек.")
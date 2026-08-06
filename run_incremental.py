"""
Скрипт для инкрементальной синхронизации Qdrant.
"""

from methods.uploader import QdrantUploader
from settings.logger import setup_logger   # добавлен импорт

# Настройка логгера (как в сервисе миграции)
logger = setup_logger(name="qdrant_scheduler")

if __name__ == "__main__":
    logger.info("Запуск инкрементальной синхронизации...")
    uploader = QdrantUploader()
    total = uploader.incremental_upload()
    logger.info(f"Инкрементальная синхронизация завершена. Обновлено {total} точек.")
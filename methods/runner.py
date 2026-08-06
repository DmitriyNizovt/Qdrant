"""
Запускает синхронизацию Qdrant.
"""

from methods.uploader import QdrantUploader
from settings.logger import setup_logger

# Инициализация логгера (файл + консоль)
logger = setup_logger(name="qdrant_scheduler")


def run_qdrant_sync() -> None:
    logger.info("Запуск синхронизации Qdrant...")
    try:
        uploader = QdrantUploader()
        total_points = uploader.sync()
        logger.info(f"Синхронизация завершена. Загружено/обновлено {total_points} точек.")
    except Exception as e:
        logger.exception(f"Ошибка при синхронизации Qdrant: {e}")
        raise
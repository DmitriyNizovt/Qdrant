import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from settings import constance
from settings.logger import setup_logger
from methods.runner import run_qdrant_sync
from methods.postgres_logger import init_log_table, get_pg_connection
from methods.clickhouse import get_ch_client


# Инициализация логгера (как в миграции)
logger = setup_logger(name="qdrant_scheduler")

# ----- Проверка подключения к PostgreSQL и создание таблицы логов -----
try:
    # Пытаемся подключиться к PostgreSQL
    conn = get_pg_connection()
    conn.close()
    logger.info("Подключение к PostgreSQL (логи) успешно")

    # Создаём таблицу логов, если её нет
    init_log_table()
    logger.info("Таблица логов qdrant_sync_log проверена/создана")

except Exception as e:
    logger.error(f"Ошибка при инициализации PostgreSQL (логи): {e}")
    sys.exit(1)

# ----- Проверка подключения к ClickHouse и создание таблицы состояния -----
try:
    # Проверяем доступность ClickHouse (простой запрос)
    ch = get_ch_client()
    ch.execute("SELECT 1")
    ch.disconnect_connection()
    logger.info("Подключение к ClickHouse (источник) успешно")

except Exception as e:
    logger.error(f"Ошибка при подключении к ClickHouse: {e}")
    sys.exit(1)

# ----- Остальной код шедулера -----
INTERVAL_MINUTES = constance.SYNC_INTERVAL_MINUTES
TIMEZONE = ZoneInfo("Europe/Moscow")

def job_wrapper():
    try:
        logger.info("Запуск задания синхронизации Qdrant...")
        run_qdrant_sync()
    except Exception as e:
        logger.error(f"Задание синхронизации завершилось ошибкой: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info(f"Запуск шедулера Qdrant с интервалом {INTERVAL_MINUTES} минут.")
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        job_wrapper,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        id="qdrant_sync",
        replace_existing=True,
        next_run_time=datetime.now(TIMEZONE) + timedelta(seconds=5)
    )
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Шедулер остановлен пользователем.")
        scheduler.shutdown()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Критическая ошибка в шедулере: {e}", exc_info=True)
        sys.exit(1)
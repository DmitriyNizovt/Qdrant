"""
Модуль для настройки логирования в проекте.
Поддерживает два формата: JSON и обычный текст.
Уровень логирования и формат задаются через переменные окружения.
"""

import json
import logging
import os
import sys
from datetime import datetime
from enum import Enum
from typing import Optional

class LogFormat(Enum):
    """Форматы вывода логов."""
    JSON = "json"
    PLAIN = "plain"

    @classmethod
    def from_string(cls, value: Optional[str]) -> "LogFormat":
        if not value:
            return cls.PLAIN
        try:
            return cls[value.upper()]
        except KeyError:
            logging.warning(f"Invalid log format '{value}', using PLAIN")
            return cls.PLAIN


class LogLevel(Enum):
    """Уровни логирования."""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

    @classmethod
    def from_string(cls, value: Optional[str]) -> "LogLevel":
        if not value:
            return cls.INFO
        try:
            return cls[value.upper()]
        except KeyError:
            logging.warning(f"Invalid log level '{value}', using INFO")
            return cls.INFO


class JSONFormatter(logging.Formatter):
    """Форматтер для JSON-логов."""
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


class PlainTextFormatter(logging.Formatter):
    """Форматтер для текстовых логов."""
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(levelname)s - %(message)s [%(module)s:%(funcName)s:%(lineno)d]",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class MemoryLogHandler(logging.Handler):
    """Хендлер для временного хранения error-логов в памяти."""
    def __init__(self, level=logging.ERROR):
        super().__init__(level)
        self.logs = []

    def emit(self, record):
        log_entry = self.format(record)
        self.logs.append(log_entry)


class LoggerConfig:
    """Конфигурация логгера из переменных окружения."""
    def __init__(self):
        self.name = os.getenv("LOGGER_NAME", "fastapi_app")
        self.format_type = LogFormat.from_string(os.getenv("LOGGER_FORMAT"))
        self.level = LogLevel.from_string(os.getenv("LOGGER_LEVEL"))


def setup_logger(
    name: Optional[str] = None,
    level: Optional[LogLevel] = None,
    format_type: Optional[LogFormat] = None,
) -> logging.Logger:
    """
    Настраивает и возвращает логгер.

    Параметры переопределяют значения из переменных окружения.
    """
    config = LoggerConfig()
    logger_name = name or config.name
    logger_level = (level or config.level).value
    logger_format = format_type or config.format_type

    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        logger.setLevel(logger_level)
        handler = logging.StreamHandler(sys.stdout)
        if logger_format == LogFormat.JSON:
            formatter = JSONFormatter()
        else:
            formatter = PlainTextFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Возвращает существующий логгер или создаёт новый с настройками по умолчанию."""
    config = LoggerConfig()
    return logging.getLogger(name or config.name)
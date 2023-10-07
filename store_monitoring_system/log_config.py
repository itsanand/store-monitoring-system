"""Handles log configurations for Store Monitoring System"""
from typing import Final, Union
from pydantic import BaseModel


class LogConfig(BaseModel):
    """Log class to Handle log configuration"""

    LOGGER_NAME: Final[str] = "store-system"
    LOG_FORMAT: Final[str] = "%(levelprefix)s| %(asctime)s | %(message)s"
    LOG_LEVEL: Final[str] = "DEBUG"

    formatters: dict[str, dict[str, str]] = {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": LOG_FORMAT,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }
    handlers: dict[str, dict[str, str]] = {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    }
    loggers: dict[str, dict[str, Union[list[str], str]]] = {
        "store-system-logger": {
            "handlers": ["default"],
            "level": LOG_LEVEL,
        },
    }

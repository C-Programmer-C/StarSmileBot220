import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SECURITY_KEY: str
    REQUEST_FORM_FIELDS: Dict[str, int] = {
        "fio": 4,
        "telephone": 5,
        "tg_account": 6,
        "tg_id": 7,
        "max_id": 25,
        "theme": 8,
        "email": 9,
        "description": 10,
    }

    USER_FORM_FIELDS: Dict[str, int] = {
        "fullname": 2,
        "telephone": 3,
        "max_id": 8,
        "tg_account": 4,
        "tg_id": 5,
    }

    LOGIN: str

    MAX_FILE_SIZE: int

    #: Лимит размера файла для канала MAX (байт). По умолчанию 500 МиБ; в .env: MAX_FILE_SIZE_MAX=524288000
    MAX_FILE_SIZE_MAX: int = 524_288_000

    #: Таймаут HTTP для больших файлов (MAX: скачивание вложений, загрузка в Pyrus, отправка)
    MAX_LARGE_FILE_TIMEOUT_SEC: float = 300.0

    BOT_TOKEN: str
    MAX_BOT_TOKEN: str

    BASE_URL: str

    CLIENT_FORM_ID: int

    APPEAL_FORM_ID: int

    #: Уровень логов: DEBUG, INFO, WARNING, ERROR (в .env: LOG_LEVEL=DEBUG)
    LOG_LEVEL: str = "INFO"

    #: Лимиты нагрузки (скользящее окно RATE_LIMIT_WINDOW_SEC)
    USER_MESSAGE_LIMIT: int = 8
    USER_FILE_LIMIT: int = 2
    GLOBAL_LIMIT: int = 30
    RATE_LIMIT_WINDOW_SEC: float = 10.0
    MAX_CONCURRENT_TASKS: int = 5

    #: Версия в User-Agent исходящих запросов к Pyrus API: Pyrus-Bot-{N}
    PYRUS_PROTOCOL_VERSION: str = "4"

    class Config:  
        env_file = ".env"

settings = Settings() # type: ignore

class StripAnsiFilter(logging.Filter):
    ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    def filter(self, record: logging.LogRecord):
        record.msg = self.ANSI_ESCAPE.sub('', str(record.msg))
        return True


def conf_logger(log_path: Optional[str] = None):
    if log_path is None:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../app.log')

    level = getattr(logging, str(settings.LOG_LEVEL).upper(), logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(StripAnsiFilter())

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    root_logger.debug("Logger initialized, log file created")
    configure_messaging_failure_logger()
    configure_rate_limit_logger()


def configure_messaging_failure_logger(log_path: Optional[str] = None) -> None:
    """Отдельный файл логов для сбоев доставки сообщений (бот ↔ CRM)."""
    if log_path is None:
        base = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base, "data")
        os.makedirs(data_dir, exist_ok=True)
        log_path = os.path.join(data_dir, "messaging_failures.log")

    lg = logging.getLogger("messaging_failures")
    if lg.handlers:
        return

    handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    handler.addFilter(StripAnsiFilter())
    lg.addHandler(handler)
    lg.setLevel(logging.INFO)
    lg.propagate = False


def configure_rate_limit_logger(log_path: Optional[str] = None) -> None:
    """Отдельный лог: ожидание из-за лимитов, перегруз (telegram/max/pyrus)."""
    if log_path is None:
        base = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base, "data")
        os.makedirs(data_dir, exist_ok=True)
        log_path = os.path.join(data_dir, "rate_limits.log")

    lg = logging.getLogger("rate_limits")
    if lg.handlers:
        return

    handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setLevel(logging.WARNING)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    handler.addFilter(StripAnsiFilter())
    lg.addHandler(handler)
    lg.setLevel(logging.WARNING)
    lg.propagate = False

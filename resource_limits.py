"""
Ограничение нагрузки: скользящее окно 10 с на пользователя и глобально,
семафор для параллельных загрузок файлов.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict

from config import settings

logger = logging.getLogger(__name__)
_rate_limit_file_logger: logging.Logger | None = None


def _get_rate_file_logger() -> logging.Logger:
    global _rate_limit_file_logger
    if _rate_limit_file_logger is None:
        from config import configure_rate_limit_logger

        configure_rate_limit_logger()
        _rate_limit_file_logger = logging.getLogger("rate_limits")
    return _rate_limit_file_logger


def log_rate_violation(
    source: str,
    event: str,
    detail: str = "",
    *,
    waited_sec: float | None = None,
) -> None:
    """Запись в data/rate_limits.log при ожидании из-за лимита или отказе."""
    lg = _get_rate_file_logger()
    extra = f" waited_sec={waited_sec:.2f}" if waited_sec is not None else ""
    msg = f"{source} | {event} | {detail}{extra}"
    lg.warning(msg)


class _SlidingWindow:
    """До max_events событий за window_sec секунд; acquire ждёт слот."""

    __slots__ = ("_window_sec", "_max_events", "_times", "_lock")

    def __init__(self, max_events: int, window_sec: float) -> None:
        self._window_sec = window_sec
        self._max_events = max_events
        self._times: Deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        *,
        log_source: str,
        log_name: str,
    ) -> None:
        total_wait = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                cutoff = now - self._window_sec
                while self._times and self._times[0] <= cutoff:
                    self._times.popleft()
                if len(self._times) < self._max_events:
                    self._times.append(now)
                    if total_wait >= 0.5:
                        log_rate_violation(
                            log_source,
                            "throttle_wait",
                            log_name,
                            waited_sec=total_wait,
                        )
                    return
                wait_sec = self._window_sec - (now - self._times[0]) + 0.02
            sleep_for = min(max(wait_sec, 0.02), 1.0)
            await asyncio.sleep(sleep_for)
            total_wait += sleep_for


class _PerKeyWindows:
    """Отдельное окно на каждый ключ (пользователь)."""

    def __init__(self, max_events: int, window_sec: float) -> None:
        self._max_events = max_events
        self._window_sec = window_sec
        self._data: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def acquire(self, key: str, *, log_source: str, log_label: str) -> None:
        total_wait = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                cutoff = now - self._window_sec
                dq = self._data[key]
                while dq and dq[0] <= cutoff:
                    dq.popleft()
                if len(dq) < self._max_events:
                    dq.append(now)
                    if total_wait >= 0.5:
                        log_rate_violation(
                            log_source,
                            "user_throttle_wait",
                            f"{log_label} key={key}",
                            waited_sec=total_wait,
                        )
                    return
                wait_sec = self._window_sec - (now - dq[0]) + 0.02
            sleep_for = min(max(wait_sec, 0.02), 1.0)
            await asyncio.sleep(sleep_for)
            total_wait += sleep_for


# Глобальный лимит на все исходящие запросы к Pyrus API (внутри процесса)
_global_pyrus: _SlidingWindow | None = None
_user_messages: _PerKeyWindows | None = None
_user_files: _PerKeyWindows | None = None
_download_semaphore: asyncio.Semaphore | None = None


def _global() -> _SlidingWindow:
    global _global_pyrus
    if _global_pyrus is None:
        _global_pyrus = _SlidingWindow(
            settings.GLOBAL_LIMIT,
            float(settings.RATE_LIMIT_WINDOW_SEC),
        )
    return _global_pyrus


def _umsg() -> _PerKeyWindows:
    global _user_messages
    if _user_messages is None:
        _user_messages = _PerKeyWindows(
            settings.USER_MESSAGE_LIMIT,
            float(settings.RATE_LIMIT_WINDOW_SEC),
        )
    return _user_messages


def _ufile() -> _PerKeyWindows:
    global _user_files
    if _user_files is None:
        _user_files = _PerKeyWindows(
            settings.USER_FILE_LIMIT,
            float(settings.RATE_LIMIT_WINDOW_SEC),
        )
    return _user_files


def get_download_semaphore() -> asyncio.Semaphore:
    global _download_semaphore
    if _download_semaphore is None:
        _download_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_TASKS)
    return _download_semaphore


async def acquire_global_pyrus_slot() -> None:
    await _global().acquire(
        log_source="pyrus_api",
        log_name="global_limit",
    )


async def acquire_user_message_slot(channel: str, user_id: int | str) -> None:
    """channel: telegram | max_messenger; user_id — tg_id или MAX user_id."""
    await _umsg().acquire(
        f"{channel}:{user_id}",
        log_source=channel,
        log_label="message_limit",
    )


async def acquire_user_file_slot(channel: str, user_id: int | str) -> None:
    await _ufile().acquire(
        f"{channel}:{user_id}",
        log_source=channel,
        log_label="file_limit",
    )


def telegram_attachment_units(message: Any) -> int:
    """Одно вложение в сообщении = 1 единица для USER_FILE_LIMIT."""
    m = message
    if getattr(m, "photo", None) or getattr(m, "document", None):
        return 1
    if getattr(m, "audio", None) or getattr(m, "voice", None):
        return 1
    if getattr(m, "video", None) or getattr(m, "sticker", None):
        return 1
    return 0

import asyncio
import time


class MaxRateLimiter:
    """Simple global limiter for MAX API requests."""

    def __init__(self, requests_per_second: int = 25):
        self._min_interval = 1.0 / float(requests_per_second)
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_at
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_at = time.monotonic()


max_rate_limiter = MaxRateLimiter(requests_per_second=25)

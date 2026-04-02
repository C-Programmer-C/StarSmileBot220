import asyncio
import json
import logging
import time
from typing import Any, Optional
import httpx
from config import settings
from resource_limits import acquire_global_pyrus_slot
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


logger = logging.getLogger(__name__)


def _log_pyrus_http_error(
    method: str,
    request_url: str,
    response: httpx.Response,
    json_data: Optional[dict[str, Any]] = None,
    *,
    had_files: bool = False,
) -> None:
    """Подробный лог при ошибочном ответе Pyrus (у 500 часто только generic JSON)."""
    status = response.status_code
    level = logging.ERROR if status >= 500 else logging.WARNING

    raw_text = response.text or ""
    if len(raw_text) > 16384:
        raw_text = raw_text[:16384] + "...<truncated>"
    try:
        parsed = response.json()
        response_body = json.dumps(parsed, ensure_ascii=False, indent=2)
    except Exception:
        response_body = raw_text

    interesting_headers: dict[str, str] = {}
    for key in (
        "x-request-id",
        "x-correlation-id",
        "content-type",
        "date",
        "retry-after",
    ):
        v = response.headers.get(key)
        if v:
            interesting_headers[key] = v

    if had_files:
        request_preview = "<multipart file upload>"
    elif json_data is not None:
        try:
            request_preview = json.dumps(json_data, ensure_ascii=False)
            if len(request_preview) > 12000:
                request_preview = request_preview[:12000] + "...<truncated>"
        except Exception:
            request_preview = repr(json_data)[:12000]
    else:
        request_preview = None

    extra_hint = ""
    if status >= 500 and "/comments" in request_url:
        extra_hint = (
            " Для POST /tasks/.../comments проверьте channel.type, формат attachments (GUID) "
            "и поля формы в Pyrus; при generic server_error часто нужна поддержка Pyrus или "
            "настройка канала в форме."
        )

    logger.log(
        level,
        "Pyrus API error: %s %s -> HTTP %s%s | response_headers=%s | response_body=%s | request_body=%s",
        method,
        request_url,
        status,
        extra_hint,
        interesting_headers or {},
        response_body,
        request_preview,
    )


class RateLimiter:
    """Rate limiter for Pyrus API (5000 requests per 10 minutes)"""
    
    def __init__(self):
        self._lock = asyncio.Lock()
        self._request_times: list[float] = []
        self._min_delay = 0.12  # Минимальная задержка между запросами (8.33 req/sec)
        self._last_request_time = 0.0
        self._rate_limit_remaining: Optional[int] = None
        self._rate_limit_reset: Optional[int] = None
        
    async def acquire(self):
        """Wait if necessary to respect rate limits"""
        async with self._lock:
            current_time = time.time()
            
            # Если есть информация о лимите из заголовков, используем её
            if self._rate_limit_remaining is not None and self._rate_limit_remaining < 100:
                if self._rate_limit_reset:
                    wait_time = self._rate_limit_reset
                    logger.warning(
                        f"Rate limit low ({self._rate_limit_remaining} remaining). "
                        f"Waiting {wait_time} seconds until reset."
                    )
                    await asyncio.sleep(wait_time)
                    self._rate_limit_remaining = None
                    self._rate_limit_reset = None
                    current_time = time.time()
            
            # Минимальная задержка между запросами
            time_since_last = current_time - self._last_request_time
            if time_since_last < self._min_delay:
                await asyncio.sleep(self._min_delay - time_since_last)
            
            # Очистка старых запросов (старше 10 минут)
            ten_minutes_ago = current_time - 600
            self._request_times = [t for t in self._request_times if t > ten_minutes_ago]
            
            # Если слишком много запросов, ждем
            if len(self._request_times) >= 4900:  # Оставляем запас
                oldest_request = min(self._request_times)
                wait_time = 600 - (current_time - oldest_request)
                if wait_time > 0:
                    logger.warning(
                        f"Rate limit approaching. Waiting {wait_time:.2f} seconds."
                    )
                    await asyncio.sleep(wait_time)
            
            self._request_times.append(time.time())
            self._last_request_time = time.time()
    
    def update_from_headers(self, headers: httpx.Headers):
        """Update rate limit info from response headers"""
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        
        if remaining:
            try:
                self._rate_limit_remaining = int(remaining)
            except (ValueError, TypeError):
                pass
        
        if reset:
            try:
                self._rate_limit_reset = int(reset)
            except (ValueError, TypeError):
                pass
    
    def get_reset_time(self) -> int:
        """Get reset time in seconds, default to 60 if not set"""
        return self._rate_limit_reset or 60


_rate_limiter = RateLimiter()


class TokenManager:
    def __init__(self):
        self._token: str | None = None
        self._lock = asyncio.Lock()

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def get_token(self) -> str:
        """Returns the current token, refreshing it if necessary."""
        if self._token:
            return self._token

        async with self._lock:
            if self._token:
                return self._token

            await self._refresh_token()

            if not self._token:
                raise RuntimeError("Failed to obtain token — _refresh_token did not set value")

            return self._token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    async def _refresh_token(self):
        logger.info("🔄 Refreshing token.")
        await acquire_global_pyrus_slot()
        ua = f"Pyrus-Bot-{settings.PYRUS_PROTOCOL_VERSION}"
        t0 = time.perf_counter()
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://accounts.pyrus.com/api/v4/auth",
                json={
                    "login": settings.LOGIN,
                    "security_key": settings.SECURITY_KEY,
                },
                headers={
                    "User-Agent": ua,
                    "X-Pyrus-Retry": "1/3",
                },
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()
            self._token = data["access_token"]
        ms = (time.perf_counter() - t0) * 1000
        logger.info("✅ Token refreshed. POST auth time_ms=%.1f", ms)

    async def invalidate(self):
        """Resets the token if the error is 404"""
        self._token = None


token_manager: TokenManager | None = None


def get_token_manager() -> TokenManager:
    """Returns a singleton TokenManager instance."""
    global token_manager
    if token_manager is None:
        token_manager = TokenManager()
    return token_manager


def retry_on_exception(exc: BaseException) -> bool:
    """Returns True if the exception should trigger a retry."""
    if isinstance(exc, httpx.HTTPStatusError):
        # Не повторяем для 403 (Forbidden) и 429 (Too Many Requests)
        # 429 обрабатывается отдельно с ожиданием
        if exc.response.status_code in (403, 429):
            return False
    return True


def _api_timeout(
    *,
    files: Optional[dict[str, tuple[Any]]],
    explicit: Optional[float],
) -> float:
    if explicit is not None:
        return explicit
    if files is not None:
        return float(settings.MAX_LARGE_FILE_TIMEOUT_SEC)
    return 30.0


async def api_request(
    method: str,
    endpoint: str = "",
    url: Optional[str] = None,
    json_data: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
    files: Optional[dict[str, tuple[Any]]] = None,
    timeout: Optional[float] = None,
) -> dict[str, Any] | bytes | None:
    """Исходящие запросы к Pyrus API: глобальный лимит, заголовки Pyrus-Bot, X-Pyrus-Retry, замер времени."""

    request_url = url if url else f"{settings.BASE_URL}{endpoint}"
    req_timeout = _api_timeout(files=files, explicit=timeout)
    ua = f"Pyrus-Bot-{settings.PYRUS_PROTOCOL_VERSION}"

    last_error: Optional[BaseException] = None
    for attempt in range(3):
        retry_label = f"{attempt + 1}/3"
        token = await get_token_manager().get_token()
        headers: dict[str, str] = {
            "Authorization": f"Bearer {token}",
            "User-Agent": ua,
            "X-Pyrus-Retry": retry_label,
        }

        await acquire_global_pyrus_slot()
        await _rate_limiter.acquire()

        t0 = time.perf_counter()
        try:
            async with httpx.AsyncClient() as client:
                if not files:
                    response = await client.request(
                        method=method,
                        url=request_url,
                        headers=headers,
                        json=json_data,
                        params=params,
                        timeout=req_timeout,
                    )
                else:
                    response = await client.request(
                        method=method,
                        url=request_url,
                        headers=headers,
                        params=params,
                        timeout=req_timeout,
                        files=files,  # type: ignore
                    )
        except Exception as e:
            logger.error("Error during API request: %s", e)
            raise

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "Pyrus API %s %s HTTP %s time_ms=%.1f X-Pyrus-Retry=%s",
            method,
            request_url,
            response.status_code,
            elapsed_ms,
            retry_label,
        )

        _rate_limiter.update_from_headers(response.headers)

        if response.status_code == 401:
            _log_pyrus_http_error(
                method,
                request_url,
                response,
                json_data,
                had_files=bool(files),
            )
            await get_token_manager().invalidate()
            last_error = httpx.HTTPStatusError(
                "Unauthorized", request=response.request, response=response
            )
            continue

        if response.status_code == 429:
            error_data = (
                response.json()
                if response.headers.get("content-type", "").startswith("application/json")
                else {}
            )
            error_msg = error_data.get("error", "Rate limit exceeded")
            reset_time = _rate_limiter.get_reset_time()
            logger.warning(
                "Rate limit exceeded (429). Error: %s. Waiting %s seconds before retry.",
                error_msg,
                reset_time,
            )
            await asyncio.sleep(reset_time)

            await acquire_global_pyrus_slot()
            await _rate_limiter.acquire()
            t1 = time.perf_counter()
            token = await get_token_manager().get_token()
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Pyrus-Retry"] = retry_label
            async with httpx.AsyncClient() as client:
                if not files:
                    response = await client.request(
                        method=method,
                        url=request_url,
                        headers=headers,
                        json=json_data,
                        params=params,
                        timeout=req_timeout,
                    )
                else:
                    response = await client.request(
                        method=method,
                        url=request_url,
                        headers=headers,
                        params=params,
                        timeout=req_timeout,
                        files=files,  # type: ignore
                    )
            retry_ms = (time.perf_counter() - t1) * 1000
            logger.info(
                "Pyrus API retry-after-429 %s %s HTTP %s time_ms=%.1f",
                method,
                request_url,
                response.status_code,
                retry_ms,
            )
            _rate_limiter.update_from_headers(response.headers)
            if response.status_code == 429:
                raise httpx.HTTPStatusError(
                    "Too Many Requests", request=response.request, response=response
                )

        if response.is_error:
            _log_pyrus_http_error(
                method,
                request_url,
                response,
                json_data,
                had_files=bool(files),
            )
        response.raise_for_status()

        if params and params.get("download"):
            return response.content

        return response.json()

    if last_error:
        raise last_error
    raise RuntimeError("Pyrus API: failed after 3 attempts")

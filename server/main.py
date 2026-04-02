import hashlib
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional
from bot_client import BotClient
from max_bot_client import MaxBotClient
from config import conf_logger, settings
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
import asyncio
from max_rate_limiter import max_rate_limiter
from pyrus_api_service import get_token_manager
from utils import (
    download_files,
    find_value,
    open_chat,
    post_crm_plain_comment,
    record_messaging_failure,
    send_message_to_max_chat,
    send_message_to_telegram_chat,
)
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


def _form_scalar_nonempty(value: Any) -> bool:
    """Есть ли в поле формы Pyrus непустое скалярное значение (как в карточке задачи)."""
    if value is None:
        return False
    if isinstance(value, dict):
        for key in ("text", "value", "plain"):
            inner = value.get(key)
            if inner is not None and str(inner).strip():
                return True
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return bool(str(value).strip())


def _pyrus_comment_text(value: Any) -> Optional[str]:
    """Normalize Pyrus task comment text to plain string, if present."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    if isinstance(value, dict):
        for key in ("text", "plain", "value", "html"):
            inner = value.get(key)
            if isinstance(inner, str):
                s = inner.strip()
                return s if s else None
        return None
    s = str(value).strip()
    return s if s else None


count_triggered = 1

webhook_queue = asyncio.Queue(maxsize=500)  # type: ignore

def require(condition: str | int | bytes, msg: str, status_code: int = 500):
    if not condition:
        logger.error(msg)
        raise HTTPException(status_code=status_code, detail=msg)

def create_file_payload(data: dict[str, Any]):
    """Creates a data folder and a json file from the request body"""
    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / "payload.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _normalize_x_pyrus_sig(header_sig: str) -> str:
    s = header_sig.strip()
    if s.lower().startswith("sha1="):
        s = s[5:].strip()
    return s.lower()


def verify_signature(header_sig: Optional[str], body: bytes) -> bool:
    """HMAC-SHA1(SECURITY_KEY, raw body), hex — как в документации Pyrus."""
    if not header_sig or not str(header_sig).strip():
        logger.debug("X-Pyrus-Sig header missing or empty")
        return False

    try:
        received = _normalize_x_pyrus_sig(header_sig)
        if not received:
            return False
        expected_sig = hmac.new(
            settings.SECURITY_KEY.encode("utf-8"),
            msg=body,
            digestmod=hashlib.sha1,
        ).hexdigest()
        ok = hmac.compare_digest(received, expected_sig.lower())
        if not ok:
            logger.warning(
                "X-Pyrus-Sig mismatch (SECURITY_KEY vs секрет бота в Pyrus, или тело изменено прокси); "
                "len(sig)=%s len(hex)=%s",
                len(received),
                len(expected_sig),
            )
        return ok
    except Exception:
        logger.exception("verify_signature failed")
        return False


async def webhook_worker():
    """Background worker to process webhooks from the queue."""
    while True:

        logger.info("Webhook worker activated.")

        body, sig, retry, user_agent = await webhook_queue.get() # type: ignore

        try:
            await process_webhook(body, sig, retry, user_agent)  # type: ignore
        except Exception as e:
            logger.exception("Error processing webhook: %s", e)
        finally:
            webhook_queue.task_done()


@asynccontextmanager 
async def lifespan(app: FastAPI):
    """Lifespan context manager to start the webhook worker on app startup."""
    worker_task = asyncio.create_task(webhook_worker())
    app.state.worker_task = worker_task
    logger.info("Starting webhook worker.")
    try:
        yield
    finally:
        logger.info("Shutting down webhook worker.")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            logger.info("Webhook worker cancelled.")


app = FastAPI(title="Pyrus Webhook (FastAPI)", lifespan=lifespan)

@app.post("/webhook")
async def pyrus_webhook(
    request: Request,
    x_pyrus_sig: Optional[str] = Header(None, alias="X-Pyrus-Sig"),
    x_pyrus_retry: Optional[str] = Header(None, alias="X-Pyrus-Retry"),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
):
    """Receives incoming POST requests from Pyrus and queues them for processing."""
    body = await request.body()

    if x_pyrus_sig is None:
        x_pyrus_sig = request.headers.get("x-pyrus-sig")

    if not user_agent or not user_agent.startswith("Pyrus-Bot-"):
        logger.error("Unexpected User-Agent: %s", user_agent)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Bad User-Agent"
        )

    if not verify_signature(x_pyrus_sig, body):
        if not x_pyrus_sig or not str(x_pyrus_sig).strip():
            logger.error(
                "Нет X-Pyrus-Sig: запрос от Pyrus? Прокси пробрасывает заголовок и не переписывает body?"
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Pyrus-Sig",
        )

    logger.info("Webhook received and queued for processing.")

    await webhook_queue.put((body, x_pyrus_sig, x_pyrus_retry, user_agent))  # type: ignore


async def process_webhook(
    body: bytes,
    x_pyrus_sig: Optional[str] = Header(None, alias="X-Pyrus-Sig"),
    x_pyrus_retry: Optional[str] = Header(None, alias="X-Pyrus-Retry"),
    user_agent: Optional[str] = Header(None, alias="User-Agent"),
):
    """
    Processes incoming POST requests from Pyrus and the last event and the last comment.
    """
    _t0 = time.perf_counter()
    try:
        return await _process_webhook_impl(body)
    finally:
        _ms = (time.perf_counter() - _t0) * 1000
        logger.info(
            "Webhook process_webhook total_ms=%.1f X-Pyrus-Retry=%r User-Agent=%r",
            _ms,
            x_pyrus_retry,
            user_agent,
        )


async def _process_webhook_impl(body: bytes):
    require(body, "No body was found during request processing.")

    try:
        data = json.loads(body)
    except Exception:
        logger.exception("Error when parsing JSON")
        raise HTTPException(status_code=422, detail="Incorrect JSON")

    task = data.get("task", {})

    access_token = data.get("access_token", {})

    fields = task.get("fields", {})

    require(task, "Task not found.")

    require(access_token, "token not found")

    require(fields, "fields not found")

    comments = task.get("comments")

    require(comments, "No comments in task")

    require(fields, "Unexpected User-Agent")

    try:
        create_file_payload(data)
    except Exception:
        logger.exception("Error when creating the file with request body")

    event = data.get("event", {})

    require(event, "No event field in payload")

    task_id = data.get("task_id", {})

    require(task_id, "No task_id field in payload")

    global count_triggered

    logger.info(f"Received webhook for task #{task_id} from Pyrus #{count_triggered}")

    count_triggered += 1

    create_date = task.get("create_date", {})

    require(create_date, "No create_date field in payload")

    last_comment = comments[-1]

    if last_comment.get("create_date") == create_date:
        rf = settings.REQUEST_FORM_FIELDS
        fid_tg = rf.get("tg_id")
        fid_max = rf.get("max_id")
        raw_tg = find_value(fields, fid_tg) if fid_tg is not None else None
        raw_max = find_value(fields, fid_max) if fid_max is not None else None
        has_tg = _form_scalar_nonempty(raw_tg)
        has_max = _form_scalar_nonempty(raw_max)

        if not has_tg and not has_max:
            logger.info(
                "Задача %s: первая запись (дата комментария = дата создания задачи), "
                "но в полях обращения пусто и tg_id (id=%s), и max_id (id=%s) — "
                "open_chat не вызываем",
                task_id,
                fid_tg,
                fid_max,
            )
            return JSONResponse(status_code=status.HTTP_200_OK, content={})

        opened: list[str] = []
        if has_tg:
            await open_chat(task_id, channel_type="telegram")
            opened.append("telegram")
        if has_max:
            await open_chat(task_id, channel_type="max_messenger")
            opened.append("max_messenger")

        logger.info(
            "Задача %s: open_chat по полям формы — открыто %d канал(ов): %s. "
            "tg_id (поле %s)=%r, max_id (поле %s)=%r",
            task_id,
            len(opened),
            ", ".join(opened),
            fid_tg,
            raw_tg if has_tg else None,
            fid_max,
            raw_max if has_max else None,
        )
        return JSONResponse(status_code=status.HTTP_200_OK, content={})

    channel_type = last_comment.get("channel", {}).get("type")
    if channel_type not in ("telegram", "max_messenger"):
        logger.info(f"The last comment of task #{task_id} has unsupported channel: {channel_type}")
        return JSONResponse(status_code=status.HTTP_200_OK, content={})

    text = _pyrus_comment_text(last_comment.get("text"))

    attachments = last_comment.get("attachments")

    bot = None
    chat_id: int | None = None

    try:
        token = await get_token_manager().get_token()

        files_info = None

        if attachments:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            if channel_type == "max_messenger":
                files_info = await download_files(
                    attachments,
                    headers,
                    timeout=float(settings.MAX_LARGE_FILE_TIMEOUT_SEC),
                    max_attachment_bytes=settings.MAX_FILE_SIZE_MAX,
                )
            else:
                files_info = await download_files(attachments, headers)

        if channel_type == "telegram":
            bot = BotClient.get_instance()
            if not bot:
                logger.error("Failed to get Telegram bot instance")
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"error": "Failed to get Telegram bot instance"},
                )
        else:
            bot = MaxBotClient.get_instance()
            if not bot:
                logger.error("Failed to get MAX bot instance")
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"error": "Failed to get MAX bot instance"},
                )
        # Telegram — tg_id в форме; MAX — max_id (как в max_handlers), иначе в API уходит tg_id → chat.not.found
        if channel_type == "max_messenger":
            field_key = "max_id"
        else:
            field_key = "tg_id"
        field_id = settings.REQUEST_FORM_FIELDS.get(field_key)
        chat_id = find_value(fields, field_id) if field_id else None

        if not chat_id:
            logger.error(
                "ID чата не найден в полях задачи (channel=%s, поле формы %s id=%s)",
                channel_type,
                field_key,
                field_id,
            )
            return JSONResponse(status_code=status.HTTP_200_OK, content={"error": "Chat ID not found in task fields"})

        try:
            chat_id = int(str(chat_id).strip())
        except (ValueError, TypeError) as e:
            logger.exception("Unable to convert chat id to int: %r", chat_id, exc_info=e)
            return JSONResponse(status_code=status.HTTP_200_OK, content={})

        if channel_type == "telegram":
            await send_message_to_telegram_chat(bot, chat_id, text, files_info)
        elif channel_type == "max_messenger":
            await send_message_to_max_chat(bot, chat_id, text, files_info)  # type: ignore[arg-type]
    except Exception as e:
        logger.exception("Error when sending message to messenger chat")
        try:
            tid = int(task_id)
        except (TypeError, ValueError):
            tid = None
        if tid is not None:
            record_messaging_failure(
                direction="crm_to_user",
                task_id=tid,
                channel=str(channel_type),
                message=str(e),
            )
            await post_crm_plain_comment(
                tid,
                f"Не удалось доставить сообщение пользователю ({channel_type}): {e}",
            )
        user_text = (
            "Не удалось доставить ответ от оператора. Попробуйте позже или напишите снова."
        )
        try:
            if bot is not None and chat_id is not None:
                if channel_type == "telegram":
                    await bot.send_message(chat_id=chat_id, text=user_text)
                elif channel_type == "max_messenger":
                    await max_rate_limiter.acquire()
                    # В карточке max_id — user_id собеседника, не chat_id (см. maxapi SendMessage)
                    await bot.send_message(user_id=chat_id, text=user_text)
        except Exception as notify_err:
            logger.exception(
                "Failed to notify user after CRM→user delivery error: %s", notify_err
            )
            if tid is not None:
                record_messaging_failure(
                    direction="user_notify_after_crm_fail",
                    task_id=tid,
                    channel=str(channel_type),
                    message=str(notify_err),
                )
        return JSONResponse(status_code=status.HTTP_200_OK, content={})

    return JSONResponse(status_code=status.HTTP_200_OK, content={})


if __name__ == "__main__":
    import uvicorn
    conf_logger()
    logger.info("Server started.")
    uvicorn.run("server.main:app", host="127.0.0.1", port=8000)

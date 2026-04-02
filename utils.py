import asyncio
import json
from io import BytesIO
import logging
import time
from typing import Any, Dict, List, Optional
import httpx
from aiogram import Bot
from aiogram.types import BufferedInputFile, InputMediaDocument
from maxapi import Bot as MaxBot
from maxapi.enums.attachment import AttachmentType
from maxapi.exceptions.max import MaxUploadFileFailed
from maxapi.enums.upload_type import UploadType
from maxapi.types.attachments.attachment import Attachment
from maxapi.types.attachments.upload import AttachmentPayload, AttachmentUpload
from maxapi.types.input_media import InputMediaBuffer

from config import settings
from max_rate_limiter import max_rate_limiter
from pyrus_api_service import api_request
from resource_limits import get_download_semaphore

logger = logging.getLogger(__name__)

_PLAIN_COMMENT_MAX = 8000


def record_messaging_failure(
    *,
    direction: str,
    message: str,
    task_id: Optional[int] = None,
    channel: Optional[str] = None,
) -> None:
    """Пишет в отдельный лог `data/messaging_failures.log` (см. config.configure_messaging_failure_logger)."""
    from config import configure_messaging_failure_logger

    configure_messaging_failure_logger()
    fl = logging.getLogger("messaging_failures")
    fl.error(
        "direction=%s task_id=%s channel=%s | %s",
        direction,
        task_id,
        channel,
        message,
    )


def _truncate_plain(text: str, max_len: int = _PLAIN_COMMENT_MAX) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


async def post_crm_plain_comment(task_id: int, text: str) -> bool:
    """Комментарий в задачу Pyrus только с полем text (без channel), для алертов операторам."""
    payload = {"text": _truncate_plain(text)}
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            await api_request(
                "POST",
                f"/tasks/{task_id}/comments",
                json_data=payload,
            )
            return True
        except Exception as e:
            last_err = e
            logger.warning(
                "post_crm_plain_comment attempt %s/3 task_id=%s: %r",
                attempt + 1,
                task_id,
                e,
            )
            if attempt < 2:
                await asyncio.sleep(1.0 * (attempt + 1))
    logger.exception(
        "post_crm_plain_comment failed task_id=%s after retries: %s",
        task_id,
        last_err,
    )
    record_messaging_failure(
        direction="crm_alert_failed",
        task_id=task_id,
        message=f"Не удалось отправить plain-комментарий в CRM: {last_err!r}",
    )
    return False


async def open_chat(
    task_id: int,
    channel_type: str = "telegram",
) -> dict[str, Any]:
    """Open chat in task in Pyrus"""
    json_data: dict[str, str | dict[str, str]] = {
        "text": "Чат открыт.",
        "channel": {"type": channel_type},
    }
    endpoint = f"/tasks/{task_id}/comments"
    logger.debug(
        "open_chat: POST %s channel=%s payload=%s",
        endpoint,
        channel_type,
        json_data,
    )
    try:
        result = await api_request(
            method="POST", endpoint=endpoint, json_data=json_data
        )
        if not isinstance(result, dict):
            logger.debug(
                "open_chat: unexpected response type task_id=%s channel=%s type=%s",
                task_id,
                channel_type,
                type(result),
            )
            raise TypeError("Expected dict response")
        task = result.get("task")
        task_keys = list(task.keys()) if isinstance(task, dict) else None
        logger.debug(
            "open_chat: OK task_id=%s channel=%s response.task.keys=%s",
            task_id,
            channel_type,
            task_keys,
        )
        return result
    except Exception as e:
        logger.error(f"Failed to open chat for task {task_id}: {e}")
        logger.debug(
            "open_chat: FAILED task_id=%s channel=%s error=%r",
            task_id,
            channel_type,
            e,
            exc_info=True,
        )
        raise


def _pyrus_field_scalar(v: Any) -> Optional[str]:
    """Скалярное значение поля формы Pyrus: число/строка или объект с text/value/plain."""
    if v is None:
        return None
    if isinstance(v, dict):
        for key in ("text", "value", "plain"):
            inner = v.get(key)
            if inner is not None:
                s = str(inner).strip()
                if s:
                    return s
        return None
    s = str(v).strip()
    return s if s else None


def extra_appeal_fields_from_client_card(
    fields_dict: dict[int, Any],
    *,
    source_channel: str,
) -> list[dict[str, Any]]:
    """
    Дополнительные поля обращения из карточки клиента (если заполнены):
    - из диалога MAX: Telegram ID и аккаунт;
    - из Telegram: MAX user id.
    """
    uf = settings.USER_FORM_FIELDS
    rf = settings.REQUEST_FORM_FIELDS
    extra: list[dict[str, Any]] = []

    if source_channel == "max_messenger":
        tg_id_val = fields_dict.get(uf["tg_id"])
        tg_s = _pyrus_field_scalar(tg_id_val)
        if tg_s:
            try:
                if int(tg_s) > 0:
                    extra.append({"id": rf["tg_id"], "value": tg_s})
            except (ValueError, TypeError):
                pass
        tg_acc = fields_dict.get(uf["tg_account"])
        acc_s = _pyrus_field_scalar(tg_acc)
        if acc_s and acc_s not in ("Не указан", "None", "-"):
            extra.append({"id": rf["tg_account"], "value": acc_s})

    elif source_channel == "telegram":
        max_s = _pyrus_field_scalar(fields_dict.get(uf["max_id"]))
        print(max_s)
        if max_s and max_s not in ("0", "Не указан", "None", "-"):
            extra.append({"id": rf["max_id"], "value": max_s})

    return extra


async def open_chats_after_appeal(
    task_id: int,
    *,
    source_channel: str,
    fields_dict: dict[int, Any],
) -> None:
    """
    Открывает чат в Pyrus для текущего канала; если в карточке клиента
    указаны связанные Telegram / MAX — открывает и второй канал.
    """
    uf = settings.USER_FORM_FIELDS

    def _has_max_id(v: Any) -> bool:
        s = _pyrus_field_scalar(v)
        if not s:
            return False
        return s not in ("0", "Не указан", "None", "-")

    def _has_tg_id(v: Any) -> bool:
        s = _pyrus_field_scalar(v)
        if not s:
            return False
        try:
            return int(s) > 0
        except (ValueError, TypeError):
            return False

    if source_channel == "max_messenger":
        await open_chat(task_id, channel_type="max_messenger")
        if _has_tg_id(fields_dict.get(uf["tg_id"])):
            try:
                await open_chat(task_id, channel_type="telegram")
                logger.info(
                    "Also opened Telegram chat for task %s (linked TG in card)",
                    task_id,
                )
            except Exception as e:
                logger.warning(
                    "Could not open secondary Telegram chat for task %s: %s",
                    task_id,
                    e,
                )
    elif source_channel == "telegram":
        max_fid = uf["max_id"]
        raw_max = fields_dict.get(max_fid)
        max_scalar = _pyrus_field_scalar(raw_max)
        has_max = _has_max_id(raw_max)

        logger.debug(
            "open_chats_after_appeal[telegram]: start task_id=%s field_ids=%s",
            task_id,
            sorted(fields_dict.keys()),
        )
        logger.debug(
            "open_chats_after_appeal[telegram]: max_id field_id=%s raw=%r scalar=%r has_max=%s",
            max_fid,
            raw_max,
            max_scalar,
            has_max,
        )
        if not has_max:
            logger.debug(
                "open_chats_after_appeal[telegram]: MAX secondary chat skipped "
                "(no valid max_id in card or empty). Check USER_FORM_FIELDS.max_id vs form."
            )

        try:
            await open_chat(task_id, channel_type="telegram")
            logger.debug(
                "open_chats_after_appeal[telegram]: primary Telegram chat open call finished task_id=%s",
                task_id,
            )
        except Exception as e:
            logger.warning(
                "open_chats_after_appeal[telegram]: FAILED to open Telegram chat task_id=%s: %s",
                task_id,
                e,
            )
            logger.debug(
                "open_chats_after_appeal[telegram]: Telegram open exception detail",
                exc_info=True,
            )
            raise

        if has_max:
            try:
                logger.debug(
                    "open_chats_after_appeal[telegram]: opening secondary MAX chat task_id=%s",
                    task_id,
                )
                await open_chat(task_id, channel_type="max_messenger")
                logger.info(
                    "Also opened MAX chat for task %s (linked max_id in card)",
                    task_id,
                )
            except Exception as e:
                logger.warning(
                    "Could not open secondary MAX chat for task %s: %s",
                    task_id,
                    e,
                )
                logger.debug(
                    "open_chats_after_appeal[telegram]: MAX secondary open exception detail",
                    exc_info=True,
                )


def prepare_fields_to_dict(fields: list[dict[str, Any]]) -> dict[int, Any]:
    """
    Словарь полей Pyrus по id поля.

    Id всегда приводится к int: в ответах API id иногда приходит строкой,
    из‑за этого раньше fields_dict.get(USER_FORM_FIELDS["max_id"]) не находил
    поле MAX в карточке — max_id не копировался в обращение и не открывался чат MAX.
    """
    out: dict[int, Any] = {}
    
    
    for field in fields:
        raw_id = field.get("id")
        if raw_id is None:
            continue
        try:
            fid = int(raw_id)
        except (TypeError, ValueError):
            continue
        if "value" not in field:
            continue
        out[fid] = field["value"]
    return out


def find_value(fields: list[dict[str, Any]], field_id: int) -> Optional[str | int]:
    """Finds a value in the fields by its ID"""

    return next(
        (field.get("value") for field in fields if field.get("id") == field_id), None
    )


async def download_one(
    client: httpx.AsyncClient,
    attachment: dict[str, Any],
    headers: dict[str, str],
    *,
    max_bytes: Optional[int] = None,
    request_timeout: Optional[float] = None,
) -> dict[str, Any]:
    """Downloads a single file from Pyrus attachment"""
    file_url = attachment.get("url")
    size = attachment.get("size")
    name = attachment.get("name")
    attachment_id = attachment.get("id")
    if not name:
        logger.error("No filename in attachment")
        return {"error": "No filename"}

    if not file_url or not size:
        logger.error(f"No URL or size for file {name}")
        return {"error": "No URL or size"}

    limit = max_bytes if max_bytes is not None else settings.MAX_FILE_SIZE
    if size > limit:
        logger.warning(f"File {name} exceeds size limit")
        return {"error": "File too large"}

    try:
        download_url = f"https://api.pyrus.com/v4/files/download/{attachment_id}"

        logger.info(f"Downloading file from URL: {download_url}")

        req_to = request_timeout if request_timeout is not None else 30.0
        response = await api_request(
            "GET",
            url=download_url,
            params={"download": True},
            timeout=req_to,
        )

        if not isinstance(response, bytes):
            logger.error(f"Invalid response type for {name}")
            return {"error": "Invalid response"}

        logger.info(
            f"Downloaded file {name} successfully. Content bytes: {response[:20]}..."
        )

        return {"filename": name, "content": response}

    except Exception as e:
        logger.exception(f"Failed to download {name}: {e}")
        return {"error": str(e)}


async def download_files(
    attachments: list[dict[str, Any]],
    headers: dict[str, str],
    timeout: float = 30.0,
    max_attachment_bytes: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Downloads files from Pyrus attachments.

    Для MAX: передайте max_attachment_bytes=settings.MAX_FILE_SIZE_MAX и
    timeout=settings.MAX_LARGE_FILE_TIMEOUT_SEC (см. server/main.py).
    """

    limits = httpx.Limits(max_connections=50)

    sem = get_download_semaphore()

    async with httpx.AsyncClient(
        headers=headers, timeout=timeout, limits=limits
    ) as client:

        async def _one(att: dict[str, Any]) -> dict[str, Any]:
            async with sem:
                return await download_one(
                    client,
                    att,
                    headers,
                    max_bytes=max_attachment_bytes,
                    request_timeout=timeout,
                )

        tasks = [_one(att) for att in attachments]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    return results


def process_file_data(attach_data: dict[str, Any]) -> BufferedInputFile | None:
    """Processes file data from attachment"""
    content = attach_data.get("content")
    name = attach_data.get("filename")
    err = attach_data.get("error")
    if not content or not name or err:
        logger.warning("No content or name found in attachment data: %s", err)
        return None

    return BufferedInputFile(file=content, filename=name)


def chunk_list(seq: List[Any], chunk_size: int = 10) -> List[List[Any]]:
    """Chunks a list into smaller lists of a specified size."""
    return [seq[i : i + chunk_size] for i in range(0, len(seq), chunk_size)]


async def send_message_to_telegram_chat(
    bot: Bot,
    chat_id: int,
    text: Optional[str],
    attachments: list[dict[str, Any]] | None,
):
    """Sends a message and attachments to a Telegram chat."""
    try:
        if text:
            logger.info("Sending message to user #%d: %s", chat_id, text)
            await bot.send_message(chat_id=chat_id, text=text)

        if not attachments:
            logger.info("No attachments to send of user #%d", chat_id)
            return

        processed_files: list[InputMediaDocument] = []

        logger.info("Processing attachments for user #%d", chat_id)

        for file in attachments:
            if processed_file := process_file_data(file):
                processed_files.append(InputMediaDocument(media=processed_file))

        if not processed_files:
            logger.warning("No valid files found to send to user #%d", chat_id)
            return

        for chunk in chunk_list(processed_files):
            try:
                await bot.send_media_group(chat_id=chat_id, media=chunk)
                logger.info("Media chunk sent successfully to user #%d", chat_id)
            except Exception as e:
                logger.error(f"Failed to send media chunk: {e}")

    except Exception as e:
        logger.exception(f"Failed to send message to {chat_id}: {e}")
        raise


async def create_user_task(json_data: dict[str, Any]) -> dict[str, Any]:
    """Creates a new task in Pyrus for a user"""
    try:
        assert json_data, "json_data must not be empty"
        result = await api_request(
            method="POST", endpoint="/tasks", json_data=json_data
        )
        if not isinstance(result, dict):
            raise TypeError("Expected dict response")

        if task := result.get("task"):
            logger.info("✅ User task created with ID: %s", task.get("id"))
            return task
        return {}
    except Exception as e:
        logger.error(f"Failed to create user task: {e}")
        raise


async def fetch_form_register_tasks(
    form_id: int,
    field_id: int,
    value: str | int,
) -> list[dict[str, Any]]:
    """Все задачи формы по значению поля (GET /forms/{id}/register?fld{n}=...)."""
    result = await api_request(
        method="GET",
        endpoint=f"/forms/{form_id}/register",
        params={f"fld{field_id}": value},
    )
    if not isinstance(result, dict):
        raise TypeError("Expected dict response")
    tasks = result.get("tasks") or []
    if not isinstance(tasks, list):
        return []
    return [t for t in tasks if isinstance(t, dict)]


def _digits_only(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def phone_register_lookup_variants(raw: str) -> list[str]:
    """
    Варианты строк для запроса fld{{telephone}} к /forms/{{id}}/register.
    Без масок в чате: берём ввод пользователя и типичные нормализации (8/+7, пробелы, BY).
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        v = (v or "").strip()
        if not v or v in seen:
            return
        seen.add(v)
        out.append(v)

    s = (raw or "").strip()
    if not s:
        return out

    add(s)
    add("".join(s.split()))

    d = _digits_only(s)
    if d:
        add(d)

    if len(d) == 11 and d.startswith("8"):
        core = d[1:]
        add("7" + core)
        add("+7" + core)
    if len(d) == 11 and d.startswith("7"):
        add("+" + d)
        add("8" + d[1:])
    if len(d) == 10 and d.startswith("9"):
        add("7" + d)
        add("+7" + d)
        add("8" + d)

    if d.startswith("375") and len(d) >= 10:
        add("+" + d)
        add(d)

    return out


def phone_register_lookup_variants_merged(*raws: str) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for raw in raws:
        for v in phone_register_lookup_variants(raw):
            if v not in seen:
                seen.add(v)
                merged.append(v)
    return merged


async def find_client_tasks_by_phone(
    form_id: int,
    telephone_field_id: int,
    *raw_phone_inputs: str,
) -> list[dict[str, Any]]:
    """
    Поиск карточек клиента по телефону: несколько запросов к регистру с разными
    написаниями номера, объединение задач по id без дубликатов.
    """
    seen_ids: set[Any] = set()
    tasks_out: list[dict[str, Any]] = []
    for q in phone_register_lookup_variants_merged(*raw_phone_inputs):
        if not q:
            continue
        try:
            batch = await fetch_form_register_tasks(form_id, telephone_field_id, q)
        except Exception as e:
            logger.warning(
                "find_client_tasks_by_phone: register GET fld%s=%r failed: %s",
                telephone_field_id,
                q,
                e,
            )
            continue
        for t in batch:
            tid = t.get("id")
            if tid is None or tid in seen_ids:
                continue
            seen_ids.add(tid)
            tasks_out.append(t)
    return tasks_out


async def ensure_max_id_on_client_task(
    task_id: int,
    max_user_id: int,
    fields_dict: dict[int, Any],
) -> None:
    """
    После входа по телефону в MAX: обновить в карточке клиента поле max_id через
    POST /tasks/{id}/comments с массивом field_updates (id поля max_id, value — MAX user_id).
    """
    fid = settings.USER_FORM_FIELDS["max_id"]
    cur = _pyrus_field_scalar(fields_dict.get(fid))
    if cur is not None and str(cur).strip() == str(max_user_id):
        return
    text = "Привязка MAX к карточке клиента (бот)."
    if cur is not None and str(cur).strip() and str(cur).strip() != str(max_user_id):
        logger.warning(
            "ensure_max_id_on_client_task: task %s max_id %r → %s (вход по телефону)",
            task_id,
            cur,
            max_user_id,
        )
        text = (
            f"Обновлён max_id при входе в MAX по телефону (было {cur}, стало {max_user_id})."
        )
    await api_request(
        "POST",
        f"/tasks/{task_id}/comments",
        json_data={
            "text": text,
            "field_updates": [{"id": fid, "value": max_user_id}],
        },
    )


_dup_warn_last: dict[str, float] = {}
DUP_WARN_TTL_SEC = 3600.0


def _register_dup_warn_should_send(cache_key: str) -> bool:
    """Не чаще одного раза в DUP_WARN_TTL_SEC на ключ (антиспам в CRM)."""
    now = time.time()
    stale = [k for k, t in _dup_warn_last.items() if now - t > DUP_WARN_TTL_SEC * 3]
    for k in stale[:300]:
        _dup_warn_last.pop(k, None)
    last = _dup_warn_last.get(cache_key)
    if last is not None and (now - last) < DUP_WARN_TTL_SEC:
        return False
    _dup_warn_last[cache_key] = now
    return True


async def warn_if_multiple_tasks_on_register(
    form_id: int,
    field_id: int,
    value: str | int,
    *,
    kind: str,
    tasks: Optional[list[dict[str, Any]]] = None,
) -> None:
    """
    Если по регистру найдено несколько задач с одним значением поля — plain-комментарий в каждую.
    Передайте tasks=..., чтобы не дублировать GET той же выборки.
    """
    if tasks is None:
        tasks = await fetch_form_register_tasks(form_id, field_id, value)
    if len(tasks) <= 1:
        return
    cache_key = f"d:{form_id}:{field_id}:{value!s}:{len(tasks)}"
    if not _register_dup_warn_should_send(cache_key):
        return
    ids = sorted(
        {int(t["id"]) for t in tasks if t.get("id") is not None},
    )
    text = (
        f"⚠️ [Бот] Дубликат записей ({kind}): форма {form_id}, поле {field_id}, "
        f"значение {value!r} — найдено {len(tasks)} задач (id: {ids}). Проверьте, пожалуйста, диалоги."
    )
    logger.warning("Duplicate register entries: %s", text)
    for tid in ids:
        await post_crm_plain_comment(tid, text)


async def warn_cross_duplicate_tg_max_clients(
    tg_id: int,
    max_s: str,
    *,
    t_tg: Optional[list[dict[str, Any]]] = None,
    t_mx: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Две и более карточки клиента по TG и/или по MAX с пересечением по связке."""
    uf = settings.USER_FORM_FIELDS
    if t_tg is None:
        t_tg = await fetch_form_register_tasks(
            settings.CLIENT_FORM_ID, uf["tg_id"], tg_id
        )
    if t_mx is None:
        t_mx = await fetch_form_register_tasks(
            settings.CLIENT_FORM_ID, uf["max_id"], max_s
        )
    if len(t_tg) <= 1 and len(t_mx) <= 1:
        return
    ids: set[int] = set()
    for t in t_tg + t_mx:
        tid = t.get("id")
        if tid is not None:
            try:
                ids.add(int(tid))
            except (TypeError, ValueError):
                pass
    if len(ids) < 2:
        return
    cache_key = f"x:{tg_id}:{max_s}"
    if not _register_dup_warn_should_send(cache_key):
        return
    text = (
        f"⚠️ [Бот] Связка TG+MAX: для tg_id={tg_id} найдено {len(t_tg)} задач(и) клиента, "
        f"для max_id={max_s!r} — {len(t_mx)}. Пересекающиеся id задач: {sorted(ids)}. "
        f"Проверьте дубликаты пользователей с одинаковыми идентификаторами."
    )
    logger.warning("%s", text)
    for tid in sorted(ids):
        await post_crm_plain_comment(tid, text)


async def operator_warn_telegram_flow(
    tg_id: int,
    fields_dict: dict[int, Any],
    *,
    client_tg_tasks: Optional[list[dict[str, Any]]] = None,
    appeal_tg_tasks: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Проверки дубликатов по регистру для сценария Telegram.

    Если переданы client_tg_tasks / appeal_tg_tasks (результат тех же запросов, что и для входа),
    лишние GET к API не выполняются.
    """
    uf = settings.USER_FORM_FIELDS
    rf = settings.REQUEST_FORM_FIELDS
    await warn_if_multiple_tasks_on_register(
        settings.CLIENT_FORM_ID,
        uf["tg_id"],
        tg_id,
        kind="клиенты по Telegram ID",
        tasks=client_tg_tasks,
    )
    await warn_if_multiple_tasks_on_register(
        settings.APPEAL_FORM_ID,
        rf["tg_id"],
        tg_id,
        kind="обращения (диалоги) по Telegram ID",
        tasks=appeal_tg_tasks,
    )
    max_s = _pyrus_field_scalar(fields_dict.get(uf["max_id"]))
    if max_s:
        client_max_tasks = await fetch_form_register_tasks(
            settings.CLIENT_FORM_ID, uf["max_id"], max_s
        )
        appeal_max_tasks = await fetch_form_register_tasks(
            settings.APPEAL_FORM_ID, rf["max_id"], max_s
        )
        await warn_if_multiple_tasks_on_register(
            settings.CLIENT_FORM_ID,
            uf["max_id"],
            max_s,
            kind="клиенты по MAX id",
            tasks=client_max_tasks,
        )
        await warn_if_multiple_tasks_on_register(
            settings.APPEAL_FORM_ID,
            rf["max_id"],
            max_s,
            kind="обращения (диалоги) по MAX id",
            tasks=appeal_max_tasks,
        )
        await warn_cross_duplicate_tg_max_clients(
            tg_id,
            max_s,
            t_tg=client_tg_tasks,
            t_mx=client_max_tasks,
        )


async def operator_warn_max_flow(
    max_user_id: int,
    fields_dict: dict[int, Any],
    *,
    client_max_tasks: Optional[list[dict[str, Any]]] = None,
    appeal_max_tasks: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Проверки дубликатов для сценария MAX.

    Передайте client_max_tasks / appeal_max_tasks из того же запроса, что уже сделан
    при входе (по max_id), чтобы не дублировать GET.
    """
    uf = settings.USER_FORM_FIELDS
    rf = settings.REQUEST_FORM_FIELDS
    if client_max_tasks is None:
        client_max_tasks = await fetch_form_register_tasks(
            settings.CLIENT_FORM_ID, uf["max_id"], max_user_id
        )
    if appeal_max_tasks is None:
        appeal_max_tasks = await fetch_form_register_tasks(
            settings.APPEAL_FORM_ID, rf["max_id"], max_user_id
        )
    await warn_if_multiple_tasks_on_register(
        settings.CLIENT_FORM_ID,
        uf["max_id"],
        max_user_id,
        kind="клиенты по MAX id",
        tasks=client_max_tasks,
    )
    await warn_if_multiple_tasks_on_register(
        settings.APPEAL_FORM_ID,
        rf["max_id"],
        max_user_id,
        kind="обращения (диалоги) по MAX id",
        tasks=appeal_max_tasks,
    )
    tg_s = _pyrus_field_scalar(fields_dict.get(uf["tg_id"]))
    if tg_s:
        try:
            tg_int = int(tg_s)
        except (ValueError, TypeError):
            tg_int = 0
        if tg_int > 0:
            client_tg_tasks = await fetch_form_register_tasks(
                settings.CLIENT_FORM_ID, uf["tg_id"], tg_int
            )
            appeal_tg_tasks = await fetch_form_register_tasks(
                settings.APPEAL_FORM_ID, rf["tg_id"], tg_int
            )
            await warn_if_multiple_tasks_on_register(
                settings.CLIENT_FORM_ID,
                uf["tg_id"],
                tg_int,
                kind="клиенты по Telegram ID (из карточки MAX)",
                tasks=client_tg_tasks,
            )
            await warn_if_multiple_tasks_on_register(
                settings.APPEAL_FORM_ID,
                rf["tg_id"],
                tg_int,
                kind="обращения по Telegram ID (связка из MAX)",
                tasks=appeal_tg_tasks,
            )
            max_scalar = str(max_user_id)
            await warn_cross_duplicate_tg_max_clients(
                tg_int,
                max_scalar,
                t_tg=client_tg_tasks,
                t_mx=client_max_tasks,
            )


async def notify_operators_anomaly(
    task_id: Optional[int],
    text: str,
    *,
    log_event: str = "operator_anomaly",
) -> None:
    """Случайные/редкие ситуации: предупреждение операторам plain-комментарием и в лог."""
    logger.warning("%s: %s", log_event, text)
    if task_id:
        await post_crm_plain_comment(task_id, f"⚠️ [Бот] {text}")
    else:
        record_messaging_failure(
            direction=log_event,
            message=text,
            task_id=None,
        )


async def check_api_element(
    lookup_value: str | int,
    form_id: int,
    field_id: int,
) -> dict[str, Any] | None:
    """Первая задача по регистру формы (как раньше — для обратной совместимости)."""
    tasks = await fetch_form_register_tasks(form_id, field_id, lookup_value)
    if not tasks:
        logger.debug(
            "No tasks found for form field lookup form_id=%s field_id=%s value=%s",
            form_id,
            field_id,
            lookup_value,
        )
        return None
    return tasks[0]



async def get_unique_file_id(
    file_bytes: BytesIO,
    filename: str,
) -> Optional[str]:
    """Uploads a file to Pyrus and returns its unique GUID"""

    try:
        files = {"file": (filename, file_bytes, "application/octet-stream")}

        result = await api_request(
            "POST", "/files/upload", files=files # type: ignore
        )

        if isinstance(result, dict):
            return result.get("guid")

    except Exception as e:
        logger.error(
            f"An error occurred while trying to get a unique ID file #{filename}: {e}"
        )
        return None


async def create_appeal_task(json_data: dict[str, Any]) -> dict[str, Any]:
    """Creates a new appeal task in Pyrus for a user"""
    try:
        assert json_data, "json_data must not be empty"
        result = await api_request(
            method="POST", endpoint="/tasks", json_data=json_data
        )
        if not isinstance(result, dict):
            raise TypeError("Expected dict response")

        if task := result.get("task"):
            logger.info("✅ Appeal task created with ID: %s", task.get("id"))
            return task
        return {}
    except Exception as e:
        logger.error(f"Failed to create appeal task: {e}")
        raise


def build_payload(
    text: Optional[str] = None,
    files: Optional[List[str]] = None,
    channel_type: str = "telegram",
) -> Dict[str, Any]:
    """Generates a payload for sending files via the API"""
    payload: Dict[str, Any] = {
        "channel": {
            "type": channel_type,
        },
    }

    if text:
        payload["text"] = text
    else:
        payload["text"] = "..."

    if files:
        payload["attachments"] = files

    return payload

async def send_comment_in_pyrus(task_id: int, json_data: Dict[str, Any]):
    """Sends a message to Pyrus"""
    result = await api_request("POST", f"/tasks/{task_id}/comments",json_data=json_data)
    task = result.get("task", {}) if isinstance(result, dict) else None
    if task:
        logger.info("The comment was sent successfully!")


def _upload_type_to_attachment_type(ut: UploadType) -> AttachmentType:
    return {
        UploadType.FILE: AttachmentType.FILE,
        UploadType.IMAGE: AttachmentType.IMAGE,
        UploadType.VIDEO: AttachmentType.VIDEO,
        UploadType.AUDIO: AttachmentType.AUDIO,
    }.get(ut, AttachmentType.FILE)


def _max_guess_upload_type(filename: str) -> UploadType:
    ext = (filename or "").rsplit(".", 1)[-1].lower()
    if ext in ("png", "jpg", "jpeg", "gif", "webp", "bmp"):
        return UploadType.IMAGE
    return UploadType.FILE


def _extract_max_upload_token(
    response_text: str,
    token_from_post_uploads: Optional[str],
    upload_type: UploadType,
) -> str:
    """Токен после POST на upload URL; если нет — из ответа POST /uploads (как у video/audio в maxapi)."""
    if upload_type in (UploadType.VIDEO, UploadType.AUDIO) and token_from_post_uploads:
        return token_from_post_uploads

    raw = (response_text or "").strip()
    if raw:
        try:
            j = json.loads(raw)
        except json.JSONDecodeError:
            j = None
        if isinstance(j, dict):
            t = j.get("token")
            if isinstance(t, str) and t:
                return t
            for key in ("access_token", "file_token"):
                v = j.get(key)
                if isinstance(v, str) and v:
                    return v
            pl = j.get("payload")
            if isinstance(pl, dict):
                for key in ("token", "file_token"):
                    v = pl.get(key)
                    if isinstance(v, str) and v:
                        return v
            if upload_type == UploadType.IMAGE:
                photos = j.get("photos")
                if isinstance(photos, dict) and photos:
                    first = next(iter(photos.values()))
                    if isinstance(first, dict):
                        tok = first.get("token")
                        if isinstance(tok, str) and tok:
                            return tok

    if token_from_post_uploads:
        logger.info(
            "MAX upload: используем token из ответа POST /uploads (в ответе после загрузки файла нет token)"
        )
        return token_from_post_uploads

    raise MaxUploadFileFailed(
        "В ответе upload-сервера отсутствует token; в POST /uploads token тоже пуст. "
        f"Ответ[:500]={raw[:500]!r}"
    )


async def _max_buffer_to_attachment(bot: MaxBot, buf: InputMediaBuffer) -> Attachment:
    """Загрузка файла в MAX и сбор Attachment без бага maxapi: только второй ответ JSON с token."""
    from maxapi.utils.message import _get_upload_info, _upload_input_media

    upload = await _get_upload_info(bot=bot, upload_type=buf.type)
    upload_file_response = await _upload_input_media(
        base_connection=bot,
        upload_url=upload.url,
        att=buf,
    )
    token = _extract_max_upload_token(
        upload_file_response,
        upload.token,
        buf.type,
    )
    au = AttachmentUpload(
        type=buf.type,
        payload=AttachmentPayload(token=token),
    )
    return Attachment(type=_upload_type_to_attachment_type(buf.type), payload=au)


async def send_message_to_max_chat(
    bot: MaxBot,
    chat_id: int,
    text: Optional[str],
    attachments: list[dict[str, Any]] | None,
):
    """Sends text and/or files в MAX (Pyrus → user).

    Параметр ``chat_id`` — это MAX **user_id** из поля формы (как в max_handlers), не recipient.chat_id.
    В ``bot.send_message`` всегда передаём ``user_id=``, иначе API ищет **chat** с таким id → chat.not.found.

    Files are uploaded via MAX upload API. По правилам MAX в одном сообщении
    допускается только **одно** файловое вложение — несколько файлов идут
    отдельными сообщениями (без лишней попытки «батча», которая даёт 400).
    """
    try:
        buffers: list[InputMediaBuffer] = []
        if attachments:
            for item in attachments:
                content = item.get("content")
                name = item.get("filename")
                err = item.get("error")
                if err or not content or not name:
                    logger.warning(
                        "Skipping MAX outbound attachment: %s",
                        err or "missing name/content",
                    )
                    continue
                if len(content) > settings.MAX_FILE_SIZE_MAX:
                    logger.warning(
                        "Skipping oversized file for MAX: %s (%d bytes)",
                        name,
                        len(content),
                    )
                    continue
                ut = _max_guess_upload_type(name)
                buffers.append(
                    InputMediaBuffer(buffer=content, filename=name, type=ut)
                )

        if text and text.strip():
            await max_rate_limiter.acquire()
            await bot.send_message(user_id=chat_id, text=text)
            logger.info("Text sent to MAX user #%d", chat_id)

        if not buffers:
            if not text or not str(text).strip():
                logger.info("Nothing to send to MAX user #%d", chat_id)
            return

        async def _upload_all() -> list[Attachment]:
            out: list[Attachment] = []
            for buf in buffers:
                await max_rate_limiter.acquire()
                out.append(await _max_buffer_to_attachment(bot, buf))
            return out

        attachments_ready = await _upload_all()

        if len(attachments_ready) > 1:
            logger.info(
                "MAX: %d вложений — отдельное сообщение на каждый файл (ограничение API)",
                len(attachments_ready),
            )
            for one in attachments_ready:
                await max_rate_limiter.acquire()
                await bot.send_message(
                    user_id=chat_id,
                    text=None,
                    attachments=[one],
                    sleep_after_input_media=False,
                )
            logger.info(
                "Sent %d file(s) in separate messages to MAX user #%d",
                len(attachments_ready),
                chat_id,
            )
        else:
            await max_rate_limiter.acquire()
            await bot.send_message(
                user_id=chat_id,
                text=None,
                attachments=attachments_ready,
                sleep_after_input_media=False,
            )
            logger.info(
                "Sent %d file(s) in one message to MAX user #%d",
                len(attachments_ready),
                chat_id,
            )
    except Exception as e:
        logger.exception("Failed to send message to MAX chat %s: %s", chat_id, e)
        raise

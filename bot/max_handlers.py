import asyncio
import io
import logging
import uuid
from pathlib import Path
from typing import Any
from weakref import WeakValueDictionary

import httpx
from maxapi import Bot, Dispatcher
from maxapi.context.state_machine import State, StatesGroup
from maxapi.enums.attachment import AttachmentType
from maxapi.types import CallbackButton, Command, MessageCallback, MessageCreated
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from config import settings
from max_rate_limiter import max_rate_limiter
from resource_limits import (
    acquire_user_file_slot,
    acquire_user_message_slot,
    get_download_semaphore,
)
from utils import (
    build_payload,
    check_api_element,
    client_telephone_from_fields,
    create_appeal_task,
    create_user_task,
    ensure_max_id_on_client_task,
    extra_appeal_fields_from_client_card,
    fetch_form_register_tasks,
    find_client_tasks_by_phone,
    get_unique_file_id,
    notify_operators_anomaly,
    open_chats_after_appeal,
    operator_warn_max_flow,
    post_crm_plain_comment,
    prepare_fields_to_dict,
    record_messaging_failure,
    send_comment_in_pyrus,
    sync_appeal_max_id_for_max_phone_login,
    warn_if_multiple_tasks_on_register,
)

logger = logging.getLogger(__name__)

_user_locks: WeakValueDictionary = WeakValueDictionary()  # type: ignore[assignment]
_locks_map_guard = asyncio.Lock()


async def get_user_lock(user_id: int) -> asyncio.Lock:
    async with _locks_map_guard:
        lock = _user_locks.get(user_id)  # type: ignore[arg-type]
        if lock is None:
            lock = asyncio.Lock()
            _user_locks[user_id] = lock  # type: ignore[index]
        return lock  # type: ignore[return-value]


class RegistrationState(StatesGroup):
    """Телефон одним сообщением, затем ФИО, если карточка по телефону не найдена."""

    input_telephone = State()
    input_fullname = State()


_MAX_PHONE_PROMPT = "📱 Укажите номер телефона — мы проверим, есть ли вы в системе."

_MAX_AUTH_SUCCESS = "✅ Вы успешно авторизованы. Добро пожаловать!"


def _registration_markup():
    kb = InlineKeyboardBuilder()
    kb.row(CallbackButton(text="📝 Зарегистрироваться", payload="register"))
    return kb.as_markup()


# У MAX/CDN в пути часто один символ (например .../i) — в CRM уезжало имя «i».
_JUNK_URL_BASENAMES = frozenset(
    {
        "i",
        "o",
        "a",
        "u",
        "x",
        "n",
        "d",
        "p",
        "image",
        "img",
        "download",
        "file",
        "photo",
        "pic",
        "data",
        "get",
        "blob",
    }
)


def _filename_from_url(url: str, fallback: str) -> str:
    raw = Path((url or "").split("?")[0]).name
    if not raw or not raw.strip():
        return fallback
    base = raw.strip()
    stem = Path(base).stem.lower()
    if len(base) <= 2:
        return fallback
    if len(stem) <= 1 and stem.isalpha():
        return fallback
    if stem in _JUNK_URL_BASENAMES:
        return fallback
    return base


def _extension_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    main = content_type.split(";")[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "video/mp4": ".mp4",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/octet-stream": None,
    }.get(main)


def _extension_from_magic(head: bytes) -> str | None:
    """Расширение по началу файла (CDN вроде i.oneme.ru/i?r=… не даёт имени и типа в URL)."""
    if len(head) < 12:
        return None
    if head[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return ".gif"
    if head[:4] == b"RIFF" and len(head) >= 12 and head[8:12] == b"WEBP":
        return ".webp"
    if head[:4] == b"%PDF":
        return ".pdf"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return ".mp4"
    return None


def _finalize_filename_for_pyrus(
    filename: str,
    data: bytes,
    content_type: str | None,
) -> str:
    sniff = _extension_from_magic(data[: min(128, len(data))])
    hdr = _extension_from_content_type(content_type)
    ext = sniff or hdr
    p = Path(filename)
    stem = p.stem or "max_file"
    if ext:
        return stem + ext
    if p.suffix:
        return filename
    return stem + ".bin"


def _extract_attachment_url_and_name(attachment: Any) -> tuple[str | None, str]:
    atype = getattr(attachment, "type", None)

    if atype == AttachmentType.IMAGE:
        payload = getattr(attachment, "payload", None)
        url = getattr(payload, "url", None)
        photo_id = getattr(payload, "photo_id", None) or 0
        token = getattr(payload, "token", None) if payload else None
        parts = ["max_image", str(photo_id)]
        if token:
            parts.append(str(token)[:32])
        parts.append(uuid.uuid4().hex[:8])
        # Расширение доберём после скачивания (magic / Content-Type); здесь — только stem.
        fallback = "-".join(parts)
        return url, _filename_from_url(url or "", fallback)

    if atype == AttachmentType.FILE:
        payload = getattr(attachment, "payload", None)
        url = getattr(payload, "url", None)
        filename = getattr(attachment, "filename", None) or _filename_from_url(
            url or "", "file.bin"
        )
        return url, filename

    if atype == AttachmentType.AUDIO:
        payload = getattr(attachment, "payload", None)
        url = getattr(payload, "url", None)
        return url, _filename_from_url(url or "", "audio.mp3")

    if atype == AttachmentType.STICKER:
        payload = getattr(attachment, "payload", None)
        url = getattr(payload, "url", None)
        code = getattr(payload, "code", "sticker")
        return url, _filename_from_url(url or "", f"{code}.webp")

    if atype == AttachmentType.VIDEO:
        urls = getattr(attachment, "urls", None)
        url = None
        if urls:
            url = (
                getattr(urls, "mp4_1080", None)
                or getattr(urls, "mp4_720", None)
                or getattr(urls, "mp4_480", None)
                or getattr(urls, "mp4_360", None)
                or getattr(urls, "mp4_240", None)
            )
        token = getattr(attachment, "token", "video")
        return url, _filename_from_url(url or "", f"{token}.mp4")

    return None, "unknown.bin"


async def _download_max_attachment_to_bytes(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
) -> tuple[bytes | None, str | None]:
    """Тело ответа и Content-Type (для расширения, если URL без имени файла)."""
    try:
        resp = await client.get(
            url,
            headers=headers,
            timeout=httpx.Timeout(60.0, connect=20.0),
            follow_redirects=True,
        )
        resp.raise_for_status()
        ct = resp.headers.get("content-type")
        return resp.content, ct
    except Exception as e:
        logger.warning("Failed to download MAX attachment %s: %s", url, e)
        return None, None


async def _process_one_max_attachment_to_pyrus(
    client: httpx.AsyncClient,
    attachment: Any,
    *,
    headers: dict[str, str],
) -> list[str]:
    url, filename = _extract_attachment_url_and_name(attachment)
    token = getattr(getattr(attachment, "payload", None), "token", None) or getattr(
        attachment, "token", None
    )
    if not url and not token:
        logger.warning("MAX attachment without url/token: %s", filename)
        return []
    candidate_urls: list[str] = []
    if url:
        candidate_urls.append(url)
    if token:
        candidate_urls.append(f"https://platform-api.max.ru/files/{token}")
        candidate_urls.append(f"https://platform-api.max.ru/files/{token}?download=true")
        candidate_urls.append(f"https://platform-api.max.ru/uploads/{token}")
    data: bytes | None = None
    content_type: str | None = None
    for u in candidate_urls:
        data, content_type = await _download_max_attachment_to_bytes(client, u, headers=headers)
        if data:
            break
    if not data:
        logger.warning("No bytes for MAX attachment %s", filename)
        return []
    upload_name = _finalize_filename_for_pyrus(filename, data, content_type)
    if upload_name != filename:
        logger.debug(
            "MAX→Pyrus: имя файла %r → %r (Content-Type=%r)",
            filename,
            upload_name,
            content_type,
        )
    bio = io.BytesIO(data)
    guid = await get_unique_file_id(bio, upload_name)
    if guid:
        return [guid]
    logger.warning("Pyrus upload returned no guid for %s", filename)
    return []


async def _download_and_upload_attachments_to_pyrus(attachments: list[Any]) -> list[str]:
    if not attachments:
        return []
    max_token = settings.MAX_BOT_TOKEN or settings.BOT_TOKEN
    headers = {"Authorization": f"Bearer {max_token}"}
    sem = get_download_semaphore()

    async with httpx.AsyncClient() as client:

        async def one(att: Any) -> list[str]:
            async with sem:
                return await _process_one_max_attachment_to_pyrus(client, att, headers=headers)

        results = await asyncio.gather(
            *[one(a) for a in attachments],
            return_exceptions=True,
        )

    uploaded_guids: list[str] = []
    for r in results:
        if isinstance(r, BaseException):
            logger.warning("MAX attachment pipeline failed: %s", r)
            continue
        uploaded_guids.extend(r)
    return uploaded_guids


def register_max_handlers(dp: Dispatcher, bot: Bot) -> None:
    client_user_field = settings.USER_FORM_FIELDS["max_id"]
    appeal_user_field = settings.REQUEST_FORM_FIELDS["max_id"]

    @dp.bot_started()
    async def on_bot_started(event):
        await max_rate_limiter.acquire()
        await bot.send_message(
            chat_id=event.chat_id,
            text="Привет! Для начала работы нужно зарегистрироваться.",
            attachments=[_registration_markup()],
        )

    @dp.message_created(Command("start"))
    async def on_start(event: MessageCreated):
        await max_rate_limiter.acquire()
        await event.message.answer(
            "ℹ️ Вы не зарегистрированы. Пожалуйста, зарегистрируйтесь, чтобы продолжить.",
            attachments=[_registration_markup()],
        )

    @dp.message_callback()
    async def on_callback(event: MessageCallback, context: Any):
        payload = event.callback.payload
        user_id = event.callback.user.user_id
        chat_id = event.message.recipient.chat_id
        if not user_id or not chat_id:
            return

        if payload == "register":
            await context.clear()
            await context.set_state(RegistrationState.input_telephone)
            await max_rate_limiter.acquire()
            await bot.send_message(chat_id=chat_id, text=_MAX_PHONE_PROMPT)
            await event.answer()
            return

    @dp.message_created(RegistrationState.input_telephone)
    async def input_telephone_handler(event: MessageCreated, context: Any):
        await max_rate_limiter.acquire()
        body = event.message.body
        user_id = event.message.sender.user_id
        chat_id = event.message.recipient.chat_id
        text = event.message.body.text if body else None
        if not user_id or not chat_id:
            return
        if not text or not str(text).strip():
            await event.message.answer("❌ Номер не может быть пустым. Отправьте номер телефона.")
            return

        phone = str(text).strip()

        await event.message.answer("⏳ Проверяем ваш номер. Пожалуйста подождите…")
        lock = await get_user_lock(user_id)
        async with lock:
            await acquire_user_message_slot("max_messenger", user_id)
            tel_fid = settings.USER_FORM_FIELDS["telephone"]
            matches = await find_client_tasks_by_phone(
                settings.CLIENT_FORM_ID,
                tel_fid,
                phone,
            )
            if matches:
                await warn_if_multiple_tasks_on_register(
                    settings.CLIENT_FORM_ID,
                    tel_fid,
                    phone,
                    kind="телефон (MAX, вход)",
                    tasks=matches,
                )
                client = matches[0]
                cid = client.get("id")
                card_fields = prepare_fields_to_dict(client.get("fields") or [])
                if cid is not None:
                    await ensure_max_id_on_client_task(int(cid), user_id, card_fields)

                tel_for_search = client_telephone_from_fields(card_fields)
                appeal_phone_fid = settings.REQUEST_FORM_FIELDS["telephone"]
                appeals = await find_client_tasks_by_phone(
                    settings.APPEAL_FORM_ID,
                    appeal_phone_fid,
                    tel_for_search,
                    phone,
                )
                extra_msg = ""
                if appeals:
                    await warn_if_multiple_tasks_on_register(
                        settings.APPEAL_FORM_ID,
                        appeal_phone_fid,
                        tel_for_search or phone,
                        kind="телефон (MAX, обращение при входе)",
                        tasks=appeals,
                    )
                    extra_msg = await sync_appeal_max_id_for_max_phone_login(
                        appeals[0],
                        user_id,
                        card_fields,
                        phone_label=tel_for_search or phone,
                    )
                else:
                    logger.info(
                        "MAX phone login: no appeal for client_id=%s phone=%r",
                        cid,
                        tel_for_search or phone,
                    )
                    extra_msg = (
                        "\n\nℹ️ Обращение с этим телефоном не найдено — "
                        "обновлена только карточка клиента."
                    )

                await event.message.answer(_MAX_AUTH_SUCCESS + extra_msg)
                await context.clear()
                return

        await context.update_data(telephone=phone)
        await context.set_state(RegistrationState.input_fullname)
        await event.message.answer("👤 Введите ваше полное имя:")

    @dp.message_created(RegistrationState.input_fullname)
    async def input_fullname_handler(event: MessageCreated, context: Any):
        await max_rate_limiter.acquire()
        body = event.message.body
        user_id = event.message.sender.user_id
        chat_id = event.message.recipient.chat_id
        text = event.message.body.text if body else None
        if not user_id or not chat_id:
            return
        if not text or not str(text).strip():
            await event.message.answer("❌ Имя пустое. Введите ФИО ещё раз:")
            return

        fullname = str(text).strip()
        data = await context.get_data()
        telephone = (data.get("telephone") or "").strip()
        if not telephone:
            await event.message.answer(
                "❌ Сессия регистрации устарела. Нажмите «Зарегистрироваться» снова."
            )
            await context.clear()
            return

        await event.message.answer("⏳ Пожалуйста подождите. Идет регистрация...")
        lock = await get_user_lock(user_id)
        async with lock:
            await acquire_user_message_slot("max_messenger", user_id)
            existing_user = await check_api_element(
                user_id, settings.CLIENT_FORM_ID, client_user_field
            )
            pyrus_user_id = existing_user.get("id") if existing_user else None

            if not pyrus_user_id:
                json_data: dict[str, Any] = {
                    "form_id": settings.CLIENT_FORM_ID,
                    "fields": [
                        {"id": settings.USER_FORM_FIELDS["fullname"], "value": fullname},
                        {"id": settings.USER_FORM_FIELDS["telephone"], "value": telephone},
                        {"id": settings.USER_FORM_FIELDS["max_id"], "value": user_id},
                    ],
                }
                created_user = await create_user_task(json_data)
                pyrus_user_id = created_user.get("id")

            if not pyrus_user_id:
                await event.message.answer("❌ Ошибка регистрации. Попробуйте позже.")
                await context.clear()
                return

            client_card = await check_api_element(
                user_id, settings.CLIENT_FORM_ID, client_user_field
            )
            card_fields: dict[int, Any] = {}
            if client_card and client_card.get("fields"):
                card_fields = prepare_fields_to_dict(client_card["fields"])

            existing_task = await check_api_element(
                user_id, settings.APPEAL_FORM_ID, appeal_user_field
            )
            task_id = existing_task.get("id") if existing_task else None

            if not task_id:
                appeal_fields: list[dict[str, Any]] = [
                    {"id": settings.REQUEST_FORM_FIELDS["fio"], "value": fullname},
                    {"id": settings.REQUEST_FORM_FIELDS["telephone"], "value": telephone},
                    {"id": appeal_user_field, "value": user_id},
                ]
                appeal_fields.extend(
                    extra_appeal_fields_from_client_card(
                        card_fields, source_channel="max_messenger"
                    )
                )
                json_data = {
                    "form_id": settings.APPEAL_FORM_ID,
                    "fields": appeal_fields,
                }
                created_task = await create_appeal_task(json_data)
                task_id = created_task.get("id")

            if task_id:
                await open_chats_after_appeal(
                    task_id,
                    source_channel="max_messenger",
                    fields_dict=card_fields,
                )
                await operator_warn_max_flow(user_id, card_fields)
                await event.message.answer(
                    f"✅ Регистрация завершена успешно!\n\nСпасибо за регистрацию, {fullname}!",
                )
            else:
                await event.message.answer("❌ Ошибка при создании обращения.")
                await context.clear()
                return
        await context.clear()

    @dp.message_created(None)
    async def on_message(event: MessageCreated):
        body = event.message.body
        if not body:
            await event.message.answer("Произошла ошибка. Попробуйте позже!")
            return
        await max_rate_limiter.acquire()
        user_id = event.message.sender.user_id
        chat_id = event.message.recipient.chat_id
        text = body.text
        attachments = body.attachments or []
        if not user_id or not chat_id:
            return

        if not text and not attachments:
            await event.message.answer("Поддерживаются только текст или вложения.")
            return

        lock = await get_user_lock(user_id)
        async with lock:
            await acquire_user_message_slot("max_messenger", user_id)
            for _ in range(len(attachments or [])):
                await acquire_user_file_slot("max_messenger", user_id)

            async def _fetch_client_register_tasks() -> list[dict[str, Any]]:
                try:
                    return await fetch_form_register_tasks(
                        settings.CLIENT_FORM_ID, client_user_field, user_id
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 403:
                        return []
                    raise

            ut_res, ap_res = await asyncio.gather(
                _fetch_client_register_tasks(),
                fetch_form_register_tasks(
                    settings.APPEAL_FORM_ID, appeal_user_field, user_id
                ),
                return_exceptions=True,
            )
            if isinstance(ut_res, BaseException):
                if isinstance(ut_res, httpx.HTTPStatusError):
                    logger.error("HTTP error (client register) for user %s: %s", user_id, ut_res)
                    await event.message.answer("❌ Ошибка при обработке запроса. Попробуйте позже.")
                    return
                raise ut_res
            if isinstance(ap_res, BaseException):
                if isinstance(ap_res, httpx.HTTPStatusError):
                    logger.error("HTTP error (appeal register) for user %s: %s", user_id, ap_res)
                    await event.message.answer("❌ Ошибка при обработке запроса. Попробуйте позже.")
                    return
                raise ap_res

            user_tasks: list[dict[str, Any]] = ut_res
            appeal_tasks: list[dict[str, Any]] = ap_res
            user = user_tasks[0] if user_tasks else None
            pyrus_user_id = user.get("id") if user else None
            if not pyrus_user_id:
                await event.message.answer(
                    "ℹ️ Вы не зарегистрированы. Нажмите кнопку ниже для регистрации.",
                    attachments=[_registration_markup()],
                )
                return

            fields = user.get("fields", [])
            if not fields:
                await event.message.answer(
                    "❌ Ошибка: не удалось получить данные профиля. Попробуйте позже."
                )
                await notify_operators_anomaly(
                    int(pyrus_user_id) if pyrus_user_id else None,
                    f"У пользователя MAX user_id={user_id} пустой список полей в карточке клиента Pyrus.",
                    log_event="empty_client_fields",
                )
                return
            fields_dict = prepare_fields_to_dict(fields)
            fullname = fields_dict.get(settings.USER_FORM_FIELDS["fullname"], "Пользователь")
            telephone = fields_dict.get(settings.USER_FORM_FIELDS["telephone"], "Не указан")

            existing_task = appeal_tasks[0] if appeal_tasks else None
            task_id = existing_task.get("id") if existing_task else None

            if not task_id:
                appeal_fields: list[dict[str, Any]] = [
                    {"id": appeal_user_field, "value": user_id},
                    {"id": settings.REQUEST_FORM_FIELDS["fio"], "value": fullname},
                    {
                        "id": settings.REQUEST_FORM_FIELDS["telephone"],
                        "value": telephone,
                    },
                ]
                appeal_fields.extend(
                    extra_appeal_fields_from_client_card(
                        fields_dict, source_channel="max_messenger"
                    )
                )
                json_data: dict[str, Any] = {
                    "form_id": settings.APPEAL_FORM_ID,
                    "fields": appeal_fields,
                }
                result = await create_appeal_task(json_data)
                task_id = result.get("id")

                if not task_id:
                    await event.message.answer("❌ Ошибка при создании обращения.")
                    return

                await open_chats_after_appeal(
                    task_id,
                    source_channel="max_messenger",
                    fields_dict=fields_dict,
                )

            await operator_warn_max_flow(
                user_id,
                fields_dict,
                client_max_tasks=user_tasks,
                appeal_max_tasks=appeal_tasks,
            )

            files = await _download_and_upload_attachments_to_pyrus(attachments)
            payload = build_payload(
                text=text,
                files=files if files else None,
                channel_type="max_messenger",
            )
            try:
                await send_comment_in_pyrus(task_id, payload)
            except Exception as e:
                logger.exception("send_comment_in_pyrus failed task_id=%s: %s", task_id, e)
                record_messaging_failure(
                    direction="user_to_crm",
                    task_id=task_id,
                    channel="max_messenger",
                    message=str(e),
                )
                await post_crm_plain_comment(
                    task_id,
                    f"Не удалось принять сообщение пользователя в CRM (MAX): {e}",
                )
                await event.message.answer(
                    "❌ Сообщение не дошло до CRM. Попробуйте позже."
                )

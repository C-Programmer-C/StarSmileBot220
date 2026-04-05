import asyncio
import logging
from typing import Any
from weakref import WeakValueDictionary
import httpx
from aiogram import F, Router, types
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.process_message import process_media_group, process_single_comment
from config import settings
from resource_limits import (
    acquire_user_file_slot,
    acquire_user_message_slot,
    telegram_attachment_units,
)
from utils import (
    check_api_element,
    create_appeal_task,
    create_user_task,
    extra_appeal_fields_from_client_card,
    fetch_form_register_tasks,
    notify_operators_anomaly,
    open_chats_after_appeal,
    operator_warn_telegram_flow,
    prepare_fields_to_dict,
)

logger = logging.getLogger(__name__)

start_router = Router()

_user_locks = WeakValueDictionary()  # type: ignore

_locks_map_guard = asyncio.Lock()

_pending_tasks: dict[int, int] = {}

_pending_tasks_guard = asyncio.Lock()

_pending_users: dict[int, int] = {}

_pending_users_guard = asyncio.Lock()


async def get_user_lock(tg_id: int) -> asyncio.Lock:
    async with _locks_map_guard:
        lock = _user_locks.get(tg_id)  # type: ignore
        if lock is None:
            lock = asyncio.Lock()
            _user_locks[tg_id] = lock
        return lock  # type: ignore


async def get_pending_task_id(tg_id: int) -> int | None:
    async with _pending_tasks_guard:
        return _pending_tasks.get(tg_id)


async def set_pending_task_id(tg_id: int, task_id: int) -> None:
    async with _pending_tasks_guard:
        _pending_tasks[tg_id] = task_id


async def clear_pending_task_id(tg_id: int) -> None:
    async with _pending_tasks_guard:
        _pending_tasks.pop(tg_id, None)


async def get_pending_user_id(tg_id: int) -> int | None:
    async with _pending_users_guard:
        return _pending_users.get(tg_id)


async def set_pending_user_id(tg_id: int, user_id: int) -> None:
    async with _pending_users_guard:
        _pending_users[tg_id] = user_id


async def clear_pending_user_id(tg_id: int) -> None:
    async with _pending_users_guard:
        _pending_users.pop(tg_id, None)


class RegistrationState(StatesGroup):
    input_fullname = State()
    input_telephone = State()


@start_router.message(StateFilter(None))
async def message_text_handler(message: Message):
    
    if not message.from_user:
        message.answer(
            "❌ Ошибка: не удалось получить информацию о пользователе. Попробуйте еще раз."
        )
        return

    tg_id = message.from_user.id

    if not tg_id:
        message.answer(
            "❌ Ошибка: не удалось получить идентификатор пользователя. Попробуйте еще раз."
        )
        return

    lock = await get_user_lock(tg_id)

    async with lock:
        await acquire_user_message_slot("telegram", tg_id)
        if not message.media_group_id:
            for _ in range(telegram_attachment_units(message)):
                await acquire_user_file_slot("telegram", tg_id)

        user_tasks: list[dict[str, Any]] = []
        appeal_tasks: list[dict[str, Any]] = []
        pending_task_id: int | None = None

        async def _fetch_client_register_tasks() -> list[dict[str, Any]]:
            try:
                return await fetch_form_register_tasks(
                    settings.CLIENT_FORM_ID,
                    settings.USER_FORM_FIELDS["tg_id"],
                    tg_id,
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    return []
                raise

        ut_res, ap_res, pd_res = await asyncio.gather(
            _fetch_client_register_tasks(),
            fetch_form_register_tasks(
                settings.APPEAL_FORM_ID,
                settings.REQUEST_FORM_FIELDS["tg_id"],
                tg_id,
            ),
            get_pending_task_id(tg_id),
            return_exceptions=True,
        )

        if isinstance(ut_res, BaseException):
            if isinstance(ut_res, httpx.HTTPStatusError):
                logger.error(f"HTTP error (client register) for user {tg_id}: {ut_res}")
                await message.answer(
                    "❌ Ошибка при обработке запроса. Пожалуйста, попробуйте позже."
                )
                raise ut_res
            raise ut_res
        if isinstance(ap_res, BaseException):
            if isinstance(ap_res, httpx.HTTPStatusError):
                logger.error(f"HTTP error (appeal register) for user {tg_id}: {ap_res}")
                await message.answer(
                    "❌ Ошибка при обработке запроса. Пожалуйста, попробуйте позже."
                )
                raise ap_res
            raise ap_res
        if isinstance(pd_res, BaseException):
            raise pd_res

        user_tasks = ut_res
        appeal_tasks = ap_res
        pending_task_id = pd_res
        user = user_tasks[0] if user_tasks else None

        user_id = user.get("id") if user else None

        if not user_id:
            await message.answer(
                "ℹ️ Вы не зарегистрированы. Пожалуйста, зарегистрируйтесь, чтобы продолжить.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="📝 Зарегистрироваться", callback_data="register"
                            )
                        ],
                    ],
                    resize_keyboard=True,
                ),
            )
            return

        assert user

        logger.info("User %s found in the system. User ID: %s", tg_id, user_id)

        fields = user.get("fields", [])

        if not fields:
            await message.answer(
                "❌ Ошибка: не удалось получить данные пользователя. Попробуйте еще раз."
            )
            await notify_operators_anomaly(
                int(user_id) if user_id else None,
                f"У пользователя Telegram tg_id={tg_id} пустой список полей в карточке клиента Pyrus.",
                log_event="empty_client_fields",
            )
            return

        task = appeal_tasks[0] if appeal_tasks else None

        task_id = task.get("id") if task else None

        if not task_id and pending_task_id:
            task_id = pending_task_id
            logger.info(
                f"Using pending task ID {task_id} from cache for user {tg_id}"
            )

        logger.info(
            f"Processing message from user {tg_id}, existing appeal task ID: {task_id}"
        )

        fields_dict = prepare_fields_to_dict(fields)

        if not task_id:

            pending_task_id = await get_pending_task_id(tg_id)
            if pending_task_id:
                task_id = pending_task_id
                logger.info(
                    f"Found pending task {task_id} for user {tg_id} on second check"
                )
            elif appeal_tasks:
                task_id = appeal_tasks[0].get("id")
                if task_id:
                    logger.info(
                        f"Found existing appeal task {task_id} for user {tg_id} (из первого запроса к реестру)"
                    )

            if task_id:
                await open_chats_after_appeal(
                    task_id,
                    source_channel="telegram",
                    fields_dict=fields_dict,
                )
                logger.info(
                    f"The chat(s) for task #{task_id} have been successfully opened"
                )
            else:
                logger.warning(
                    f"No existing appeal task for user {tg_id}. Creating a new one."
                )

                fullname = fields_dict.get(
                    settings.USER_FORM_FIELDS["fullname"], "Пользователь"
                )
                telephone = fields_dict.get(
                    settings.USER_FORM_FIELDS["telephone"], "Не указан"
                )
                tg_account = fields_dict.get(
                    settings.USER_FORM_FIELDS["tg_account"], "Не указан"
                )

                appeal_fields: list[dict[str, Any]] = [
                    {"id": settings.REQUEST_FORM_FIELDS["tg_id"], "value": tg_id},
                    {"id": settings.REQUEST_FORM_FIELDS["fio"], "value": fullname},
                    {
                        "id": settings.REQUEST_FORM_FIELDS["telephone"],
                        "value": telephone,
                    },
                    {
                        "id": settings.REQUEST_FORM_FIELDS["tg_account"],
                        "value": tg_account,
                    },
                ]
                appeal_fields.extend(
                    extra_appeal_fields_from_client_card(
                        fields_dict, source_channel="telegram"
                    )
                )

                json_data: dict[str, Any] = {
                    "form_id": settings.APPEAL_FORM_ID,
                    "fields": appeal_fields,
                }

                result = await create_appeal_task(json_data)

                created_task_id = result.get("id")

                if result and created_task_id:
                    logger.info(
                        f"Created new appeal task ID {created_task_id} for user {tg_id}"
                    )
                    await set_pending_task_id(tg_id, created_task_id)
                else:
                    logger.error(f"Failed to create appeal task for user {tg_id}")
                    await message.answer(
                        "❌ Ошибка при создании обращения. Пожалуйста, попробуйте позже."
                    )
                    return

                task_id = created_task_id

                max_retries = 5
                for attempt in range(max_retries):
                    await asyncio.sleep(0.2 * (attempt + 1))

                    task = await check_api_element(
                        tg_id, settings.APPEAL_FORM_ID, settings.REQUEST_FORM_FIELDS["tg_id"]
                    )

                    found_task_id = task.get("id") if task else None

                    if found_task_id:
                        if found_task_id != created_task_id:
                            logger.warning(
                                f"Another task {found_task_id} was found for user {tg_id} after creating {created_task_id}. Using existing task."
                            )
                            await clear_pending_task_id(tg_id)
                            await notify_operators_anomaly(
                                int(found_task_id),
                                f"Коллизия обращений: создана задача {created_task_id}, API вернул другую {found_task_id}. Проверьте дубликаты диалогов (tg_id={tg_id}).",
                                log_event="appeal_id_collision",
                            )
                            await notify_operators_anomaly(
                                int(created_task_id),
                                f"Коллизия обращений: индекс указывает на задачу {found_task_id}, только что создана {created_task_id}. Проверьте дубликаты (tg_id={tg_id}).",
                                log_event="appeal_id_collision",
                            )
                        task_id = found_task_id
                        break

                if task_id == created_task_id:
                    logger.info(
                        f"Task {created_task_id} not found in API after {max_retries} attempts. Using created task ID from cache."
                    )

                await open_chats_after_appeal(
                    task_id,
                    source_channel="telegram",
                    fields_dict=fields_dict,
                )
                logger.info(
                    f"The chat(s) for task #{task_id} have been successfully opened"
                )

                await clear_pending_task_id(tg_id)

    if not task_id:
        await message.answer(
            "❌ Произошла ошибка при попытке получить данные в обращении."
        )
        return

    await operator_warn_telegram_flow(
        tg_id,
        fields_dict,
        client_tg_tasks=user_tasks,
        appeal_tg_tasks=appeal_tasks,
    )

    if message.media_group_id:
        logger.info(
            f"The media group #{message.media_group_id} processing for task #{task_id} process has begun"
        )
        await process_media_group(message, tg_id, task_id)
    else:
        logger.info(
            f"The message #{message.message_id} processing for task #{task_id} process has begun"
        )
        await process_single_comment(message, task_id)


@start_router.callback_query(F.data == "register", StateFilter(None))
async def register_callback_handler(
    callback_query: types.CallbackQuery, state: FSMContext
):
    if not callback_query.message:
        await callback_query.answer(
            "❌ Ошибка: не удалось получить сообщение. Попробуйте еще раз."
        )
        return
    await callback_query.message.answer("📝 Пожалуйста, введите ваше полное имя:")
    await state.set_state(RegistrationState.input_fullname)
    await callback_query.answer()


@start_router.message(RegistrationState.input_fullname)
async def input_fullname_handler(message: Message, state: FSMContext):
    if not message.text:
        await message.answer(
            "❌ Ошибка: не удалось получить ваше имя. Пожалуйста, введите повторно ваше полное имя:"
        )
        return
    fullname = message.text.strip()
    await state.update_data(fullname=fullname)
    await message.answer("📝 Пожалуйста, введите ваш номер телефона:")
    await state.set_state(RegistrationState.input_telephone)


@start_router.message(RegistrationState.input_telephone)
async def input_telephone_handler(message: Message, state: FSMContext):
    if not message.text:
        await message.answer(
            "❌ Ошибка: не удалось получить ваш номер телефона. Пожалуйста, введите ваш номер телефона:"
        )
        return

    if not message.from_user:
        await message.answer(
            "❌ Ошибка: не удалось получить информацию о пользователе. Попробуйте еще раз."
        )
        return
    data = await state.get_data()
    telephone = message.text.strip()
    fullname = data.get("fullname")
    tg_id = message.from_user.id
    tg_account = message.from_user.username

    await message.answer("⏳ Пожалуйста подождите. Идет регистрация...")

    lock = await get_user_lock(tg_id)

    async with lock:
        pending_user_id = await get_pending_user_id(tg_id)
        
        try:
            existing_user = await check_api_element(
                tg_id, settings.CLIENT_FORM_ID, settings.USER_FORM_FIELDS["tg_id"]
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                logger.warning(f"Access denied for user {tg_id}: {e}")
                existing_user = None
            else:
                logger.error(f"HTTP error for user {tg_id}: {e}")
                await message.answer(
                    "❌ Ошибка при обработке запроса. Пожалуйста, попробуйте позже."
                )
                await state.clear()
                return
        except Exception as e:
            logger.error(f"Unexpected error for user {tg_id}: {e}")
            await message.answer(
                "❌ Ошибка при обработке запроса. Пожалуйста, попробуйте позже."
            )
            await state.clear()
            return

        user_id = existing_user.get("id") if existing_user else None

        if not user_id and pending_user_id:
            user_id = pending_user_id
            logger.info(
                f"Using pending user ID {user_id} from cache for user {tg_id}"
            )

        if user_id:
            logger.warning(
                f"User {tg_id} already exists with ID {user_id}. Skipping registration."
            )
            await message.answer(
                "ℹ️ Вы уже зарегистрированы. Можете отправлять сообщения."
            )
            await state.clear()
            return

        pending_user_id = await get_pending_user_id(tg_id)
        if pending_user_id:
            user_id = pending_user_id
            logger.info(
                f"Found pending user {user_id} for TG ID {tg_id} on second check"
            )
        else:
            existing_user = await check_api_element(
                tg_id, settings.CLIENT_FORM_ID, settings.USER_FORM_FIELDS["tg_id"]
            )
            user_id = existing_user.get("id") if existing_user else None

        if user_id:
            logger.warning(
                f"User {tg_id} already exists with ID {user_id}. Skipping registration."
            )
            await message.answer(
                "ℹ️ Вы уже зарегистрированы. Можете отправлять сообщения."
            )
            await state.clear()
            return

        json_data: dict[str, Any] = {
            "form_id": settings.CLIENT_FORM_ID,
            "fields": [
                {"id": settings.USER_FORM_FIELDS["fullname"], "value": fullname},
                {"id": settings.USER_FORM_FIELDS["telephone"], "value": telephone},
                {"id": settings.USER_FORM_FIELDS["tg_account"], "value": tg_account},
                {"id": settings.USER_FORM_FIELDS["tg_id"], "value": tg_id},
            ],
        }
        logger.info(
            f"User registration: {fullname}, Phone: {telephone}, TG ID: {tg_id}, TG Account: {tg_account}"
        )

        try:
            user_data = await create_user_task(json_data)
        except Exception as e:
            await message.answer(
                "❌ Ошибка при попытке регистрации. Пожалуйста, попробуйте позже."
            )
            logger.error(f"Error creating user task for {tg_id}: {e}")
            await state.clear()
            return

        created_user_id = user_data.get("id")

        if not created_user_id:
            await message.answer(
                "❌ Ошибка: не удалось зарегистрировать пользователя. Попробуйте еще раз."
            )
            await state.clear()
            return

        await set_pending_user_id(tg_id, created_user_id)
        user_id = created_user_id

        max_retries = 5
        for attempt in range(max_retries):
            await asyncio.sleep(0.2 * (attempt + 1))

            existing_user = await check_api_element(
                tg_id, settings.CLIENT_FORM_ID, settings.USER_FORM_FIELDS["tg_id"]
            )

            found_user_id = existing_user.get("id") if existing_user else None

            if found_user_id:
                if found_user_id != created_user_id:
                    logger.warning(
                        f"Another user {found_user_id} was found for TG ID {tg_id} after creating {created_user_id}. Using existing user."
                    )
                    await clear_pending_user_id(tg_id)
                    await notify_operators_anomaly(
                        int(found_user_id),
                        f"Коллизия карточек клиента: создан id {created_user_id}, API вернул {found_user_id} (tg_id={tg_id}). Проверьте дубликаты.",
                        log_event="client_id_collision",
                    )
                    await notify_operators_anomaly(
                        int(created_user_id),
                        f"Коллизия карточек: индекс указывает на клиента {found_user_id}, только что создан {created_user_id} (tg_id={tg_id}).",
                        log_event="client_id_collision",
                    )
                user_id = found_user_id
                break

        if user_id == created_user_id:
            logger.info(
                f"User {created_user_id} not found in API after {max_retries} attempts. Using created user ID from cache."
            )

        existing_user = await check_api_element(
            tg_id, settings.CLIENT_FORM_ID, settings.USER_FORM_FIELDS["tg_id"]
        )
        
        if existing_user and existing_user.get("id"):
            await clear_pending_user_id(tg_id)

        logger.info(
            f"User {fullname} successfully registered with TG ID {tg_id} and Task ID {user_id}"
        )

        await message.answer(f"✅ Регистрация завершена успешно!\n\nСпасибо за регистрацию, {fullname}!")

        pending_task_id = await get_pending_task_id(tg_id)
        
        existing_task = await check_api_element(
            tg_id, settings.APPEAL_FORM_ID, settings.REQUEST_FORM_FIELDS["tg_id"]
        )

        task_id = existing_task.get("id") if existing_task else None

        if not task_id and pending_task_id:
            task_id = pending_task_id
            logger.info(
                f"Using pending task ID {task_id} from cache for user {tg_id} during registration"
            )

        card_fields = (
            prepare_fields_to_dict(existing_user["fields"])
            if existing_user and existing_user.get("fields")
            else {}
        )

        if not task_id:
            appeal_fields_reg: list[dict[str, Any]] = [
                {"id": settings.REQUEST_FORM_FIELDS["fio"], "value": fullname},
                {"id": settings.REQUEST_FORM_FIELDS["telephone"], "value": telephone},
                {"id": settings.REQUEST_FORM_FIELDS["tg_account"], "value": tg_account},
                {"id": settings.REQUEST_FORM_FIELDS["tg_id"], "value": tg_id},
            ]
            appeal_fields_reg.extend(
                extra_appeal_fields_from_client_card(
                    card_fields, source_channel="telegram"
                )
            )
            json_data = {
                "form_id": settings.APPEAL_FORM_ID,
                "fields": appeal_fields_reg,
            }

            result = await create_appeal_task(json_data)

            created_task_id = result.get("id")

            if result and created_task_id:
                logger.info(f"Created new appeal task ID {created_task_id} for user {tg_id}")
                await set_pending_task_id(tg_id, created_task_id)

                task_id = created_task_id

                max_retries = 5
                for attempt in range(max_retries):
                    await asyncio.sleep(0.2 * (attempt + 1))

                    existing_task = await check_api_element(
                        tg_id, settings.APPEAL_FORM_ID, settings.REQUEST_FORM_FIELDS["tg_id"]
                    )

                    found_task_id = existing_task.get("id") if existing_task else None

                    if found_task_id:
                        if found_task_id != created_task_id:
                            logger.warning(
                                f"Another task {found_task_id} was found for user {tg_id} after creating {created_task_id}. Using existing task."
                            )
                            await clear_pending_task_id(tg_id)
                            await notify_operators_anomaly(
                                int(found_task_id),
                                f"Коллизия обращений при регистрации: создана {created_task_id}, API вернул {found_task_id} (tg_id={tg_id}).",
                                log_event="appeal_id_collision",
                            )
                            await notify_operators_anomaly(
                                int(created_task_id),
                                f"Коллизия обращений при регистрации: индекс указывает на {found_task_id}, создана {created_task_id} (tg_id={tg_id}).",
                                log_event="appeal_id_collision",
                            )
                        task_id = found_task_id
                        break

                if task_id == created_task_id:
                    logger.info(
                        f"Task {created_task_id} not found in API after {max_retries} attempts. Using created task ID from cache."
                    )
                    
                existing_task = await check_api_element(
                    tg_id, settings.APPEAL_FORM_ID, settings.REQUEST_FORM_FIELDS["tg_id"]
                )
                
                if existing_task and existing_task.get("id"):
                    await clear_pending_task_id(tg_id)

            if not task_id:
                logger.error(f"Failed to create appeal task for user {tg_id}")
                await message.answer(
                    "❌ Ошибка при создании обращения. Пожалуйста, попробуйте позже."
                )
                await state.clear()
                return

        if task_id:
            await open_chats_after_appeal(
                task_id,
                source_channel="telegram",
                fields_dict=card_fields,
            )
            logger.info(f"The chat(s) for task #{task_id} have been successfully opened")
            await operator_warn_telegram_flow(tg_id, card_fields)

    await state.clear()

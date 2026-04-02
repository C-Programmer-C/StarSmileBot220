import asyncio
import io
import logging
from typing import Optional
from aiogram import Bot, types
from bot_client import BotClient
from config import settings
from resource_limits import acquire_user_file_slot, telegram_attachment_units
from utils import (
    build_payload,
    get_unique_file_id,
    post_crm_plain_comment,
    record_messaging_failure,
    send_comment_in_pyrus,
)

logger = logging.getLogger(__name__)

media_groups_data: dict[str, list[types.Message]] = {}
processing_tasks: set[str] = set()


def identify_file_data(
    message: types.Message,
) -> tuple[str | None, str | None, int | None]:
    """Identify file data from the message"""
    if message.photo:
        file = message.photo[-1]
        return file.file_id, f"{file.file_unique_id}.jpg", file.file_size
    elif message.document:
        return (
            message.document.file_id,
            message.document.file_name if message.document.file_name else "document",
            message.document.file_size,
        )
    elif message.audio:
        return (
            message.audio.file_id,
            f"audio_{message.audio.file_id}.mp3",
            message.audio.file_size,
        )
    elif message.voice:
        return (
            message.voice.file_id,
            f"voice_{message.voice.file_id}.ogg",
            message.voice.file_size,
        )
    elif message.video:
        return (
            message.video.file_id,
            f"video_{message.video.file_id}.mp4",
            message.video.file_size,
        )
    elif message.sticker:
        sticker = message.sticker

        if sticker.is_animated:
            ext = "tgs"
        elif sticker.is_video:
            ext = "webm"
        else:
            ext = "webp"
        return (
            sticker.file_id,
            f"sticker_{sticker.file_unique_id}.{ext}",
            sticker.file_size or 0,
        )
    return None, None, None


async def process_single_file_for_comment(
    message: types.Message,
) -> str | tuple[str, str | None] | None:
    """Processing files with the correct size check"""
    try:
        file_id, filename, file_size = identify_file_data(message)
        if not file_id:
            return "⚠️ Неподдерживаемый тип файла"

        if file_size and file_size > settings.MAX_FILE_SIZE:
            return "⚠️ Файл слишком большой! Максимум 20МБ"

        return file_id, filename

    except Exception as e:
        logger.error(f"File processing error: {e}")
        return "⚠️ Ошибка обработки файла"


async def process_single_comment(msg: types.Message, task_id: int):
    file = None

    if msg.text:
        comment_text = msg.text

    else:
        file_result = await process_single_file_for_comment(msg)

        if isinstance(file_result, str):
            logger.warning(f"File error: {file_result} in message {msg.message_id}")
            await msg.reply("Произошла ошибка при обработке файла. Попробуйте еще раз.")
            return

        if not file_result or len(file_result) != 2:
            logger.warning(f"Invalid file_result: {file_result}")
            await msg.reply("Произошла ошибка при обработке файла. Попробуйте еще раз.")
            return

        file_id, file_name = file_result

        bot = BotClient.get_instance()
        file = await process_file(file_id, file_name, bot)

        if not file:
            await msg.reply("Произошла ошибка при обработке файла. Попробуйте еще раз.")
            return

        comment_text = msg.caption

    json_data = build_payload(comment_text, [file] if file else [])

    try:
        await send_comment_in_pyrus(task_id, json_data)
    except Exception as e:
        logger.error(f"Failed to process comment: {e}")
        record_messaging_failure(
            direction="user_to_crm",
            task_id=task_id,
            channel="telegram",
            message=str(e),
        )
        await post_crm_plain_comment(
            task_id,
            f"Не удалось принять сообщение пользователя в CRM (Telegram): {e}",
        )
        await msg.reply("Произошла ошибка при отправке комментария.")
        return


async def process_file(file_id: str, file_name: str, bot: Bot) -> Optional[str]:
    """Process a file"""
    try:
        file = await bot.get_file(file_id)
        file_bytes = io.BytesIO()

        file_path = file.file_path
        if not file_path:
            logger.error(f"File path is None for file_id: {file_id}")
            return None

        await bot.download_file(file_path, destination=file_bytes)
        file_bytes.seek(0)

        unique_file_id: str | None = await get_unique_file_id(file_bytes, file_name)
        return unique_file_id
    except Exception as e:
        logger.error(
            f"Failed to process file #file_id: {file_id} #file_name: {file_name}: {e}"
        )
        return None


async def process_media_group(message: types.Message, tg_id: int, task_id: int):

    if not message.media_group_id:
        return

    media_group_id = message.media_group_id

    media_groups_data.setdefault(media_group_id, []).append(message)

    if media_group_id in processing_tasks:
        return

    logger.info(f"Scheduling processing for media group {media_group_id}")

    processing_tasks.add(media_group_id)

    async def process_group():
        await asyncio.sleep(3)
        group_messages = media_groups_data.pop(media_group_id, [])
        processing_tasks.remove(media_group_id)
        if not group_messages:
            return
        logger.info(
            f"Processing media group {media_group_id} with {len(group_messages)} messages"
        )

        for msg in group_messages:
            for _ in range(telegram_attachment_units(msg)):
                await acquire_user_file_slot("telegram", tg_id)

        comment_text = group_messages[0].caption or None
        unique_file_ids: list[str] = []

        for msg in group_messages:

            file_result = await process_single_file_for_comment(msg)

            if isinstance(file_result, str):
                logger.warning(
                    f"Media group #{media_group_id} has errors: {file_result} in message {msg.message_id}"
                )
                msg.reply(
                    f"Произошла ошибка при обработке неизвестного файла: {file_result}. Попробуйте еще раз."
                )
                continue

            if file_result and len(file_result) == 2:
                file_id, file_name = file_result
            else:
                logger.warning(
                    f"An error occurred while processing the file {file_result}"
                )
                msg.reply(
                    f"Произошла ошибка при обработке неизвестного файла: {file_result}. Попробуйте еще раз."
                )
                continue

            bot = BotClient.get_instance()

            if not file_id or not file_name:
                logger.warning(
                    "the file_id or filename is missing from the file {file_result}"
                )
                continue

            file = await process_file(file_id, file_name, bot)
            if file:
                unique_file_ids.append(file)
            else:
                msg.reply(
                    f"Произошла ошибка при обработке файла #{file_name}. Попробуйте еще раз."
                )
                continue

        json_data = build_payload(comment_text, unique_file_ids)

        try:
            await send_comment_in_pyrus(task_id, json_data)
        except Exception as e:
            logger.error("Failed to send media group to Pyrus: %s", e)
            record_messaging_failure(
                direction="user_to_crm",
                task_id=task_id,
                channel="telegram",
                message=str(e),
            )
            await post_crm_plain_comment(
                task_id,
                f"Не удалось принять сообщение пользователя в CRM (Telegram, медиагруппа): {e}",
            )
            if group_messages:
                await group_messages[0].reply(
                    "Произошла ошибка при отправке комментария."
                )

    asyncio.create_task(process_group())

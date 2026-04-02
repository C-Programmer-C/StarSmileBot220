from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.base import BaseStorage
from config import settings
from typing import Optional


class BotClient:
    _instance: Optional[Bot] = None
    storage: Optional[BaseStorage] = None

    @classmethod
    def get_instance(cls) -> Bot:
        if cls._instance is None:
            cls._instance = Bot(
                token=settings.BOT_TOKEN,
                default=DefaultBotProperties(parse_mode="HTML"),
            )
        return cls._instance

    @classmethod
    def set_storage(cls, storage: BaseStorage) -> None:
        """
        Сохраняем экземпляр storage, чтобы можно было потом достать его из любого места.
        """
        cls.storage = storage

    @classmethod
    async def close(cls) -> None:
        if cls._instance:
            await cls._instance.session.close()
            cls._instance = None

from typing import Optional

from maxapi import Bot

from config import settings


class MaxBotClient:
    _instance: Optional[Bot] = None

    @classmethod
    def get_instance(cls) -> Bot:
        if cls._instance is None:
            token = settings.MAX_BOT_TOKEN
            cls._instance = Bot(token=token)
        return cls._instance

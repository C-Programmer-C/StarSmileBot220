import asyncio
import logging
from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from bot.telegram_handlers import start_router
from bot_client import BotClient
from config import settings


logger = logging.getLogger(__name__)

async def main():
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    logger.info("Starting bot (LOG_LEVEL=%s)", settings.LOG_LEVEL)

    bot = BotClient.get_instance()
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(start_router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot) # type: ignore

if __name__ == "__main__":
    asyncio.run(main())

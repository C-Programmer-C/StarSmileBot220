import asyncio
import logging

from maxapi import Dispatcher

from bot.max_handlers import register_max_handlers
from config import settings
from max_bot_client import MaxBotClient

logger = logging.getLogger(__name__)


async def main():
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    logger.info("Starting MAX bot (LOG_LEVEL=%s)", settings.LOG_LEVEL)

    bot = MaxBotClient.get_instance()
    await bot.delete_webhook()
    dp = Dispatcher()
    register_max_handlers(dp, bot)
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())

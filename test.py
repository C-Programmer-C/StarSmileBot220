import asyncio
import logging

from maxapi import Bot, Dispatcher
from maxapi.types import BotStarted, Command, MessageCreated

from bot_client import settings

logging.basicConfig(level=logging.INFO)

# Внесите токен бота в переменную окружения MAX_BOT_TOKEN
# Не забудьте загрузить переменные из .env в os.environ
# или задайте его аргументом в Bot(token='...')
bot = Bot(
    token="f9LHodD0cOJhxySjvzoXqpOwUvfIPuT4wCnlSjbC3F_5a_p9Nait9cSWFVxMC22V9RPO0FSLTzkRvWAG-nFQ"
)
dp = Dispatcher()


# Ответ бота при нажатии на кнопку "Начать"
@dp.bot_started()
async def bot_started(event: BotStarted):
    await event.bot.send_message(
        chat_id=event.chat_id, text="Привет! Отправь мне /start"
    )


# Ответ бота на команду /start
@dp.message_created(Command("start"))
async def hello(event: MessageCreated):
    await event.message.answer("Пример чат-бота для MAX 💙")


async def sending_message(user, message):
    await bot.send_message(user_id=user, text=message)


bot = Bot(token=settings.MAX_BOT_TOKEN)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(sending_message(user="113124161", message="test"))

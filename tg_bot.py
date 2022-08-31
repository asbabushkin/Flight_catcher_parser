"""
Рализация через aiogram & asyncio
В проекте не использую.
"""
import asyncio

from aiogram import Bot, Dispatcher, executor, types
from fl_catcher_data import token, telegram_user_id, url_ottrip
from main import get_one_two_trip

bot = Bot(token=token, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)



async def check_price_every_minute():
    while True:
        price = get_one_two_trip(url_ottrip)
        await bot.send_message(telegram_user_id, price)
        await asyncio.sleep(30)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(check_price_every_minute())
    executor.start_polling(dp)

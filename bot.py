import asyncio
import json
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import settings
from database import Database

logging.basicConfig(level=logging.INFO)


db = Database()
bot = Bot(token=settings.bot_token)
dp = Dispatcher()


def build_keyboard(buttons_json: str | None) -> InlineKeyboardMarkup | None:
    if not buttons_json:
        return None
    try:
        buttons = json.loads(buttons_json)
    except json.JSONDecodeError:
        return None

    keyboard: list[list[InlineKeyboardButton]] = []
    for row in buttons:
        line: list[InlineKeyboardButton] = []
        for item in row:
            text = item.get("text")
            url = item.get("url")
            callback_data = item.get("callback_data")
            if not text:
                continue
            if url:
                line.append(InlineKeyboardButton(text=text, url=url))
            elif callback_data:
                line.append(InlineKeyboardButton(text=text, callback_data=callback_data))
        if line:
            keyboard.append(line)

    return InlineKeyboardMarkup(inline_keyboard=keyboard) if keyboard else None


@dp.message(CommandStart())
async def start_command(message: Message) -> None:
    if db.is_blacklisted(message.from_user.id):
        return
    db.add_user(message.from_user.id, message.from_user.username)
    await message.answer("Добро пожаловать в Кадровый Навигатор! Напишите команду или триггер.")


@dp.message(F.text)
async def process_text_message(message: Message) -> None:
    user_id = message.from_user.id
    if db.is_blacklisted(user_id):
        return

    db.add_user(user_id, message.from_user.username)
    scenario = db.get_scenario_by_trigger(message.text)
    if scenario:
        markup = build_keyboard(scenario.get("buttons_json"))
        await message.answer(scenario["bot_reply_text"], reply_markup=markup)
        return

    if message.text.strip() == "/start":
        await start_command(message)
        return

    await message.answer("Сценарий не найден. Обратитесь к администратору.")


async def main() -> None:
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN не задан в .env")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

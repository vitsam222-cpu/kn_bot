import asyncio
import json
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

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
            step_id = item.get("step_id")
            step_trigger = item.get("step_trigger")
            if not text:
                continue
            if url:
                line.append(InlineKeyboardButton(text=text, url=url))
            elif step_id:
                line.append(InlineKeyboardButton(text=text, callback_data=f"step:{step_id}"))
            elif step_trigger:
                line.append(InlineKeyboardButton(text=text, callback_data=f"stepref:{step_trigger}"))
            elif callback_data:
                line.append(InlineKeyboardButton(text=text, callback_data=callback_data))
        if line:
            keyboard.append(line)

    return InlineKeyboardMarkup(inline_keyboard=keyboard) if keyboard else None


async def send_scenario_message(message: Message, scenario: dict, user_id: int | None = None) -> None:
    resolved_user_id = user_id or (message.from_user.id if message.from_user else message.chat.id)
    scenario_id = int(scenario["id"])
    db.increment_scenario_visit(scenario_id, user_id=resolved_user_id)
    db.add_user_tag(resolved_user_id, f"step_{scenario_id}")
    markup = build_keyboard(scenario.get("buttons_json"))
    image_path = scenario.get("scenario_image_path")
    if image_path and Path(image_path).exists():
        try:
            await message.answer_photo(
                photo=FSInputFile(image_path),
                caption=scenario["bot_reply_text"],
                reply_markup=markup,
                parse_mode="Markdown",
            )
            return
        except Exception:
            # fallback to text if image sending fails
            pass
    await message.answer(scenario["bot_reply_text"], reply_markup=markup, parse_mode="Markdown")


@dp.message(CommandStart())
async def start_command(message: Message) -> None:
    if db.is_blacklisted(message.from_user.id):
        return
    db.add_user(message.from_user.id, message.from_user.username)
    db.add_user_event(message.from_user.id, "command_start", "/start")
    start_scenario = db.get_scenario_by_trigger("/start")
    if start_scenario:
        await send_scenario_message(message, start_scenario, user_id=message.from_user.id)
        return
    await message.answer("Добро пожаловать в Кадровый Навигатор! Напишите команду или триггер.")


@dp.message(F.text)
async def process_text_message(message: Message) -> None:
    user_id = message.from_user.id
    if db.is_blacklisted(user_id):
        return

    db.add_user(user_id, message.from_user.username)
    db.add_user_event(user_id, "incoming_text", message.text)
    scenario = db.get_scenario_by_trigger(message.text)
    if scenario:
        await send_scenario_message(message, scenario, user_id=user_id)
        return

    if message.text.strip() == "/start":
        await start_command(message)
        return

    await message.answer("Сценарий не найден. Обратитесь к администратору.")


@dp.callback_query(F.data.startswith("step:"))
async def process_step_callback(callback: CallbackQuery) -> None:
    if db.is_blacklisted(callback.from_user.id):
        await callback.answer()
        return

    try:
        step_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный шаг", show_alert=True)
        return

    scenario = db.get_scenario_by_id(step_id)
    if not scenario:
        await callback.answer("Шаг не найден", show_alert=True)
        return

    db.add_user_event(callback.from_user.id, "callback_step", callback.data)
    await send_scenario_message(callback.message, scenario, user_id=callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data.startswith("stepref:"))
async def process_stepref_callback(callback: CallbackQuery) -> None:
    if db.is_blacklisted(callback.from_user.id):
        await callback.answer()
        return

    try:
        step_ref = callback.data.split(":", 1)[1].strip()
    except (ValueError, IndexError, AttributeError):
        await callback.answer("Некорректный шаг", show_alert=True)
        return

    scenario = db.get_scenario_by_trigger(step_ref)
    if not scenario:
        await callback.answer("Шаг не найден", show_alert=True)
        return

    db.add_user_event(callback.from_user.id, "callback_stepref", callback.data)
    await send_scenario_message(callback.message, scenario, user_id=callback.from_user.id)
    await callback.answer()


async def main() -> None:
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN не задан в .env")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

import os
from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes


WELCOME_TEXT = (
    "Привет! 👋 Добро пожаловать в бота для кадровиков, HR и бухгалтеров.\n\n"
    "Здесь всегда под рукой шаблоны документов и полезные курсы.\n\n"
    "Выберите, что нужно прямо сейчас:"
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [["📗 Шаблоны документов"], ["📅 Курсы", "🔷 Консультация"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    if update.message:
        await update.message.reply_text(WELCOME_TEXT, reply_markup=reply_markup)


def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")

    if not token:
        raise RuntimeError("BOT_TOKEN не найден. Создайте .env на основе .env.example")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.run_polling()


if __name__ == "__main__":
    main()

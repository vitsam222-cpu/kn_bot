"""Handlers for the start command and main menu."""

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database import Database

router = Router(name="start")


def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Create main menu inline keyboard.
    
    Returns:
        InlineKeyboardMarkup with main menu buttons.
    """
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Документы", callback_data="documents")]
        ]
    )
    return keyboard


@router.message(Command("start"))
async def cmd_start(message: Message, db: Database) -> None:
    """Handle /start command.
    
    Args:
        message: Incoming message object.
        db: Database instance.
    """
    user_id = message.from_user.id
    username = message.from_user.username

    try:
        # Add/update user in database
        await db.add_user(user_id, username)

        # Send welcome message with main menu
        welcome_text = (
            f"👋 Привет, {message.from_user.first_name}!\n\n"
            "Я бот для выдачи документов и напоминаний.\n"
            "Выберите действие из меню ниже:"
        )

        await message.answer(
            text=welcome_text,
            reply_markup=get_main_menu_keyboard()
        )
    except Exception as e:
        # Log error but don't crash
        print(f"Error in cmd_start: {e}")
        await message.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")


@router.callback_query(F.data == "main_menu")
async def show_main_menu(callback: CallbackQuery, db: Database) -> None:
    """Show main menu.
    
    Args:
        callback: Callback query object.
        db: Database instance.
    """
    try:
        text = (
            "📋 Главное меню\n\n"
            "Выберите действие:"
        )
        await callback.message.edit_text(
            text=text,
            reply_markup=get_main_menu_keyboard()
        )
        await callback.answer()
    except Exception as e:
        print(f"Error in show_main_menu: {e}")
        await callback.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")

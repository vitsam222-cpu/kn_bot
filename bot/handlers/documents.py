"""Handlers for documents and reminders."""

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database import Database

router = Router(name="documents")


def get_documents_keyboard() -> InlineKeyboardMarkup:
    """Create documents list inline keyboard.
    
    Returns:
        InlineKeyboardMarkup with document links.
    """
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="📄 Документ 1",
                url="https://example.com/doc1"
            )],
            [InlineKeyboardButton(
                text="📄 Документ 2",
                url="https://example.com/doc2"
            )],
            [InlineKeyboardButton(
                text="📄 Документ 3",
                url="https://example.com/doc3"
            )],
            [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="main_menu")]
        ]
    )
    return keyboard


@router.callback_query(F.data == "documents")
async def handle_documents(callback: CallbackQuery, db: Database) -> None:
    """Handle documents button click.
    
    Sends document links and creates a reminder for the user.
    
    Args:
        callback: Callback query object.
        db: Database instance.
    """
    user_id = callback.from_user.id

    try:
        # Add reminder to database
        await db.add_reminder(
            user_id=user_id,
            trigger_type="documents",
            delay_days=3
        )

        # Send documents list
        documents_text = (
            "📚 **Документы**\n\n"
            "Выберите нужный документ из списка ниже:\n\n"
            "📄 Документ 1 - Описание первого документа\n"
            "📄 Документ 2 - Описание второго документа\n"
            "📄 Документ 3 - Описание третьего документа"
        )

        await callback.message.answer(
            text=documents_text,
            reply_markup=get_documents_keyboard()
        )

        # Send confirmation about reminder
        await callback.message.answer(
            "✅ Ссылки отправлены. Напоминание о мероприятии придёт через 3 дня."
        )

        await callback.answer()

    except Exception as e:
        print(f"Error in handle_documents: {e}")
        await callback.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")

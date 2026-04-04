"""FSM states and handlers for multi-step scenarios."""

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import Database

router = Router(name="fsm_steps")


class ExampleSteps(StatesGroup):
    """Example FSM state group for multi-step scenarios."""
    step1 = State()
    step2 = State()
    step3 = State()


@router.callback_query(F.data == "start_fsm_example")
async def start_fsm_example(callback: CallbackQuery, state: FSMContext) -> None:
    """Start example FSM scenario.
    
    Args:
        callback: Callback query object.
        state: FSM context.
    """
    try:
        await state.set_state(ExampleSteps.step1)
        await callback.message.answer(
            "🔹 **Шаг 1 из 3**\n\n"
            "Это пример пошагового сценария (FSM).\n"
            "Введите любой текст для продолжения:"
        )
        await callback.answer()
    except Exception as e:
        print(f"Error in start_fsm_example: {e}")
        await callback.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")


@router.message(ExampleSteps.step1)
async def process_step1(message: Message, state: FSMContext) -> None:
    """Process first step of FSM.
    
    Args:
        message: Incoming message.
        state: FSM context.
    """
    try:
        # Store user input if needed
        await state.update_data(step1_response=message.text)

        await state.set_state(ExampleSteps.step2)
        await message.answer(
            "🔹 **Шаг 2 из 3**\n\n"
            f"Вы ввели: {message.text}\n"
            "Введите текст для следующего шага:"
        )
    except Exception as e:
        print(f"Error in process_step1: {e}")
        await message.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        await state.clear()


@router.message(ExampleSteps.step2)
async def process_step2(message: Message, state: FSMContext) -> None:
    """Process second step of FSM.
    
    Args:
        message: Incoming message.
        state: FSM context.
    """
    try:
        await state.update_data(step2_response=message.text)

        await state.set_state(ExampleSteps.step3)
        await message.answer(
            "🔹 **Шаг 3 из 3**\n\n"
            "Финальный шаг. Введите текст для завершения:"
        )
    except Exception as e:
        print(f"Error in process_step2: {e}")
        await message.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        await state.clear()


@router.message(ExampleSteps.step3)
async def process_step3(message: Message, state: FSMContext, db: Database) -> None:
    """Process final step of FSM.
    
    Args:
        message: Incoming message.
        state: FSM context.
        db: Database instance.
    """
    try:
        data = await state.get_data()
        
        await message.answer(
            "✅ **Сценарий завершён!**\n\n"
            f"Шаг 1: {data.get('step1_response', 'Нет данных')}\n"
            f"Шаг 2: {data.get('step2_response', 'Нет данных')}\n"
            f"Шаг 3: {message.text}\n\n"
            "Спасибо за прохождение!"
        )

        # Clear FSM state
        await state.clear()

    except Exception as e:
        print(f"Error in process_step3: {e}")
        await message.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")
        await state.clear()


@router.message(F.text == "/cancel")
async def cancel_fsm(message: Message, state: FSMContext) -> None:
    """Cancel current FSM scenario.
    
    Args:
        message: Incoming message.
        state: FSM context.
    """
    try:
        current_state = await state.get_state()
        if current_state:
            await state.clear()
            await message.answer("❌ Сценарий отменён.")
        else:
            await message.answer("Нет активного сценария для отмены.")
    except Exception as e:
        print(f"Error in cancel_fsm: {e}")
        await message.answer("Произошла ошибка. Пожалуйста, попробуйте позже.")

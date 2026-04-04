"""Main entry point for the Telegram bot."""

import asyncio
import signal
import logging
from typing import List

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import BOT_TOKEN, LOG_LEVEL
from database import Database
from scheduler import ReminderScheduler
from handlers import start, documents, fsm_steps

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Main function to run the bot."""
    # Initialize bot and dispatcher
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # Initialize database
    db = Database()
    await db.connect()
    logger.info("Database connection established")

    # Include routers
    dp.include_routers(
        start.router,
        documents.router,
        fsm_steps.router
    )

    # Pass database instance to all handlers
    dp.workflow_data["db"] = db

    # Initialize and start scheduler
    scheduler = ReminderScheduler(bot=bot, db=db, check_interval=3600)
    await scheduler.start()
    logger.info("Reminder scheduler started (check interval: 1 hour)")

    # Setup graceful shutdown
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def handle_signal(sig: signal.Signals) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {sig.name}, shutting down...")
        stop_event.set()

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    try:
        # Start polling
        logger.info("Starting bot polling...")
        
        # Run polling in a task
        polling_task = asyncio.create_task(dp.start_polling(bot))

        # Wait for stop signal
        await stop_event.wait()

        # Cancel polling
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass

    finally:
        # Cleanup
        logger.info("Cleaning up resources...")
        await scheduler.stop()
        await db.close()
        await bot.session.close()
        logger.info("Bot stopped gracefully")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrupted by user")

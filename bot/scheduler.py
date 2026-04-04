"""Scheduler module for background reminder tasks."""

import asyncio
import signal
from datetime import datetime
from typing import Optional

from aiogram import Bot

from database import Database


class ReminderScheduler:
    """Background scheduler for sending reminders."""

    def __init__(
        self,
        bot: Bot,
        db: Database,
        check_interval: int = 3600  # 1 hour in seconds
    ) -> None:
        """Initialize scheduler.
        
        Args:
            bot: Aiogram Bot instance.
            db: Database instance.
            check_interval: Interval between checks in seconds (default: 1 hour).
        """
        self.bot: Bot = bot
        self.db: Database = db
        self.check_interval: int = check_interval
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the scheduler background task."""
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the scheduler background task."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._check_and_send_reminders()
            except Exception as e:
                print(f"Error in scheduler loop: {e}")
            
            # Wait for next check interval
            await asyncio.sleep(self.check_interval)

    async def _check_and_send_reminders(self) -> None:
        """Check for pending reminders and send them."""
        try:
            pending_reminders = await self.db.get_pending_reminders()

            for reminder_id, user_id, trigger_type in pending_reminders:
                try:
                    # Send reminder message
                    await self.bot.send_message(
                        chat_id=user_id,
                        text="🔔 Напоминаем о мероприятии!"
                    )

                    # Mark reminder as sent
                    await self.db.mark_reminder_sent(reminder_id)

                    print(f"Reminder sent to user {user_id} (trigger: {trigger_type})")

                except Exception as e:
                    print(f"Error sending reminder to user {user_id}: {e}")
                    # Don't mark as sent if delivery failed, will retry next cycle

        except Exception as e:
            print(f"Error checking reminders: {e}")


async def create_scheduler(
    bot: Bot,
    db: Database,
    check_interval: int = 3600
) -> ReminderScheduler:
    """Create and return a scheduler instance.
    
    Args:
        bot: Aiogram Bot instance.
        db: Database instance.
        check_interval: Interval between checks in seconds.
        
    Returns:
        Configured ReminderScheduler instance.
    """
    return ReminderScheduler(bot=bot, db=db, check_interval=check_interval)

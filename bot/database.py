"""Database module for SQLite operations with async support."""

import sqlite3
import asyncio
from datetime import datetime
from typing import Optional, List, Tuple
from pathlib import Path

import aiosqlite

from config import DATABASE_PATH


class Database:
    """Async SQLite database manager for the Telegram bot."""

    def __init__(self, db_path: str = DATABASE_PATH) -> None:
        """Initialize database connection.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path: str = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Establish database connection and create tables if needed."""
        self._connection = await aiosqlite.connect(self.db_path)
        await self._create_tables()

    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()

    async def _create_tables(self) -> None:
        """Create necessary database tables if they don't exist."""
        if not self._connection:
            raise RuntimeError("Database connection not established")

        # Create users table
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create reminders table
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                trigger_type TEXT NOT NULL,
                triggered_at TIMESTAMP NOT NULL,
                delay_days INTEGER NOT NULL,
                sent BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        await self._connection.commit()

    async def add_user(self, user_id: int, username: Optional[str] = None) -> None:
        """Add or update user in the database.
        
        Args:
            user_id: Telegram user ID.
            username: Telegram username (optional).
        """
        if not self._connection:
            raise RuntimeError("Database connection not established")

        await self._connection.execute(
            """
            INSERT OR REPLACE INTO users (user_id, username, created_at)
            VALUES (?, ?, ?)
            """,
            (user_id, username, datetime.now())
        )
        await self._connection.commit()

    async def get_user(self, user_id: int) -> Optional[Tuple[int, Optional[str], datetime]]:
        """Get user by ID.
        
        Args:
            user_id: Telegram user ID.
            
        Returns:
            User tuple (user_id, username, created_at) or None if not found.
        """
        if not self._connection:
            raise RuntimeError("Database connection not established")

        cursor = await self._connection.execute(
            "SELECT user_id, username, created_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row  # type: ignore

    async def add_reminder(
        self,
        user_id: int,
        trigger_type: str,
        delay_days: int = 3
    ) -> None:
        """Add a new reminder for a user.
        
        Args:
            user_id: Telegram user ID.
            trigger_type: Type of trigger (e.g., "documents").
            delay_days: Number of days before reminder is sent.
        """
        if not self._connection:
            raise RuntimeError("Database connection not established")

        await self._connection.execute(
            """
            INSERT INTO reminders (user_id, trigger_type, triggered_at, delay_days, sent)
            VALUES (?, ?, ?, ?, 0)
            """,
            (user_id, trigger_type, datetime.now(), delay_days)
        )
        await self._connection.commit()

    async def get_pending_reminders(self) -> List[Tuple[int, int, str]]:
        """Get all pending reminders that are due.
        
        Returns:
            List of tuples (id, user_id, trigger_type) for reminders to send.
        """
        if not self._connection:
            raise RuntimeError("Database connection not established")

        cursor = await self._connection.execute("""
            SELECT id, user_id, trigger_type
            FROM reminders
            WHERE sent = 0
            AND datetime(triggered_at, '+' || delay_days || ' days') <= datetime('now')
        """)
        rows = await cursor.fetchall()
        return rows  # type: ignore

    async def mark_reminder_sent(self, reminder_id: int) -> None:
        """Mark a reminder as sent.
        
        Args:
            reminder_id: Reminder ID to update.
        """
        if not self._connection:
            raise RuntimeError("Database connection not established")

        await self._connection.execute(
            "UPDATE reminders SET sent = 1 WHERE id = ?",
            (reminder_id,)
        )
        await self._connection.commit()

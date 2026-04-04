import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from config import settings


class Database:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or settings.database_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout = 5000;")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id INTEGER PRIMARY KEY
                );

                CREATE TABLE IF NOT EXISTS scenarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_text TEXT UNIQUE NOT NULL,
                    bot_reply_text TEXT NOT NULL,
                    buttons_json TEXT,
                    next_step INTEGER,
                    scenario_image_path TEXT
                );

                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    remind_at TEXT,
                    message TEXT
                );
                """
            )
            scenario_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(scenarios)").fetchall()
            }
            if "scenario_image_path" not in scenario_columns:
                conn.execute("ALTER TABLE scenarios ADD COLUMN scenario_image_path TEXT")

    def add_user(self, user_id: int, username: str | None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users(user_id, username)
                VALUES(?, ?)
                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
                """,
                (user_id, username),
            )

    def is_blacklisted(self, user_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (user_id,)).fetchone()
        return row is not None

    def set_blacklist(self, user_id: int, banned: bool) -> None:
        with self.connect() as conn:
            if banned:
                conn.execute("INSERT OR IGNORE INTO blacklist(user_id) VALUES(?)", (user_id,))
            else:
                conn.execute("DELETE FROM blacklist WHERE user_id = ?", (user_id,))

    def get_scenario_by_trigger(self, trigger_text: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM scenarios WHERE lower(trigger_text)=lower(?)", (trigger_text.strip(),)
            ).fetchone()
        return dict(row) if row else None

    def get_all_scenarios(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM scenarios ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def get_scenario_by_id(self, scenario_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM scenarios WHERE id = ?", (scenario_id,)).fetchone()
        return dict(row) if row else None

    def upsert_scenario(
        self,
        trigger_text: str,
        bot_reply_text: str,
        buttons_json: str | None,
        next_step: int | None,
        scenario_image_path: str | None = None,
        scenario_id: int | None = None,
    ) -> None:
        if buttons_json:
            json.loads(buttons_json)

        with self.connect() as conn:
            if scenario_id:
                conn.execute(
                    """
                    UPDATE scenarios
                    SET trigger_text=?, bot_reply_text=?, buttons_json=?, next_step=?, scenario_image_path=?
                    WHERE id=?
                    """,
                    (trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path, scenario_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO scenarios(trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path)
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path),
                )

    def delete_scenario(self, scenario_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM scenarios WHERE id=?", (scenario_id,))

    def get_users_with_status(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT u.user_id, u.username, u.created_at,
                       CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END AS is_banned
                FROM users u
                LEFT JOIN blacklist b ON u.user_id = b.user_id
                ORDER BY u.created_at DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> dict[str, int]:
        with self.connect() as conn:
            users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            blocked_count = conn.execute("SELECT COUNT(*) FROM blacklist").fetchone()[0]
        return {"users_count": users_count, "blocked_count": blocked_count}

    def get_active_user_ids(self) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id FROM users
                WHERE user_id NOT IN (SELECT user_id FROM blacklist)
                """
            ).fetchall()
        return [int(r[0]) for r in rows]

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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP
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

                CREATE TABLE IF NOT EXISTS broadcast_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_text TEXT NOT NULL,
                    buttons_json TEXT,
                    photo_path TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    timezone TEXT,
                    scheduled_at TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sent_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS scenario_metrics (
                    scenario_id INTEGER PRIMARY KEY,
                    visits_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS user_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            scenario_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(scenarios)").fetchall()
            }
            if "scenario_image_path" not in scenario_columns:
                conn.execute("ALTER TABLE scenarios ADD COLUMN scenario_image_path TEXT")
            user_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()
            }
            if "last_seen_at" not in user_columns:
                conn.execute("ALTER TABLE users ADD COLUMN last_seen_at TIMESTAMP")
            start_exists = conn.execute(
                "SELECT 1 FROM scenarios WHERE lower(trigger_text)=lower('/start')"
            ).fetchone()
            if not start_exists:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO scenarios(trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path)
                    VALUES('/start', 'Добро пожаловать! Выберите раздел ниже.', NULL, NULL, NULL)
                    """
                )

    def add_user(self, user_id: int, username: str | None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users(user_id, username, last_seen_at)
                VALUES(?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    last_seen_at=CURRENT_TIMESTAMP
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

    def resolve_scenario_ref(self, ref_value: str | None) -> int | None:
        if ref_value is None:
            return None
        normalized = ref_value.strip()
        if not normalized:
            return None
        if normalized.isdigit():
            return int(normalized)
        scenario = self.get_scenario_by_trigger(normalized)
        if scenario:
            return int(scenario["id"])
        return None

    def upsert_scenario(
        self,
        trigger_text: str,
        bot_reply_text: str,
        buttons_json: str | None,
        next_step: int | None,
        scenario_image_path: str | None = None,
        scenario_id: int | None = None,
    ) -> None:
        normalized_trigger = trigger_text.strip()
        if buttons_json:
            json.loads(buttons_json)

        with self.connect() as conn:
            if scenario_id:
                cur = conn.execute(
                    """
                    UPDATE scenarios
                    SET trigger_text=?, bot_reply_text=?, buttons_json=?, next_step=?, scenario_image_path=?
                    WHERE id=?
                    """,
                    (normalized_trigger, bot_reply_text, buttons_json, next_step, scenario_image_path, scenario_id),
                )
                if cur.rowcount == 0:
                    conn.execute(
                        """
                        INSERT INTO scenarios(id, trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path)
                        VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (scenario_id, normalized_trigger, bot_reply_text, buttons_json, next_step, scenario_image_path),
                    )
                return
            else:
                existing_start = conn.execute(
                    "SELECT id FROM scenarios WHERE lower(trigger_text)=lower('/start')"
                ).fetchone()
                if normalized_trigger.lower() == "/start" and existing_start:
                    conn.execute(
                        """
                        UPDATE scenarios
                        SET bot_reply_text=?, buttons_json=?, next_step=?, scenario_image_path=?
                        WHERE id=?
                        """,
                        (bot_reply_text, buttons_json, next_step, scenario_image_path, int(existing_start["id"])),
                    )
                    return

                total = conn.execute("SELECT COUNT(*) FROM scenarios").fetchone()[0]
                has_id_1 = conn.execute("SELECT 1 FROM scenarios WHERE id=1").fetchone() is not None
                if total == 0 and normalized_trigger.lower() == "/start" and not has_id_1:
                    conn.execute(
                        """
                        INSERT INTO scenarios(id, trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path)
                        VALUES(1, ?, ?, ?, ?, ?)
                        """,
                        (normalized_trigger, bot_reply_text, buttons_json, next_step, scenario_image_path),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO scenarios(trigger_text, bot_reply_text, buttons_json, next_step, scenario_image_path)
                        VALUES(?, ?, ?, ?, ?)
                        """,
                        (normalized_trigger, bot_reply_text, buttons_json, next_step, scenario_image_path),
                    )

    def delete_scenario(self, scenario_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM scenarios WHERE id=?", (scenario_id,))

    def get_users_with_status(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT u.user_id, u.username, u.created_at, u.last_seen_at,
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
            dau = conn.execute(
                "SELECT COUNT(*) FROM users WHERE datetime(last_seen_at) >= datetime('now', '-1 day')"
            ).fetchone()[0]
            wau = conn.execute(
                "SELECT COUNT(*) FROM users WHERE datetime(last_seen_at) >= datetime('now', '-7 day')"
            ).fetchone()[0]
        return {"users_count": users_count, "blocked_count": blocked_count, "dau": dau, "wau": wau}

    def get_active_user_ids(self) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id FROM users
                WHERE user_id NOT IN (SELECT user_id FROM blacklist)
                """
            ).fetchall()
        return [int(r[0]) for r in rows]

    def log_broadcast(
        self,
        message_text: str,
        buttons_json: str | None,
        photo_path: str | None,
        timezone: str | None,
        scheduled_at: str | None,
        status: str = "pending",
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO broadcast_history(message_text, buttons_json, photo_path, timezone, scheduled_at, status)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (message_text, buttons_json, photo_path, timezone, scheduled_at, status),
            )
            return int(cur.lastrowid)

    def update_broadcast_status(
        self, broadcast_id: int, status: str, sent_count: int = 0, failed_count: int = 0, error_text: str | None = None
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE broadcast_history
                SET status=?, sent_count=?, failed_count=?, error_text=?
                WHERE id=?
                """,
                (status, sent_count, failed_count, error_text, broadcast_id),
            )

    def get_broadcast_history(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM broadcast_history ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_pending_broadcasts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM broadcast_history
                WHERE status='pending' AND scheduled_at IS NOT NULL
                  AND datetime(scheduled_at) <= datetime('now')
                ORDER BY scheduled_at
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def increment_scenario_visit(self, scenario_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO scenario_metrics(scenario_id, visits_count)
                VALUES(?, 1)
                ON CONFLICT(scenario_id) DO UPDATE SET visits_count = visits_count + 1
                """,
                (scenario_id,),
            )

    def get_scenario_metrics(self) -> dict[int, int]:
        with self.connect() as conn:
            rows = conn.execute("SELECT scenario_id, visits_count FROM scenario_metrics").fetchall()
        return {int(r["scenario_id"]): int(r["visits_count"]) for r in rows}

    def add_user_event(self, user_id: int, event_type: str, payload: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO user_events(user_id, event_type, payload) VALUES(?, ?, ?)",
                (user_id, event_type, payload),
            )

    def get_user_events(self, user_id: int, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM user_events WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def import_blacklist(self, user_ids: list[int]) -> None:
        with self.connect() as conn:
            conn.executemany("INSERT OR IGNORE INTO blacklist(user_id) VALUES(?)", [(uid,) for uid in user_ids])

    def import_whitelist(self, user_ids: list[int]) -> None:
        with self.connect() as conn:
            conn.executemany("DELETE FROM blacklist WHERE user_id = ?", [(uid,) for uid in user_ids])

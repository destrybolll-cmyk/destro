import sqlite3
from typing import Optional


class Database:
    def __init__(self, db_path: str = "bot.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id   INTEGER UNIQUE NOT NULL,
                    first_name TEXT DEFAULT '',
                    username  TEXT DEFAULT '',
                    language_code TEXT DEFAULT '',
                    is_banned INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id   INTEGER NOT NULL,
                    anon_id   INTEGER NOT NULL,
                    text      TEXT DEFAULT '',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Migration: add language_code if upgrading from old schema
            try:
                conn.execute("ALTER TABLE users ADD COLUMN language_code TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
            # Migration: add direction column to messages
            try:
                conn.execute("ALTER TABLE messages ADD COLUMN direction TEXT DEFAULT 'user_to_admin'")
            except sqlite3.OperationalError:
                pass


    def add_user(self, user_id: int, first_name: str = "", username: str = "", language_code: str = "") -> tuple:
        with self._get_conn() as conn:
            existing = conn.execute(
                "SELECT id, is_banned FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if existing:
                conn.execute("""
                    UPDATE users
                    SET first_name = ?, username = ?, language_code = ?, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """, (first_name, username, language_code, user_id))
                return existing["id"], existing["is_banned"]
            cursor = conn.execute("""
                INSERT INTO users (user_id, first_name, username, language_code)
                VALUES (?, ?, ?, ?)
            """, (user_id, first_name, username, language_code))
            return cursor.lastrowid, 0

    def user_exists(self, user_id: int) -> bool:
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
            ).fetchone() is not None

    def is_banned(self, user_id: int) -> bool:
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT is_banned FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return result is not None and result["is_banned"] == 1

    def user_exists_by_anon(self, anon_id: int) -> bool:
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT 1 FROM users WHERE id = ?", (anon_id,)
            ).fetchone() is not None

    def ban_user(self, user_id: int):
        with self._get_conn() as conn:
            conn.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))

    def unban_user(self, user_id: int):
        with self._get_conn() as conn:
            conn.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))

    def get_user(self, user_id: int) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()

    def get_user_by_anon(self, anon_id: int) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE id = ?", (anon_id,)
            ).fetchone()

    def get_user_id_by_anon(self, anon_id: int) -> Optional[int]:
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT user_id FROM users WHERE id = ?", (anon_id,)
            ).fetchone()
            return result["user_id"] if result else None

    def get_anon_id_by_user_id(self, user_id: int) -> Optional[int]:
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT id FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return result["id"] if result else None

    def get_all_users(self):
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users ORDER BY last_active DESC"
            ).fetchall()

    def get_banned_users(self):
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE is_banned = 1 ORDER BY last_active DESC"
            ).fetchall()

    def save_message(self, user_id: int, anon_id: int, text: str, direction: str = "user_to_admin"):
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO messages (user_id, anon_id, text, direction) VALUES (?, ?, ?, ?)",
                (user_id, anon_id, text, direction),
            )

    def get_last_admin_message(self, user_id: int) -> Optional[str]:
        with self._get_conn() as conn:
            result = conn.execute(
                "SELECT text FROM messages WHERE user_id = ? AND direction = 'admin_to_user' ORDER BY id DESC LIMIT 1",
                (user_id,),
            ).fetchone()
            return result["text"] if result else None

    def rename_user(self, anon_id: int, new_name: str):
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE users SET first_name = ? WHERE id = ?",
                (new_name, anon_id),
            )

    def delete_user(self, anon_id: int):
        with self._get_conn() as conn:
            user = conn.execute("SELECT user_id FROM users WHERE id = ?", (anon_id,)).fetchone()
            if user:
                uid = user["user_id"]
                conn.execute("DELETE FROM messages WHERE user_id = ?", (uid,))
                conn.execute("DELETE FROM users WHERE id = ?", (anon_id,))

    def get_messages_since(self, minutes: int = 60):
        with self._get_conn() as conn:
            return conn.execute(
                """
                SELECT m.*, u.first_name, u.username
                FROM messages m
                LEFT JOIN users u ON m.user_id = u.user_id
                WHERE m.timestamp >= datetime('now', ?)
                ORDER BY m.timestamp ASC
                """,
                (f"-{minutes} minutes",),
            ).fetchall()

    def get_stats(self) -> dict:
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            banned = conn.execute(
                "SELECT COUNT(*) FROM users WHERE is_banned = 1"
            ).fetchone()[0]
            return {"total": total, "banned": banned, "active": total - banned}




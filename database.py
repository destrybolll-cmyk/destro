import sqlite3
from typing import Optional


class Database:
    def __init__(self, db_path: str = "bot.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    player1_anon_id INTEGER NOT NULL,
                    player2_anon_id INTEGER NOT NULL,
                    player1_user_id INTEGER NOT NULL,
                    player2_user_id INTEGER NOT NULL,
                    board           TEXT DEFAULT '_________',
                    current_turn    INTEGER,
                    x_player        INTEGER,
                    status          TEXT DEFAULT 'pending',
                    winner          TEXT DEFAULT '',
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at     TIMESTAMP
                )
            """)
            try:
                conn.execute("ALTER TABLE users ADD COLUMN language_code TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN is_deleted INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN appeal_count INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN last_appeal TIMESTAMP")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE games ADD COLUMN admin_msg_id INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE messages ADD COLUMN direction TEXT DEFAULT 'user_to_admin'")
            except sqlite3.OperationalError:
                pass

    def add_user(self, user_id: int, first_name: str = "", username: str = "", language_code: str = "") -> tuple:
        with self._get_conn() as conn:
            existing = conn.execute(
                "SELECT id, is_banned, is_deleted FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if existing:
                conn.execute("""
                    UPDATE users
                    SET first_name = ?, username = ?, language_code = ?, last_active = CURRENT_TIMESTAMP,
                        is_deleted = 0
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
                "SELECT * FROM users WHERE is_deleted != 1 ORDER BY last_active DESC"
            ).fetchall()

    def get_deleted_users(self):
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE is_deleted = 1 ORDER BY last_active DESC"
            ).fetchall()

    def get_banned_users(self):
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE is_banned = 1 ORDER BY last_active DESC"
            ).fetchall()

    def get_blocked_users(self):
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE is_blocked = 1 ORDER BY last_active DESC"
            ).fetchall()

    def mark_blocked(self, user_id: int):
        with self._get_conn() as conn:
            conn.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))

    def unmark_blocked(self, anon_id: int):
        with self._get_conn() as conn:
            conn.execute("UPDATE users SET is_blocked = 0 WHERE id = ?", (anon_id,))

    def check_appeal_limit(self, anon_id: int, max_per_hour: int = 3) -> tuple:
        with self._get_conn() as conn:
            u = conn.execute("SELECT appeal_count, last_appeal FROM users WHERE id = ?", (anon_id,)).fetchone()
            if not u:
                return True, 0
            from datetime import datetime, timedelta
            last = u["last_appeal"]
            count = u["appeal_count"] or 0
            if last:
                last_dt = datetime.fromisoformat(last)
                if datetime.now() - last_dt > timedelta(hours=1):
                    conn.execute("UPDATE users SET appeal_count = 0, last_appeal = NULL WHERE id = ?", (anon_id,))
                    return True, 0
            return count < max_per_hour, max_per_hour - count

    def increment_appeal(self, anon_id: int):
        with self._get_conn() as conn:
            from datetime import datetime
            conn.execute("""
                UPDATE users SET appeal_count = COALESCE(appeal_count, 0) + 1, last_appeal = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), anon_id))

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
            conn.execute("UPDATE users SET is_deleted = 1 WHERE id = ?", (anon_id,))

    def restore_user(self, anon_id: int):
        with self._get_conn() as conn:
            conn.execute("UPDATE users SET is_deleted = 0 WHERE id = ?", (anon_id,))

    def hard_delete_user(self, anon_id: int):
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
            total = conn.execute("SELECT COUNT(*) FROM users WHERE is_deleted != 1").fetchone()[0]
            banned = conn.execute(
                "SELECT COUNT(*) FROM users WHERE is_banned = 1 AND is_deleted != 1"
            ).fetchone()[0]
            deleted = conn.execute("SELECT COUNT(*) FROM users WHERE is_deleted = 1").fetchone()[0]
            return {"total": total, "banned": banned, "active": total - banned, "deleted": deleted}

    # ──────────── Tic-Tac-Toe methods ────────────

    def create_game(self, p1_anon: int, p2_anon: int, p1_uid: int, p2_uid: int, x_player: int) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO games (player1_anon_id, player2_anon_id, player1_user_id, player2_user_id,
                                   board, current_turn, x_player, status)
                VALUES (?, ?, ?, ?, '_________', ?, ?, 'pending')
            """, (p1_anon, p2_anon, p1_uid, p2_uid, x_player, x_player))
            return cursor.lastrowid

    def get_player_game(self, anon_id: int, statuses=("active", "pending")) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            placeholders = ",".join("?" for _ in statuses)
            return conn.execute(f"""
                SELECT * FROM games
                WHERE (player1_anon_id = ? OR player2_anon_id = ?)
                  AND status IN ({placeholders})
                ORDER BY id DESC LIMIT 1
            """, (anon_id, anon_id, *statuses)).fetchone()

    def get_game(self, game_id: int) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            return conn.execute("SELECT * FROM games WHERE id = ?", (game_id,)).fetchone()

    def update_game(self, game_id: int, **kwargs):
        with self._get_conn() as conn:
            sets = ", ".join(f"{k} = ?" for k in kwargs)
            vals = list(kwargs.values()) + [game_id]
            conn.execute(f"UPDATE games SET {sets} WHERE id = ?", vals)

    def get_player_stats(self, anon_id: int) -> dict:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM games
                WHERE (player1_anon_id = ? OR player2_anon_id = ?)
                  AND status = 'finished'
            """, (anon_id, anon_id)).fetchall()
        wins = losses = draws = 0
        opponents = set()
        for r in rows:
            x = r["x_player"]
            w = r["winner"]
            opp = r["player2_anon_id"] if r["player1_anon_id"] == anon_id else r["player1_anon_id"]
            opponents.add(opp)
            if w == "draw":
                draws += 1
            elif (anon_id == x and w == "X") or (anon_id != x and w == "O"):
                wins += 1
            else:
                losses += 1
        return {"wins": wins, "losses": losses, "draws": draws, "total": wins + losses + draws, "opponents": len(opponents)}

    def get_game_history(self, anon_id: int, opponent_anon_id: int = None, limit: int = 20) -> list:
        with self._get_conn() as conn:
            if opponent_anon_id:
                return conn.execute("""
                    SELECT * FROM games
                    WHERE ((player1_anon_id = ? AND player2_anon_id = ?)
                        OR (player1_anon_id = ? AND player2_anon_id = ?))
                      AND status = 'finished'
                    ORDER BY id DESC LIMIT ?
                """, (anon_id, opponent_anon_id, opponent_anon_id, anon_id, limit)).fetchall()
            return conn.execute("""
                SELECT * FROM games
                WHERE (player1_anon_id = ? OR player2_anon_id = ?)
                  AND status = 'finished'
                ORDER BY id DESC LIMIT ?
            """, (anon_id, anon_id, limit)).fetchall()

import os
import json
import urllib.request
from typing import Optional, Any


class TursoRow:
    def __init__(self, cols: list, row: list):
        self._cols = [c["name"] for c in cols]
        self._vals = []
        for v in row:
            if isinstance(v, dict):
                t = v.get("type", "text")
                val = v.get("value")
                if t == "integer" or t == "float":
                    val = int(val) if "." not in str(val) else float(val)
                elif t == "null" or val is None:
                    val = None
                elif t == "text":
                    pass
                self._vals.append(val)
            else:
                self._vals.append(v)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._vals[key]
        try:
            idx = self._cols.index(key)
            return self._vals[idx]
        except ValueError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self._vals[key] = value
        else:
            idx = self._cols.index(key)
            self._vals[idx] = value

    def get(self, key, default=None):
        try:
            return self[key] if self[key] is not None else default
        except (KeyError, IndexError):
            return default

    def keys(self):
        return self._cols


class Database:
    def __init__(self, db_path: str = ""):
        self.db_url = os.getenv("TURSO_DB_URL", "https://cookie-anon-bot-destrybolll-cmyk.aws-us-west-2.turso.io")
        self.db_token = os.getenv("TURSO_DB_TOKEN", "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Nzg3NjM3MzQsImlkIjoiMDE5ZTI2OTQtMWUwMS03MWM5LWI3ZTQtMDA2MDQ5OWU4ZTZlIiwicmlkIjoiMmI2YTQzYzItZDhjNS00OGRjLTk0NTYtMjNhM2JmNjkwOWIwIn0.d7jSily5EfbjH34WM3-Gq6kYcVFc1b0xmpQJk91iQunrVXw9-mUZUZVoXQdX2OdKgfoQde3M0CwDHCFq6b6HBw")
        self._last_rowid = 0
        self._init_db()

    def _req(self, sql: str, params: list = None, readonly: bool = False) -> dict:
        stmt = {"sql": sql}
        if params:
            typed_args = []
            for v in params:
                if v is None:
                    typed_args.append({"type": "null"})
                elif isinstance(v, bool):
                    typed_args.append({"type": "text", "value": "1" if v else "0"})
                elif isinstance(v, int):
                    typed_args.append({"type": "text", "value": str(v)})
                elif isinstance(v, float):
                    typed_args.append({"type": "text", "value": str(v)})
                else:
                    typed_args.append({"type": "text", "value": str(v)})
            stmt["args"] = typed_args
        query = {"requests": [{"type": "execute", "stmt": stmt}]}
        if readonly:
            query["requests"][0]["type"] = "execute"
        qs = json.dumps(query)
        req = urllib.request.Request(
            f'{self.db_url}/v2/pipeline',
            data=qs.encode(),
            headers={'Authorization': f'Bearer {self.db_token}', 'Content-Type': 'application/json'},
            method='POST',
        )
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        r = result["results"][0]
        if r["type"] != "ok":
            raise Exception(f"DB error: {r}")
        res = r["response"]["result"]
        lid = res.get("last_insert_rowid")
        if lid is not None:
            self._last_rowid = int(lid)
        return res

    def _exec(self, sql: str, params: list = None) -> dict:
        r = self._req(sql, params)
        try:
            lid = r.get("last_insert_rowid")
            if lid is not None:
                return {"rowid": int(lid)}
        except Exception:
            pass
        return r

    def _fetchall(self, sql: str, params: list = None) -> list:
        r = self._req(sql, params)
        cols = r.get("cols", [])
        return [TursoRow(cols, row) for row in r.get("rows", [])]

    def _fetchone(self, sql: str, params: list = None):
        rows = self._fetchall(sql, params)
        return rows[0] if rows else None

    def _fetchval(self, sql: str, params: list = None):
        r = self._req(sql, params)
        rows = r.get("rows", [])
        if not rows:
            return None
        v = rows[0][0]
        if isinstance(v, dict):
            t = v.get("type", "text")
            val = v.get("value")
            if t == "integer" or t == "float":
                return int(val) if "." not in str(val) else float(val)
            return val
        return v

    def _lastrowid(self) -> int:
        return self._last_rowid

    def _init_db(self):
        for sql in [
            '''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER UNIQUE NOT NULL,
                first_name TEXT DEFAULT '', username TEXT DEFAULT '', language_code TEXT DEFAULT '',
                is_banned INTEGER DEFAULT 0, is_blocked INTEGER DEFAULT 0, is_deleted INTEGER DEFAULT 0,
                ban_reason TEXT DEFAULT '',
                appeal_count INTEGER DEFAULT 0, last_appeal TIMESTAMP,
                cookies INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                anon_id INTEGER NOT NULL, text TEXT DEFAULT '',
                direction TEXT DEFAULT 'user_to_admin',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player1_anon_id INTEGER NOT NULL, player2_anon_id INTEGER NOT NULL,
                player1_user_id INTEGER NOT NULL, player2_user_id INTEGER NOT NULL,
                board TEXT DEFAULT '_________', current_turn INTEGER, x_player INTEGER,
                status TEXT DEFAULT 'pending', winner TEXT DEFAULT '',
                admin_msg_id INTEGER DEFAULT 0, user_msg_id INTEGER DEFAULT 0,
                rematch_sent INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS dice_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player1_anon_id INTEGER NOT NULL, player2_anon_id INTEGER NOT NULL,
                p1_score INTEGER DEFAULT 0, p2_score INTEGER DEFAULT 0,
                winner TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS ideas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, anon_id INTEGER NOT NULL, text TEXT NOT NULL,
                status TEXT DEFAULT 'pending', cookies_awarded INTEGER DEFAULT 0,
                admin_comment TEXT DEFAULT '', is_top INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
            '''CREATE TABLE IF NOT EXISTS diary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''',
        ]:
            self._exec(sql)
    # ──────────── User methods ────────────

    def add_user(self, user_id: int, first_name: str = "", username: str = "", language_code: str = "") -> tuple:
        existing = self._fetchone("SELECT id, is_banned, is_deleted FROM users WHERE user_id = ?", [user_id])
        if existing:
            self._exec("UPDATE users SET language_code = ?, last_active = CURRENT_TIMESTAMP, is_deleted = 0 WHERE user_id = ?",
                       [language_code, user_id])
            return existing["id"], existing["is_banned"]
        self._exec("INSERT INTO users (user_id, first_name, username, language_code) VALUES (?, ?, ?, ?)",
                   [user_id, first_name, username, language_code])
        return self._lastrowid(), 0

    def user_exists(self, user_id: int) -> bool:
        return self._fetchone("SELECT 1 FROM users WHERE user_id = ?", [user_id]) is not None

    def is_banned(self, user_id: int) -> bool:
        r = self._fetchone("SELECT is_banned FROM users WHERE user_id = ?", [user_id])
        return r is not None and r["is_banned"] == 1

    def user_exists_by_anon(self, anon_id: int) -> bool:
        return self._fetchone("SELECT 1 FROM users WHERE id = ?", [anon_id]) is not None

    def ban_user(self, user_id: int):
        self._exec("UPDATE users SET is_banned = 1 WHERE user_id = ?", [user_id])

    def unban_user(self, user_id: int):
        self._exec("UPDATE users SET is_banned = 0 WHERE user_id = ?", [user_id])

    def ban_user_with_reason(self, user_id: int, reason: str = ""):
        self._exec("UPDATE users SET is_banned = 1, ban_reason = ? WHERE user_id = ?", [reason, user_id])

    def get_user(self, user_id: int) -> Optional[TursoRow]:
        return self._fetchone("SELECT * FROM users WHERE user_id = ?", [user_id])

    def get_user_by_anon(self, anon_id: int) -> Optional[TursoRow]:
        return self._fetchone("SELECT * FROM users WHERE id = ?", [anon_id])

    def get_user_id_by_anon(self, anon_id: int) -> Optional[int]:
        r = self._fetchone("SELECT user_id FROM users WHERE id = ?", [anon_id])
        return r["user_id"] if r else None

    def get_anon_id_by_user_id(self, user_id: int) -> Optional[int]:
        r = self._fetchone("SELECT id FROM users WHERE user_id = ?", [user_id])
        return r["id"] if r else None

    def get_all_users(self):
        return self._fetchall("SELECT * FROM users WHERE is_deleted != 1 ORDER BY last_active DESC")

    def get_deleted_users(self):
        return self._fetchall("SELECT * FROM users WHERE is_deleted = 1 ORDER BY last_active DESC")

    def get_banned_users(self):
        return self._fetchall("SELECT * FROM users WHERE is_banned = 1 ORDER BY last_active DESC")

    def get_blocked_users(self):
        return self._fetchall("SELECT * FROM users WHERE is_blocked = 1 ORDER BY last_active DESC")

    def mark_blocked(self, user_id: int):
        self._exec("UPDATE users SET is_blocked = 1 WHERE user_id = ?", [user_id])

    def unmark_blocked(self, anon_id: int):
        self._exec("UPDATE users SET is_blocked = 0 WHERE id = ?", [anon_id])

    def check_appeal_limit(self, anon_id: int, max_per_day: int = 3) -> tuple:
        u = self._fetchone("SELECT appeal_count, last_appeal FROM users WHERE id = ?", [anon_id])
        if not u:
            return True, 0
        from datetime import datetime, timedelta
        last = u["last_appeal"]
        count = u["appeal_count"] or 0
        if last:
            last_dt = datetime.fromisoformat(last)
            if datetime.now() - last_dt > timedelta(days=1):
                self._exec("UPDATE users SET appeal_count = 0, last_appeal = NULL WHERE id = ?", [anon_id])
                return True, 0
        return count < max_per_day, max_per_day - count

    def increment_appeal(self, anon_id: int):
        from datetime import datetime
        self._exec("UPDATE users SET appeal_count = COALESCE(appeal_count, 0) + 1, last_appeal = ? WHERE id = ?",
                   [datetime.now().isoformat(), anon_id])

    def save_message(self, user_id: int, anon_id: int, text: str, direction: str = "user_to_admin"):
        self._exec("INSERT INTO messages (user_id, anon_id, text, direction) VALUES (?, ?, ?, ?)",
                   [user_id, anon_id, text, direction])

    def get_last_admin_message(self, user_id: int) -> Optional[str]:
        r = self._fetchone("SELECT text FROM messages WHERE user_id = ? AND direction = 'admin_to_user' ORDER BY id DESC LIMIT 1", [user_id])
        return r["text"] if r else None

    def rename_user(self, anon_id: int, new_name: str):
        self._exec("UPDATE users SET first_name = ? WHERE id = ?", [new_name, anon_id])

    def delete_user(self, anon_id: int):
        self._exec("UPDATE users SET is_deleted = 1 WHERE id = ?", [anon_id])

    def restore_user(self, anon_id: int):
        self._exec("UPDATE users SET is_deleted = 0 WHERE id = ?", [anon_id])

    def hard_delete_user(self, anon_id: int):
        user = self._fetchone("SELECT user_id FROM users WHERE id = ?", [anon_id])
        if user:
            uid = user["user_id"]
            self._exec("DELETE FROM messages WHERE user_id = ?", [uid])
            self._exec("DELETE FROM users WHERE id = ?", [anon_id])

    def get_messages_since(self, minutes: int = 60):
        return self._fetchall(
            """SELECT m.*, u.first_name, u.username FROM messages m
            LEFT JOIN users u ON m.user_id = u.user_id
            WHERE m.timestamp >= datetime('now', ?) ORDER BY m.timestamp ASC""",
            [f"-{minutes} minutes"])

    def get_all_messages(self, page: int = 1, per_page: int = 15):
        offset = (page - 1) * per_page
        rows = self._fetchall(
            """SELECT m.*, u.first_name, u.username FROM messages m
            LEFT JOIN users u ON m.user_id = u.user_id
            ORDER BY m.timestamp DESC LIMIT ? OFFSET ?""", [per_page, offset])
        total = self._fetchval("SELECT COUNT(*) FROM messages")
        total_pages = max(1, (total + per_page - 1) // per_page)
        return rows, total_pages

    def get_user_messages(self, anon_id: int, page: int = 1, per_page: int = 20, date_filter: str = None):
        offset = (page - 1) * per_page
        where = "messages.anon_id = ?"
        params = [anon_id]
        if date_filter == "today":
            where += " AND date(messages.timestamp) = date('now')"
        elif date_filter == "yesterday":
            where += " AND date(messages.timestamp) = date('now', '-1 day')"
        elif date_filter:
            where += " AND date(messages.timestamp) = date(?)"
            params.append(date_filter)
        rows = self._fetchall(
            f"""SELECT messages.*, u.first_name, u.username FROM messages
            LEFT JOIN users u ON messages.user_id = u.user_id
            WHERE {where} ORDER BY messages.timestamp DESC LIMIT ? OFFSET ?""",
            params + [per_page, offset])
        uid_row = self._fetchone("SELECT user_id FROM users WHERE id = ?", [anon_id])
        total = self._fetchval(f"SELECT COUNT(*) FROM messages WHERE {where}", params)
        total_pages = max(1, (total + per_page - 1) // per_page)
        return rows, total_pages, uid_row["user_id"] if uid_row else None

    def get_stats(self) -> dict:
        total = self._fetchval("SELECT COUNT(*) FROM users WHERE is_deleted != 1")
        banned = self._fetchval("SELECT COUNT(*) FROM users WHERE is_banned = 1 AND is_deleted != 1")
        deleted = self._fetchval("SELECT COUNT(*) FROM users WHERE is_deleted = 1")
        return {"total": total, "banned": banned, "active": total - banned, "deleted": deleted}

    # ──────────── Game methods ────────────

    def create_game(self, p1_anon: int, p2_anon: int, p1_uid: int, p2_uid: int, x_player: int) -> int:
        self._exec("INSERT INTO games (player1_anon_id, player2_anon_id, player1_user_id, player2_user_id, board, current_turn, x_player, status) VALUES (?, ?, ?, ?, '_________', ?, ?, 'pending')",
                   [p1_anon, p2_anon, p1_uid, p2_uid, x_player, x_player])
        return self._lastrowid()

    def get_player_game(self, anon_id: int, statuses=("active", "pending")) -> Optional[TursoRow]:
        placeholders = ",".join("?" for _ in statuses)
        return self._fetchone(f"SELECT * FROM games WHERE (player1_anon_id = ? OR player2_anon_id = ?) AND status IN ({placeholders}) ORDER BY id DESC LIMIT 1",
                             [anon_id, anon_id, *statuses])

    def get_game(self, game_id: int) -> Optional[TursoRow]:
        return self._fetchone("SELECT * FROM games WHERE id = ?", [game_id])

    def update_game(self, game_id: int, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [game_id]
        self._exec(f"UPDATE games SET {sets} WHERE id = ?", vals)

    def get_player_stats(self, anon_id: int) -> dict:
        rows = self._fetchall("SELECT * FROM games WHERE (player1_anon_id = ? OR player2_anon_id = ?) AND status = 'finished'", [anon_id, anon_id])
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
        if opponent_anon_id:
            return self._fetchall(
                "SELECT * FROM games WHERE ((player1_anon_id = ? AND player2_anon_id = ?) OR (player1_anon_id = ? AND player2_anon_id = ?)) AND status = 'finished' ORDER BY id DESC LIMIT ?",
                [anon_id, opponent_anon_id, opponent_anon_id, anon_id, limit])
        return self._fetchall("SELECT * FROM games WHERE (player1_anon_id = ? OR player2_anon_id = ?) AND status = 'finished' ORDER BY id DESC LIMIT ?",
                             [anon_id, anon_id, limit])

    # ──────────── Ideas methods ────────────

    def save_idea(self, anon_id: int, user_id: int, text: str) -> int:
        self._exec("INSERT INTO ideas (user_id, anon_id, text) VALUES (?, ?, ?)", [user_id, anon_id, text])
        return self._lastrowid()

    def get_ideas(self, status: str = None):
        if status:
            return self._fetchall(
                "SELECT s.*, u.first_name, u.username FROM ideas s LEFT JOIN users u ON s.anon_id = u.id WHERE s.status = ? ORDER BY s.id DESC", [status])
        return self._fetchall(
            "SELECT s.*, u.first_name, u.username FROM ideas s LEFT JOIN users u ON s.anon_id = u.id ORDER BY s.id DESC")

    def get_idea(self, idea_id: int):
        return self._fetchone("SELECT * FROM ideas WHERE id = ?", [idea_id])

    def count_today_ideas(self, user_id: int) -> int:
        return self._fetchval("SELECT COUNT(*) FROM ideas WHERE user_id = ? AND date(created_at) = date('now')", [user_id])

    def update_idea(self, idea_id: int, status: str, comment: str = ""):
        if comment:
            self._exec("UPDATE ideas SET status = ?, admin_comment = ? WHERE id = ?", [status, comment, idea_id])
        else:
            self._exec("UPDATE ideas SET status = ? WHERE id = ?", [status, idea_id])

    # ──────────── Dice methods ────────────

    def create_dice_game(self, p1_anon: int, p2_anon: int) -> int:
        self._exec("INSERT INTO dice_games (player1_anon_id, player2_anon_id) VALUES (?, ?)", [p1_anon, p2_anon])
        return self._lastrowid()

    def get_dice_game(self, game_id: int):
        return self._fetchone("SELECT * FROM dice_games WHERE id = ?", [game_id])

    def finish_dice_game(self, game_id: int, p1_score: int, p2_score: int):
        winner = ""
        if p1_score > p2_score: winner = "p1"
        elif p2_score > p1_score: winner = "p2"
        else: winner = "draw"
        self._exec("UPDATE dice_games SET p1_score = ?, p2_score = ?, winner = ? WHERE id = ?", [p1_score, p2_score, winner, game_id])

    def get_dice_stats(self, anon_id: int) -> dict:
        rows = self._fetchall("SELECT * FROM dice_games WHERE (player1_anon_id = ? OR player2_anon_id = ?)", [anon_id, anon_id])
        wins = losses = draws = 0
        opponents = set()
        for r in rows:
            opp = r["player2_anon_id"] if r["player1_anon_id"] == anon_id else r["player1_anon_id"]
            opponents.add(opp)
            w = r["winner"]
            if w == "draw": draws += 1
            elif (r["player1_anon_id"] == anon_id and w == "p1") or (r["player2_anon_id"] == anon_id and w == "p2"):
                wins += 1
            else: losses += 1
        return {"wins": wins, "losses": losses, "draws": draws, "total": wins + losses + draws, "opponents": len(opponents)}

    # ──────────── Diary methods ────────────

    def add_diary_entry(self, text: str) -> int:
        self._exec("INSERT INTO diary (text) VALUES (?)", [text])
        return self._lastrowid()

    def get_diary_entries(self, page: int = 1, per_page: int = 5):
        offset = (page - 1) * per_page
        rows = self._fetchall("SELECT * FROM diary ORDER BY id DESC LIMIT ? OFFSET ?", [per_page, offset])
        total = self._fetchval("SELECT COUNT(*) FROM diary")
        total_pages = max(1, (total + per_page - 1) // per_page)
        return rows, total_pages

    def get_diary_entry(self, entry_id: int):
        return self._fetchone("SELECT * FROM diary WHERE id = ?", [entry_id])

    def update_diary_entry(self, entry_id: int, text: str):
        self._exec("UPDATE diary SET text = ? WHERE id = ?", [text, entry_id])

    def delete_diary_entry(self, entry_id: int):
        self._exec("DELETE FROM diary WHERE id = ?", [entry_id])

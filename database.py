import sqlite3
import os
from typing import List, Dict, Optional

DB_PATH = os.getenv("DB_PATH", "bot.db")


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                first_name  TEXT    NOT NULL DEFAULT '',
                last_name   TEXT    NOT NULL DEFAULT '',
                username    TEXT    NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS user_groups (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                group_name  TEXT    NOT NULL,
                UNIQUE(chat_id, user_id, group_name)
            );

            CREATE INDEX IF NOT EXISTS idx_users_chat ON users(chat_id);
            CREATE INDEX IF NOT EXISTS idx_ug_chat    ON user_groups(chat_id);
        """)
        self.conn.commit()

    # ─── Write ────────────────────────────────────────────────────────────────

    def add_user(self, chat_id: int, user_id: int, first_name: str,
                 last_name: str, username: str, group_name: str):
        """Добавляет/обновляет пользователя и записывает его в группу."""
        cur = self.conn.cursor()

        # upsert user info
        existing = cur.execute(
            "SELECT id FROM users WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        ).fetchone()
        if existing:
            cur.execute(
                "UPDATE users SET first_name=?, last_name=?, username=? WHERE chat_id=? AND user_id=?",
                (first_name, last_name, username, chat_id, user_id)
            )
        else:
            cur.execute(
                "INSERT INTO users(chat_id, user_id, first_name, last_name, username) VALUES(?,?,?,?,?)",
                (chat_id, user_id, first_name, last_name, username)
            )

        # upsert group membership
        cur.execute(
            "INSERT OR IGNORE INTO user_groups(chat_id, user_id, group_name) VALUES(?,?,?)",
            (chat_id, user_id, group_name)
        )
        self.conn.commit()

    def remove_user(self, chat_id: int, user_id: int):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM user_groups WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        cur.execute("DELETE FROM users WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        self.conn.commit()

    # ─── Read ─────────────────────────────────────────────────────────────────

    def get_all_users(self, chat_id: int) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT DISTINCT user_id, first_name, last_name, username FROM users WHERE chat_id=?",
            (chat_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_group_users(self, chat_id: int, group_name: str) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT u.user_id, u.first_name, u.last_name, u.username
            FROM users u
            JOIN user_groups ug ON ug.chat_id=u.chat_id AND ug.user_id=u.user_id
            WHERE u.chat_id=? AND LOWER(ug.group_name)=LOWER(?)
            """,
            (chat_id, group_name)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_user_groups(self, chat_id: int, user_id: int) -> List[str]:
        rows = self.conn.execute(
            "SELECT group_name FROM user_groups WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        ).fetchall()
        return [r["group_name"] for r in rows]

    def get_all_groups(self, chat_id: int) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT group_name AS name, COUNT(*) AS count
            FROM user_groups WHERE chat_id=?
            GROUP BY LOWER(group_name)
            ORDER BY count DESC
            """,
            (chat_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def find_by_username(self, chat_id: int, username: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM users WHERE chat_id=? AND LOWER(username)=LOWER(?)",
            (chat_id, username)
        ).fetchone()
        return dict(row) if row else None

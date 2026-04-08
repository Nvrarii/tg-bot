import os
import sqlite3
from typing import List, Dict, Optional

DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

# Убедимся что папка существует
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id    INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                first_name TEXT NOT NULL DEFAULT '',
                last_name  TEXT NOT NULL DEFAULT '',
                username   TEXT NOT NULL DEFAULT '',
                UNIQUE(chat_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS user_groups (
                chat_id    INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                UNIQUE(chat_id, user_id, group_name)
            );
        """)
        self.conn.commit()

    def register_user(self, chat_id, user_id, first_name, last_name, username):
        self.conn.execute("""
            INSERT INTO users(chat_id, user_id, first_name, last_name, username)
            VALUES(?,?,?,?,?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                username=excluded.username
        """, (chat_id, user_id, first_name, last_name, username))
        self.conn.commit()

    def add_user(self, chat_id, user_id, first_name, last_name, username, group_name):
        self.register_user(chat_id, user_id, first_name, last_name, username)
        self.conn.execute(
            "INSERT OR IGNORE INTO user_groups(chat_id, user_id, group_name) VALUES(?,?,?)",
            (chat_id, user_id, group_name)
        )
        self.conn.commit()

    def remove_user(self, chat_id, user_id):
        self.conn.execute("DELETE FROM user_groups WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        self.conn.execute("DELETE FROM users WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        self.conn.commit()

    def get_all_users(self, chat_id) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT user_id, first_name, last_name, username FROM users WHERE chat_id=?", (chat_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_group_users(self, chat_id, group_name) -> List[Dict]:
        rows = self.conn.execute("""
            SELECT u.user_id, u.first_name, u.last_name, u.username
            FROM users u
            JOIN user_groups ug ON ug.chat_id=u.chat_id AND ug.user_id=u.user_id
            WHERE u.chat_id=? AND LOWER(ug.group_name)=LOWER(?)
        """, (chat_id, group_name)).fetchall()
        return [dict(r) for r in rows]

    def get_user_groups(self, chat_id, user_id) -> List[str]:
        rows = self.conn.execute(
            "SELECT group_name FROM user_groups WHERE chat_id=? AND user_id=?", (chat_id, user_id)
        ).fetchall()
        return [r["group_name"] for r in rows]

    def get_all_groups(self, chat_id) -> List[Dict]:
        rows = self.conn.execute("""
            SELECT group_name AS name, COUNT(*) AS count
            FROM user_groups WHERE chat_id=?
            GROUP BY LOWER(group_name)
            ORDER BY count DESC
        """, (chat_id,)).fetchall()
        return [dict(r) for r in rows]

    def find_by_username(self, chat_id, username) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM users WHERE chat_id=? AND LOWER(username)=LOWER(?)", (chat_id, username)
        ).fetchone()
        return dict(row) if row else None

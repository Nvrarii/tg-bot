import os
from typing import List, Dict, Optional

DATABASE_URL = os.getenv("DATABASE_URL")  # Railway PostgreSQL

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    class Database:
        def __init__(self):
            self.url = DATABASE_URL
            self._init()

        def _conn(self):
            return psycopg2.connect(self.url, cursor_factory=psycopg2.extras.RealDictCursor)

        def _init(self):
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            chat_id    BIGINT NOT NULL,
                            user_id    BIGINT NOT NULL,
                            first_name TEXT   NOT NULL DEFAULT '',
                            last_name  TEXT   NOT NULL DEFAULT '',
                            username   TEXT   NOT NULL DEFAULT '',
                            PRIMARY KEY (chat_id, user_id)
                        );
                        CREATE TABLE IF NOT EXISTS user_groups (
                            chat_id    BIGINT NOT NULL,
                            user_id    BIGINT NOT NULL,
                            group_name TEXT   NOT NULL,
                            PRIMARY KEY (chat_id, user_id, group_name)
                        );
                    """)

        def register_user(self, chat_id, user_id, first_name, last_name, username):
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO users(chat_id, user_id, first_name, last_name, username)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT(chat_id, user_id) DO UPDATE SET
                            first_name=EXCLUDED.first_name,
                            last_name=EXCLUDED.last_name,
                            username=EXCLUDED.username
                    """, (chat_id, user_id, first_name, last_name, username))

        def add_user(self, chat_id, user_id, first_name, last_name, username, group_name):
            self.register_user(chat_id, user_id, first_name, last_name, username)
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO user_groups(chat_id, user_id, group_name)
                        VALUES(%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (chat_id, user_id, group_name))

        def remove_user(self, chat_id, user_id):
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM user_groups WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
                    cur.execute("DELETE FROM users WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))

        def get_all_users(self, chat_id) -> List[Dict]:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id, first_name, last_name, username FROM users WHERE chat_id=%s", (chat_id,))
                    return [dict(r) for r in cur.fetchall()]

        def get_group_users(self, chat_id, group_name) -> List[Dict]:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT u.user_id, u.first_name, u.last_name, u.username
                        FROM users u
                        JOIN user_groups ug ON ug.chat_id=u.chat_id AND ug.user_id=u.user_id
                        WHERE u.chat_id=%s AND LOWER(ug.group_name)=LOWER(%s)
                    """, (chat_id, group_name))
                    return [dict(r) for r in cur.fetchall()]

        def get_user_groups(self, chat_id, user_id) -> List[str]:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT group_name FROM user_groups WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
                    return [r["group_name"] for r in cur.fetchall()]

        def get_all_groups(self, chat_id) -> List[Dict]:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT group_name AS name, COUNT(*) AS count
                        FROM user_groups WHERE chat_id=%s
                        GROUP BY LOWER(group_name), group_name
                        ORDER BY count DESC
                    """, (chat_id,))
                    return [dict(r) for r in cur.fetchall()]

        def find_by_username(self, chat_id, username) -> Optional[Dict]:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM users WHERE chat_id=%s AND LOWER(username)=LOWER(%s)",
                        (chat_id, username)
                    )
                    r = cur.fetchone()
                    return dict(r) if r else None

else:
    import sqlite3

    DB_PATH = os.getenv("DB_PATH", "bot.db")

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

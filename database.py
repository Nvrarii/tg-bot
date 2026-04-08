import os
import asyncio
from typing import List, Dict, Optional

DATABASE_URL = os.getenv("DATABASE_URL")  # Railway PostgreSQL


if DATABASE_URL:
    import asyncpg

    class Database:
        def __init__(self):
            self.url = DATABASE_URL
            self.pool = None
            asyncio.get_event_loop().run_until_complete(self._init())

        async def _init(self):
            self.pool = await asyncpg.create_pool(self.url)
            async with self.pool.acquire() as conn:
                await conn.execute("""
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

        def _run(self, coro):
            return asyncio.get_event_loop().run_until_complete(coro)

        async def _register_user_async(self, chat_id, user_id, first_name, last_name, username):
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO users(chat_id, user_id, first_name, last_name, username)
                    VALUES($1,$2,$3,$4,$5)
                    ON CONFLICT(chat_id, user_id) DO UPDATE SET
                        first_name=EXCLUDED.first_name,
                        last_name=EXCLUDED.last_name,
                        username=EXCLUDED.username
                """, chat_id, user_id, first_name, last_name, username)

        def register_user(self, chat_id, user_id, first_name, last_name, username):
            self._run(self._register_user_async(chat_id, user_id, first_name, last_name, username))

        def add_user(self, chat_id, user_id, first_name, last_name, username, group_name):
            async def _add():
                await self._register_user_async(chat_id, user_id, first_name, last_name, username)
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO user_groups(chat_id, user_id, group_name)
                        VALUES($1,$2,$3) ON CONFLICT DO NOTHING
                    """, chat_id, user_id, group_name)
            self._run(_add())

        def remove_user(self, chat_id, user_id):
            async def _rem():
                async with self.pool.acquire() as conn:
                    await conn.execute("DELETE FROM user_groups WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
                    await conn.execute("DELETE FROM users WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
            self._run(_rem())

        def get_all_users(self, chat_id) -> List[Dict]:
            async def _get():
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("SELECT user_id, first_name, last_name, username FROM users WHERE chat_id=$1", chat_id)
                    return [dict(r) for r in rows]
            return self._run(_get())

        def get_group_users(self, chat_id, group_name) -> List[Dict]:
            async def _get():
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT u.user_id, u.first_name, u.last_name, u.username
                        FROM users u
                        JOIN user_groups ug ON ug.chat_id=u.chat_id AND ug.user_id=u.user_id
                        WHERE u.chat_id=$1 AND LOWER(ug.group_name)=LOWER($2)
                    """, chat_id, group_name)
                    return [dict(r) for r in rows]
            return self._run(_get())

        def get_user_groups(self, chat_id, user_id) -> List[str]:
            async def _get():
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("SELECT group_name FROM user_groups WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
                    return [r["group_name"] for r in rows]
            return self._run(_get())

        def get_all_groups(self, chat_id) -> List[Dict]:
            async def _get():
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT group_name AS name, COUNT(*) AS count
                        FROM user_groups WHERE chat_id=$1
                        GROUP BY LOWER(group_name), group_name
                        ORDER BY count DESC
                    """, chat_id)
                    return [dict(r) for r in rows]
            return self._run(_get())

        def find_by_username(self, chat_id, username) -> Optional[Dict]:
            async def _get():
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM users WHERE chat_id=$1 AND LOWER(username)=LOWER($2)",
                        chat_id, username
                    )
                    return dict(row) if row else None
            return self._run(_get())

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

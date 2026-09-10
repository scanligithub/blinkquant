# backend/scheduler/db.py
from __future__ import annotations
import os
import json
import asyncio
import aiosqlite
from contextlib import asynccontextmanager
from typing import Any, Optional, List, Dict

from .config import SCHEDULER_DB_PATH

_pool: Optional[aiosqlite.Connection] = None
_pool_lock = asyncio.Lock()


def _convert_placeholders(query: str) -> str:
    """将 $1,$2... 转为 ? (sqlite 占位符)"""
    import re
    return re.sub(r'\$\d+', '?', query)


def _row_to_dict(row: aiosqlite.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


class _ConnWrapper:
    """Wrap aiosqlite connection to provide asyncpg-like API"""
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    async def execute(self, query: str, *args):
        q = _convert_placeholders(query)
        # args might be a single tuple or multiple args
        params = args[0] if len(args) == 1 and isinstance(args[0], (tuple, list)) else args
        cur = await self._conn.execute(q, params)
        return cur

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        q = _convert_placeholders(query)
        params = args[0] if len(args) == 1 and isinstance(args[0], (tuple, list)) else args
        cur = await self._conn.execute(q, params)
        rows = await cur.fetchall()
        return [_row_to_dict(r) for r in rows]

    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        q = _convert_placeholders(query)
        params = args[0] if len(args) == 1 and isinstance(args[0], (tuple, list)) else args
        cur = await self._conn.execute(q, params)
        row = await cur.fetchone()
        return _row_to_dict(row) if row else None

    async def fetchval(self, query: str, *args) -> Any:
        q = _convert_placeholders(query)
        params = args[0] if len(args) == 1 and isinstance(args[0], (tuple, list)) else args
        cur = await self._conn.execute(q, params)
        row = await cur.fetchone()
        return row[0] if row else None


async def init_pool() -> None:
    global _pool
    async with _pool_lock:
        if _pool is not None:
            return
        os.makedirs(os.path.dirname(SCHEDULER_DB_PATH), exist_ok=True)
        _pool = await aiosqlite.connect(
            SCHEDULER_DB_PATH,
            isolation_level=None,
            check_same_thread=False,
        )
        _pool.row_factory = aiosqlite.Row
        await _pool.execute("PRAGMA journal_mode=WAL")
        await _pool.execute("PRAGMA synchronous=NORMAL")
        await _pool.execute("PRAGMA foreign_keys=ON")
        await _load_schema()
        print(f"[db] SQLite pool opened: {SCHEDULER_DB_PATH}")


async def _load_schema() -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    await _pool.executescript(sql)
    await _pool.commit()


async def close_pool() -> None:
    global _pool
    async with _pool_lock:
        if _pool is not None:
            await _pool.close()
            _pool = None
            print("[db] SQLite pool closed")


@asynccontextmanager
async def acquire():
    """事务上下文：BEGIN IMMEDIATE → yield wrapped conn → commit/rollback"""
    if _pool is None:
        await init_pool()
    await _pool.execute("BEGIN IMMEDIATE")
    wrapper = _ConnWrapper(_pool)
    try:
        yield wrapper
        await _pool.commit()
    except Exception:
        await _pool.rollback()
        raise


async def execute(query: str, *args) -> str:
    """执行单条 DML，返回影响行数字符串（兼容 asyncpg 返回 "UPDATE n"）"""
    if _pool is None:
        await init_pool()
    q = _convert_placeholders(query)
    cur = await _pool.execute(q, args)
    await _pool.commit()
    return f"{cur.rowcount} row{'s' if cur.rowcount != 1 else ''} affected"


async def fetch(query: str, *args) -> List[Dict[str, Any]]:
    if _pool is None:
        await init_pool()
    q = _convert_placeholders(query)
    cur = await _pool.execute(q, args)
    rows = await cur.fetchall()
    return [_row_to_dict(r) for r in rows]


async def fetchrow(query: str, *args) -> Optional[Dict[str, Any]]:
    if _pool is None:
        await init_pool()
    q = _convert_placeholders(query)
    cur = await _pool.execute(q, args)
    row = await cur.fetchone()
    return _row_to_dict(row) if row else None


async def fetchval(query: str, *args) -> Any:
    if _pool is None:
        await init_pool()
    q = _convert_placeholders(query)
    cur = await _pool.execute(q, args)
    row = await cur.fetchone()
    return row[0] if row else None


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def json_loads(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None
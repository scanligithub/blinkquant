# backend/scheduler/db.py
import os
import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncIterator

_pool: asyncpg.Pool | None = None

async def init_pool(dsn: str | None = None, min_size: int = 2, max_size: int = 10) -> None:
    global _pool
    dsn = dsn or os.getenv("POSTGRES_URL")
    if not dsn:
        raise RuntimeError("POSTGRES_URL not set")
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=60,
    )

async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

@asynccontextmanager
async def acquire() -> AsyncIterator[asyncpg.Connection]:
    if _pool is None:
        await init_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            yield conn

async def fetchrow(query: str, *args) -> asyncpg.Record | None:
    async with acquire() as conn:
        return await conn.fetchrow(query, *args)

async def fetch(query: str, *args) -> list[asyncpg.Record]:
    async with acquire() as conn:
        return await conn.fetch(query, *args)

async def execute(query: str, *args) -> str:
    async with acquire() as conn:
        return await conn.execute(query, *args)
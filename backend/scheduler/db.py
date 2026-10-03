# backend/scheduler/db.py
from __future__ import annotations
import os
import json
import asyncio
import aiosqlite
import traceback
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
        try:
            await _load_schema()
            await _migrate()
        except Exception as exc:
            # Keep the original exception visible in HF Space run logs. The
            # request layer may otherwise collapse this into a generic 500 {},
            # hiding the actual legacy-db migration failure.
            print(
                f"[db] schema/migration failed: {type(exc).__name__}: {exc}",
                flush=True,
            )
            traceback.print_exc()
            # Never leave a partially initialized connection behind. A failed
            # schema/migration must be retried cleanly or fail startup loudly.
            conn = _pool
            _pool = None
            await conn.close()
            raise
        print(f"[db] SQLite pool opened: {SCHEDULER_DB_PATH}")


async def _load_schema() -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    await _pool.executescript(sql)
    await _pool.commit()


async def _ensure_columns(table: str, definitions: dict[str, str]) -> None:
    """Additive SQLite migration for tables that may predate the current schema.

    CREATE TABLE IF NOT EXISTS never changes an existing persistent table.  User
    asset tables therefore need an explicit column migration before their routes
    are allowed to query newer fields.
    """
    cursor = await _pool.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in await cursor.fetchall()}
    for column, ddl in definitions.items():
        if column not in existing:
            await _pool.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


async def _migrate_user_asset_schema() -> None:
    await _ensure_columns("strategies", {
        "timeframe": "TEXT NOT NULL DEFAULT 'D'",
        "source_backtest_strategy_id": "INTEGER",
        "source_backtest_strategy_version": "INTEGER",
        "source_backtest_strategy_name": "TEXT",
        "source_backtest_strategy_trigger": "TEXT",
        "source_backtest_artifact_id": "INTEGER",
        "source_backtest_artifact_title": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    })
    await _ensure_columns("strategy_versions", {
        "timeframe": "TEXT NOT NULL DEFAULT 'D'",
        "source_backtest_strategy_id": "INTEGER",
        "source_backtest_strategy_version": "INTEGER",
        "source_backtest_strategy_name": "TEXT",
        "source_backtest_strategy_trigger": "TEXT",
        "source_backtest_artifact_id": "INTEGER",
        "source_backtest_artifact_title": "TEXT",
        "created_at": "TEXT",
    })
    await _ensure_columns("watchlists", {
        "is_default": "INTEGER NOT NULL DEFAULT 0",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    })
    await _ensure_columns("watchlist_items", {
        "created_at": "TEXT",
    })

    # SQLite ALTER TABLE ADD COLUMN only permits constant defaults. Backfill
    # timestamp columns explicitly after adding them so legacy persistent DBs migrate safely.
    await _pool.execute("UPDATE strategies SET created_at=COALESCE(created_at,datetime('now')), updated_at=COALESCE(updated_at,datetime('now'))")
    await _pool.execute("UPDATE strategy_versions SET created_at=COALESCE(created_at,datetime('now'))")
    await _pool.execute("UPDATE watchlists SET created_at=COALESCE(created_at,datetime('now')), updated_at=COALESCE(updated_at,datetime('now'))")
    await _pool.execute("UPDATE watchlist_items SET created_at=COALESCE(created_at,datetime('now'))")
    # A legacy database may contain multiple default lists from before the unique
    # partial index existed. Materialize the keeper IDs first, then update the
    # base table from that temporary table. This keeps the migration independent
    # of same-table subquery planner/locking behavior on old SQLite databases.
    await _pool.execute("DROP TABLE IF EXISTS temp._watchlist_default_keep")
    await _pool.execute(
        "CREATE TEMP TABLE _watchlist_default_keep AS "
        "SELECT user_id, MIN(id) AS keep_id "
        "FROM watchlists WHERE is_default=1 GROUP BY user_id"
    )
    await _pool.execute(
        "UPDATE watchlists SET is_default=0 "
        "WHERE is_default=1 AND id NOT IN "
        "(SELECT keep_id FROM _watchlist_default_keep)"
    )
    await _pool.execute("DROP TABLE temp._watchlist_default_keep")
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategies_user_updated "
        "ON strategies (user_id, updated_at DESC, id DESC)"
    )
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy "
        "ON strategy_versions (strategy_id, version_no DESC)"
    )
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlists_user "
        "ON watchlists (user_id)"
    )
    await _pool.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlists_one_default "
        "ON watchlists (user_id) WHERE is_default = 1"
    )
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlist_items_list "
        "ON watchlist_items (watchlist_id)"
    )
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlist_items_code "
        "ON watchlist_items (code)"
    )


async def _migrate() -> None:
    """增量迁移：ALTER TABLE ADD COLUMN IF NOT EXISTS（SQLite 不支持 IF NOT EXISTS，用 try/except）"""
    await _migrate_user_asset_schema()
    cursor = await _pool.execute("PRAGMA table_info(task_queue)")
    existing = {row[1] for row in await cursor.fetchall()}

    if "result_summary" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN result_summary TEXT")
    if "result_uri" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN result_uri TEXT")
    if "timeout_sec" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN timeout_sec INTEGER")
    if "progress_pct" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN progress_pct REAL")
    if "progress_json" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN progress_json TEXT")
    if "result_bytes" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN result_bytes INTEGER")
    cursor = await _pool.execute("PRAGMA table_info(artifacts)")
    artifact_existing = {row[1] for row in await cursor.fetchall()}
    if "updated_at" not in artifact_existing:
        await _pool.execute("ALTER TABLE artifacts ADD COLUMN updated_at TEXT")
        await _pool.execute("UPDATE artifacts SET updated_at = COALESCE(finished_at, created_at, datetime('now')) WHERE updated_at IS NULL")
    await _pool.execute(
        "CREATE INDEX IF NOT EXISTS idx_artifacts_user_updated "
        "ON artifacts (user_id, updated_at DESC, id DESC)"
    )
    if "strategy_template_id" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN strategy_template_id INTEGER")
    if "strategy_template_name" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN strategy_template_name TEXT")
    if "strategy_template_updated_at" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN strategy_template_updated_at TEXT")
    if "strategy_template_version" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN strategy_template_version INTEGER")

    # Existing user templates predate version history; preserve them as immutable v1 snapshots.
    await _pool.execute("""
        INSERT INTO backtest_strategy_versions (
            strategy_template_id, version_no, name, description, config, created_at
        )
        SELECT t.id, 1, t.name, t.description, t.config, COALESCE(t.created_at, datetime('now'))
        FROM backtest_strategy_templates t
        WHERE NOT EXISTS (
            SELECT 1 FROM backtest_strategy_versions v
            WHERE v.strategy_template_id = t.id
        )
    """)

    if "source_task_id" not in existing:
        await _pool.execute("ALTER TABLE task_queue ADD COLUMN source_task_id INTEGER")
    await _pool.execute("CREATE INDEX IF NOT EXISTS idx_tq_source_task ON task_queue (source_task_id)")
    # Backfill the independent Artifact registry for completed historical tasks.
    cursor = await _pool.execute("""
        SELECT t.id, t.user_id, t.task_type, t.payload, t.result, t.result_summary,
               t.result_uri, t.result_bytes, t.strategy_template_id,
               t.strategy_template_name, t.strategy_template_updated_at,
               t.strategy_template_version, t.source_task_id, t.assigned_node,
               t.created_at, t.finished_at
        FROM task_queue t
        LEFT JOIN artifacts a ON a.task_id = t.id
        WHERE t.status = "done"
          AND t.task_type IN ("selection", "backtest")
          AND a.id IS NULL
        ORDER BY t.id ASC
    """)
    for row in await cursor.fetchall():
        payload = {}
        try:
            payload = json.loads(row[3]) if row[3] else {}
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if row[2] == "selection":
            source = payload.get("selection_strategy_snapshot") or {}
            title = f"{source.get('name')} 选股成果" if source.get("name") else f"选股成果 #{row[0]}"
            result_json = row[4]
        else:
            title = row[9] or f"回测成果 #{row[0]}"
            result_json = None
        metadata = json.dumps({
            "payload": payload,
            "assigned_node": row[13],
            "strategy_template_id": row[8],
            "strategy_template_name": row[9],
            "strategy_template_updated_at": row[10],
            "strategy_template_version": row[11],
            "source_task_id": row[12],
        }, ensure_ascii=False, separators=(",", ":"))
        backfill_result_bytes = int(row[7] or 0)
        if result_json:
            backfill_result_bytes = len(str(result_json).encode("utf-8"))
        await _pool.execute("""
            INSERT OR IGNORE INTO artifacts (
                user_id, artifact_type, task_id, title, status, metadata,
                summary, result_json, result_uri, result_bytes, created_at, finished_at
            ) VALUES (?, ?, ?, ?, "ready", ?, ?, ?, ?, ?, ?, ?)
        """, row[1], row[2], row[0], title, metadata, row[5], result_json,
        row[6], backfill_result_bytes, row[14], row[15])


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
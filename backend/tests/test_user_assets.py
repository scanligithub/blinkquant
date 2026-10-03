import asyncio
import sqlite3

from backend.scheduler import db
from backend.scheduler.user_assets import router


def test_user_asset_static_export_routes_precede_dynamic_id_routes():
    paths = [route.path for route in router.routes]
    assert paths.index("/internal/user-assets/strategies/export") < paths.index(
        "/internal/user-assets/strategies/{strategy_id}"
    )
    assert paths.index("/internal/user-assets/watchlists/export") < paths.index(
        "/internal/user-assets/watchlists/{watchlist_id}"
    )


def test_user_asset_schema_migrates_legacy_columns(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                formula TEXT NOT NULL
            );
            CREATE TABLE strategy_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id INTEGER NOT NULL,
                version_no INTEGER NOT NULL,
                name TEXT NOT NULL,
                formula TEXT NOT NULL,
                UNIQUE(strategy_id, version_no)
            );
            CREATE TABLE watchlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                UNIQUE(user_id, name)
            );
            CREATE TABLE watchlist_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watchlist_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                UNIQUE(watchlist_id, code)
            );
            """
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            expected = {
                "strategies": {
                    "source_backtest_strategy_id",
                    "source_backtest_strategy_version",
                    "source_backtest_strategy_name",
                    "source_backtest_strategy_trigger",
                    "source_backtest_artifact_id",
                    "source_backtest_artifact_title",
                },
                "strategy_versions": {
                    "source_backtest_strategy_id",
                    "source_backtest_strategy_version",
                    "source_backtest_strategy_name",
                    "source_backtest_strategy_trigger",
                    "source_backtest_artifact_id",
                    "source_backtest_artifact_title",
                },
                "watchlists": {"updated_at"},
                "watchlist_items": {"created_at"},
            }
            for table, columns in expected.items():
                rows = await db.fetch(f"PRAGMA table_info({table})")
                actual = {row["name"] for row in rows}
                assert columns <= actual, (table, columns - actual)
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_legacy_schema_indexes_do_not_block_migration(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE task_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                assigned_node TEXT,
                cluster_job_id TEXT,
                result TEXT,
                result_summary TEXT,
                result_uri TEXT,
                result_bytes INTEGER,
                error TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                queued_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 2,
                preempted_by INTEGER,
                generation INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                artifact_type TEXT NOT NULL,
                task_id INTEGER NOT NULL UNIQUE,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ready',
                metadata TEXT NOT NULL DEFAULT '{}',
                summary TEXT,
                result_json TEXT,
                result_uri TEXT,
                result_bytes INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT
            );
            """
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            for table in ("strategies", "strategy_versions", "watchlists", "watchlist_items"):
                assert await db.fetchval(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    table,
                ) == 1

            task_cols = {row["name"] for row in await db.fetch("PRAGMA table_info(task_queue)")}
            artifact_cols = {row["name"] for row in await db.fetch("PRAGMA table_info(artifacts)")}
            assert "source_task_id" in task_cols
            assert "updated_at" in artifact_cols

            index_names = {row["name"] for row in await db.fetch("PRAGMA index_list(artifacts)")}
            assert "idx_artifacts_user_updated" in index_names
            watchlist_index_names = {row["name"] for row in await db.fetch("PRAGMA index_list(watchlists)")}
            assert "idx_watchlists_one_default" in watchlist_index_names
            strategy_index_names = {row["name"] for row in await db.fetch("PRAGMA index_list(strategies)")}
            assert "idx_strategies_user_updated" in strategy_index_names
        finally:
            await db.close_pool()

    asyncio.run(run())

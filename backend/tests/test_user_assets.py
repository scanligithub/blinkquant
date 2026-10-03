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


def test_legacy_duplicate_default_watchlists_are_deduped_before_unique_index(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE watchlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, name)
            );
            INSERT INTO watchlists (user_id, name, is_default)
            VALUES
                ('legacy-user', '默认A', 1),
                ('legacy-user', '默认B', 1),
                ('legacy-user', '普通列表', 0),
                ('legacy-user-2', '默认C', 1);
            """
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            defaults = await db.fetch(
                "SELECT id, user_id, name FROM watchlists "
                "WHERE is_default=1 ORDER BY id"
            )
            assert [(row["user_id"], row["name"]) for row in defaults] == [
                ("legacy-user", "默认A"),
                ("legacy-user-2", "默认C"),
            ]
            index_names = {
                row["name"] for row in await db.fetch(
                    "PRAGMA index_list(watchlists)"
                )
            }
            assert "idx_watchlists_one_default" in index_names
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_artifact_backfill_handles_legacy_done_tasks(tmp_path, monkeypatch):
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
                result TEXT,
                result_summary TEXT,
                result_uri TEXT,
                result_bytes INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                finished_at TEXT,
                strategy_template_id INTEGER,
                strategy_template_name TEXT,
                strategy_template_updated_at TEXT,
                strategy_template_version INTEGER,
                source_task_id INTEGER
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
            INSERT INTO task_queue (
                user_id, task_type, payload, status, result, result_summary,
                result_uri, result_bytes, strategy_template_id,
                strategy_template_name, strategy_template_updated_at,
                strategy_template_version, source_task_id, assigned_node,
                created_at, finished_at
            ) VALUES (
                'legacy-user', 'selection', '{"selection_strategy_snapshot":{"name":"legacy-sel"}}',
                'done', '{"signals":[{"code":"000001"}]}', '{"count":1}',
                'results/legacy', 123, NULL, NULL, NULL, NULL, NULL, 'node1',
                '2026-01-01T00:00:00', '2026-01-01T00:01:00'
            );
            """
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            artifact = await db.fetchrow(
                "SELECT user_id, artifact_type, task_id, title, result_json, result_uri, result_bytes "
                "FROM artifacts WHERE task_id=1"
            )
            assert artifact is not None
            assert artifact["user_id"] == "legacy-user"
            assert artifact["artifact_type"] == "selection"
            assert artifact["title"] == "legacy-sel 选股成果"
            assert artifact["result_uri"] == "results/legacy"
            assert artifact["result_bytes"] == len('{"signals":[{"code":"000001"}]}'.encode("utf-8"))
        finally:
            await db.close_pool()

    asyncio.run(run())

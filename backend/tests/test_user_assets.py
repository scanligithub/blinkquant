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
                formula TEXT NOT NULL,
                timeframe TEXT NOT NULL DEFAULT 'D',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE strategy_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id INTEGER NOT NULL,
                version_no INTEGER NOT NULL,
                name TEXT NOT NULL,
                formula TEXT NOT NULL,
                timeframe TEXT NOT NULL DEFAULT 'D',
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(strategy_id, version_no)
            );
            CREATE TABLE watchlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
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

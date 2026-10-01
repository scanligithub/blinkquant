import asyncio
import json
import os

import polars as pl

from scheduler import db
from scheduler.result_store import persist_frames


def test_artifact_parquet_survives_sqlite_reopen(tmp_path, monkeypatch):
    async def run():
        db_path = tmp_path / "scheduler.db"
        result_dir = tmp_path / "results"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(db_path))

        await db.close_pool()
        await db.init_pool()
        try:
            await db.execute(
                """INSERT INTO task_queue
                   (user_id, task_type, payload, status, finished_at)
                   VALUES (?, 'backtest', ?, 'done', datetime('now'))""",
                "u-restart",
                json.dumps({"strategy": {"entry": "CLOSE > MA(CLOSE,20)"}}),
            )
            task_id = int(await db.fetchval("SELECT MAX(id) FROM task_queue"))

            equity = pl.DataFrame(
                {"date": ["2026-09-21", "2026-09-22"], "equity": [100.0, 105.0]}
            )
            trades = pl.DataFrame(
                {"code": ["000001"], "side": ["buy"], "price": [10.0]}
            )
            positions = pl.DataFrame(
                {"date": ["2026-09-22"], "code": ["000001"], "shares": [100]}
            )

            summary, uri, nbytes = persist_frames(
                task_id,
                user_id="u-restart",
                result_dir=str(result_dir),
                meta={"metrics": {"total_return": 0.05}},
                equity_curve=equity,
                trades=trades,
                positions_daily=positions,
            )

            physical_bytes = sum(
                p.stat().st_size
                for p in (result_dir / uri).iterdir()
                if p.is_file()
            )
            assert nbytes == physical_bytes
            assert nbytes > 0

            await db.execute(
                """UPDATE task_queue
                   SET result_summary=?, result_uri=?, result_bytes=?
                   WHERE id=?""",
                json.dumps(summary),
                uri,
                nbytes,
                task_id,
            )
            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata, summary,
                    result_uri, result_bytes, finished_at)
                   VALUES (?, 'backtest', ?, ?, '{}', ?, ?, ?, datetime('now'))""",
                "u-restart",
                task_id,
                "Restart persistence",
                json.dumps(summary),
                uri,
                nbytes,
            )
            artifact_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))

            # Simulate Node1 restart: close and reopen the same SQLite file.
            await db.close_pool()
            await db.init_pool()

            artifact = await db.fetchrow(
                "SELECT * FROM artifacts WHERE id = ?", artifact_id
            )
            assert artifact is not None
            assert artifact["result_uri"] == uri
            assert artifact["result_bytes"] == nbytes

            assert await db.fetchval(
                "SELECT id FROM task_queue WHERE id = ?", task_id
            ) == task_id

            loaded = pl.read_parquet(result_dir / uri / "equity_curve.parquet")
            assert loaded.height == 2
            assert loaded["equity"].to_list() == [100.0, 105.0]

            # Task deletion must not remove the persisted artifact files.
            await db.execute("DELETE FROM task_queue WHERE id = ?", task_id)
            assert await db.fetchval(
                "SELECT id FROM artifacts WHERE id = ?", artifact_id
            ) == artifact_id
            assert (result_dir / uri / "equity_curve.parquet").exists()
            assert (result_dir / uri / "trades.parquet").exists()
            assert (result_dir / uri / "positions_daily.parquet").exists()
        finally:
            await db.close_pool()

    asyncio.run(run())

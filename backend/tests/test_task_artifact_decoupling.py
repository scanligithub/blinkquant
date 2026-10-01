import asyncio
import json
import os

from scheduler import db
from scheduler.routes import delete_task


def test_task_delete_preserves_registered_artifact_and_files(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        result_dir = tmp_path / "results"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            uri = "by_user/u1/task_2001"
            await db.execute(
                "INSERT INTO task_queue "
                "(user_id, task_type, payload, status, result_uri, result_bytes, finished_at) "
                "VALUES (?, 'backtest', ?, 'done', ?, ?, datetime('now'))",
                "u1", json.dumps({"strategy": {"entry": {"condition": "CLOSE > MA(CLOSE,20)"}}}),
                uri, 456,
            )
            task_id = int(await db.fetchval("SELECT MAX(id) FROM task_queue"))
            await db.execute(
                "INSERT INTO artifacts "
                "(user_id, artifact_type, task_id, title, metadata, result_uri, result_bytes, finished_at) "
                "VALUES (?, 'backtest', ?, ?, '{}', ?, ?, datetime('now'))",
                "u1", task_id, "回测成果", uri, 456,
            )
            artifact_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))

            os.makedirs(result_dir / uri, exist_ok=True)
            result_file = result_dir / uri / "equity_curve.parquet"
            result_file.write_bytes(b"persistent-result")

            deleted = await delete_task(task_id, user_id="u1", role=None)
            assert deleted["ok"] is True
            assert deleted["artifact_id"] == artifact_id
            assert await db.fetchval("SELECT id FROM task_queue WHERE id = ?", task_id) is None
            assert await db.fetchval("SELECT id FROM artifacts WHERE id = ?", artifact_id) == artifact_id
            assert result_file.read_bytes() == b"persistent-result"
        finally:
            await db.close_pool()

    asyncio.run(run())

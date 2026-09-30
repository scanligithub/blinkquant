import asyncio
import json

from scheduler import db
from scheduler.scheduler import ClusterScheduler


def test_artifact_survives_task_deletion_and_is_idempotent(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            payload = {
                "formula": "CLOSE > MA(CLOSE,20)",
                "timeframe": "D",
                "date": "2026-09-22",
            }
            result = {"codes": ["000001", "600000"], "signal_date": "2026-09-22"}

            await db.execute(
                """INSERT INTO task_queue
                   (user_id, task_type, payload, status, result, finished_at)
                   VALUES (?, 'selection', ?, 'done', ?, datetime('now'))""",
                "u-artifact", json.dumps(payload), json.dumps(result),
            )
            task_id = int(await db.fetchval("SELECT MAX(id) FROM task_queue"))

            scheduler = ClusterScheduler()
            artifact_id = await scheduler._register_task_artifact(task_id)
            assert artifact_id is not None
            assert await scheduler._register_task_artifact(task_id) == artifact_id

            row = await db.fetchrow("SELECT task_id, user_id, result_json, result_bytes FROM artifacts WHERE id = ?", artifact_id)
            assert row["task_id"] == task_id
            assert row["user_id"] == "u-artifact"
            assert json.loads(row["result_json"]) == result
            assert row["result_bytes"] == len(json.dumps(result).encode("utf-8"))

            await db.execute("DELETE FROM task_queue WHERE id = ?", task_id)
            kept = await db.fetchrow("SELECT * FROM artifacts WHERE id = ?", artifact_id)
            assert kept is not None
            assert kept["task_id"] == task_id
            assert json.loads(kept["result_json"]) == result
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_artifact_quota_gc_does_not_touch_new_artifact(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        result_dir = tmp_path / "results"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        monkeypatch.setenv("ARTIFACT_QUOTA_BYTES_PER_USER", "10")
        monkeypatch.setenv("RESULT_DIR", str(result_dir))
        await db.close_pool()
        await db.init_pool()
        try:
            # Import after env is patched so config sees the test quota.
            from scheduler.config import ARTIFACT_QUOTA_BYTES_PER_USER
            assert ARTIFACT_QUOTA_BYTES_PER_USER == 10

            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata, result_bytes, finished_at)
                   VALUES (?, 'backtest', ?, ?, '{}', ?, datetime('now', '-2 days'))""",
                "u-quota", 9001, "old", 20,
            )
            old_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))
            old_uri = "by_user/u-quota/task_9001"
            import os
            os.makedirs(result_dir / old_uri, exist_ok=True)
            (result_dir / old_uri / "meta.json").write_text("{}")

            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata, result_bytes, finished_at)
                   VALUES (?, 'selection', ?, ?, '{}', ?, datetime('now'))""",
                "u-quota", 9002, "new", 5,
            )
            new_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))

            # Method reads the module-level config value; exercise the concrete value used at runtime.
            scheduler = ClusterScheduler()
            import scheduler.scheduler as scheduler_module
            scheduler_module.ARTIFACT_QUOTA_BYTES_PER_USER = 10

            # The method imports config inside the function, so monkeypatch the config module.
            import scheduler.config as config_module
            config_module.ARTIFACT_QUOTA_BYTES_PER_USER = 10
            config_module.RESULT_DIR = str(result_dir)

            evicted = await scheduler._enforce_artifact_quota("u-quota", keep_artifact_id=new_id)
            assert evicted == [old_id]
            assert await db.fetchval("SELECT id FROM artifacts WHERE id = ?", old_id) is None
            assert await db.fetchval("SELECT id FROM artifacts WHERE id = ?", new_id) == new_id
            assert not (result_dir / old_uri).exists()
        finally:
            await db.close_pool()

    asyncio.run(run())

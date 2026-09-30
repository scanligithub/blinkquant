import asyncio
import json
import os

import pytest

from scheduler import db
from scheduler.routes import ArtifactUpdate, delete_artifact, update_artifact


def test_artifact_rename_and_delete_keep_source_task(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        result_dir = tmp_path / "results"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            uri = "by_user/u1/task_1001"
            await db.execute(
                """INSERT INTO task_queue
                   (user_id, task_type, payload, status, finished_at)
                   VALUES (?, 'backtest', ?, 'done', datetime('now'))""",
                "u1", json.dumps({"strategy": {}}),
            )
            task_id = int(await db.fetchval("SELECT MAX(id) FROM task_queue"))
            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata, result_uri, result_bytes, finished_at)
                   VALUES (?, 'backtest', ?, ?, '{}', ?, ?, datetime('now'))""",
                "u1", task_id, "旧名称", uri, 123,
            )
            artifact_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))

            monkeypatch.setenv("RESULT_DIR", str(result_dir))
            import scheduler.config as config_module
            config_module.RESULT_DIR = str(result_dir)
            os.makedirs(result_dir / uri, exist_ok=True)
            (result_dir / uri / "meta.json").write_text("{}")

            updated = await update_artifact(
                artifact_id,
                ArtifactUpdate(title="  新名称  "),
                user_id="u1",
                role=None,
            )
            assert updated["title"] == "新名称"
            assert updated["updated_at"]

            deleted = await delete_artifact(artifact_id, user_id="u1", role=None)
            assert deleted["artifact_id"] == artifact_id
            assert await db.fetchval("SELECT id FROM artifacts WHERE id = ?", artifact_id) is None
            assert await db.fetchval("SELECT id FROM task_queue WHERE id = ?", task_id) == task_id
            assert not (result_dir / uri).exists()
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_artifact_management_is_owner_scoped(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata)
                   VALUES (?, 'selection', ?, ?, '{}')""",
                "u-owner", 2001, "owner", 
            )
            artifact_id = int(await db.fetchval("SELECT MAX(id) FROM artifacts"))
            with pytest.raises(Exception) as exc:
                await update_artifact(
                    artifact_id,
                    ArtifactUpdate(title="forged"),
                    user_id="u-other",
                    role=None,
                )
            assert getattr(exc.value, "status_code", None) == 403
            row = await db.fetchrow("SELECT title FROM artifacts WHERE id = ?", artifact_id)
            assert row["title"] == "owner"
        finally:
            await db.close_pool()

    asyncio.run(run())

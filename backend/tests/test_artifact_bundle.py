import json
import tempfile
import zipfile
from pathlib import Path

from scheduler.result_store import (
    build_artifact_bundle,
    validate_artifact_bundle,
)


def test_artifact_bundle_strips_owner_and_internal_ids():
    with tempfile.TemporaryDirectory() as tmp:
        result_dir = Path(tmp)
        task_dir = result_dir / "by_user" / "alice" / "task_7"
        task_dir.mkdir(parents=True)
        (task_dir / "signal_trace.json").write_text(
            json.dumps({"schema_version": "1.0.0", "traces": []}),
            encoding="utf-8",
        )
        row = {
            "artifact_type": "backtest",
            "title": "测试成果",
            "created_at": "2026-10-03 00:00:00",
            "finished_at": "2026-10-03 00:01:00",
            "metadata": json.dumps({
                "payload": {
                    "source_selection_strategy": {
                        "id": 123,
                        "name": "S1",
                        "version_no": 2,
                    },
                    "user_id": "alice",
                },
                "user_id": "alice",
                "task_id": 7,
            }),
            "summary": json.dumps({"final_equity": 101.0}),
            "result_json": None,
            "result_uri": "by_user/alice/task_7",
        }
        bundle = build_artifact_bundle(row, str(result_dir))
        with zipfile.ZipFile(__import__("io").BytesIO(bundle)) as zf:
            manifest = json.loads(zf.read("manifest.json"))
            assert manifest["format"] == "blinkquant-artifact-v1"
            text = zf.read("manifest.json").decode("utf-8")
            assert '"user_id"' not in text
            assert '"task_id"' not in text
            payload = manifest["metadata"]["payload"]
            assert "id" not in payload["source_selection_strategy"]
            assert payload["source_selection_strategy"]["name"] == "S1"
            assert "parts/signal_trace.json" in manifest["files"]


def test_validate_artifact_bundle_rejects_duplicate_members():
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps({
            "format": "blinkquant-artifact-v1",
            "artifact_type": "selection",
            "title": "dup",
            "files": ["result.json"],
        }))
        zf.writestr("result.json", "{}")
        zf.writestr("result.json", "{}")
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        try:
            validate_artifact_bundle(zf)
        except ValueError as exc:
            assert "duplicate" in str(exc)
        else:
            raise AssertionError("duplicate bundle members must be rejected")


def test_backtest_artifact_export_can_be_imported_roundtrip(tmp_path, monkeypatch):
    async def run():
        import asyncio
        import io
        import sqlite3
        import polars as pl
        from fastapi import UploadFile

        from scheduler import db
        from scheduler.routes import import_artifact_bundle
        from scheduler.result_store import build_artifact_bundle
        import scheduler.config as config

        db_path = tmp_path / "scheduler.db"
        result_dir = tmp_path / "results"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(db_path))
        monkeypatch.setattr(config, "RESULT_DIR", str(result_dir))

        await db.close_pool()
        await db.init_pool()
        try:
            await db.execute(
                """INSERT INTO task_queue
                   (user_id, task_type, payload, status, finished_at)
                   VALUES (?, 'backtest', ?, 'done', datetime('now'))""",
                "source-user",
                json.dumps({"strategy": {"entry": "CLOSE > MA(CLOSE,20)"}}),
            )
            task_id = int(await db.fetchval("SELECT MAX(id) FROM task_queue"))
            result_uri = f"by_user/source-user/task_{task_id}"
            task_dir = result_dir / result_uri
            task_dir.mkdir(parents=True)

            pl.DataFrame({"date": ["2026-10-01"], "equity": [100.0]}).write_parquet(
                task_dir / "equity_curve.parquet"
            )
            pl.DataFrame(
                {"code": ["000001"], "side": ["BUY"], "price": [10.0]}
            ).write_parquet(task_dir / "trades.parquet")
            pl.DataFrame(
                {"date": ["2026-10-01"], "code": ["000001"], "shares": [100]}
            ).write_parquet(task_dir / "positions_daily.parquet")

            await db.execute(
                """INSERT INTO artifacts
                   (user_id, artifact_type, task_id, title, metadata, summary,
                    result_uri, result_bytes, finished_at)
                   VALUES (?, 'backtest', ?, ?, '{}', ?, ?, ?, datetime('now'))""",
                "source-user",
                task_id,
                "Import roundtrip",
                json.dumps({"final_equity": 100.0}),
                result_uri,
                1,
            )
            source_artifact = await db.fetchrow(
                "SELECT * FROM artifacts WHERE task_id = ?", task_id
            )
            bundle = build_artifact_bundle(source_artifact, str(result_dir))

            with zipfile.ZipFile(io.BytesIO(bundle)) as zf:
                manifest = json.loads(zf.read("manifest.json"))
                assert manifest["format"] == "blinkquant-artifact-v1"
                assert manifest["artifact_type"] == "backtest"
                assert set(manifest["files"]) >= {
                    "parts/equity_curve.parquet",
                    "parts/trades.parquet",
                    "parts/positions_daily.parquet",
                }

            uploaded = UploadFile(
                file=io.BytesIO(bundle),
                filename="blinkquant_artifact_roundtrip.zip",
            )
            imported = await import_artifact_bundle(
                uploaded, user_id="import-user", role=None
            )
            assert imported["ok"] is True
            assert imported["artifact_type"] == "backtest"

            imported_artifact = await db.fetchrow(
                "SELECT task_id, user_id, artifact_type, result_uri, result_bytes "
                "FROM artifacts WHERE id = ?",
                imported["artifact_id"],
            )
            assert imported_artifact["user_id"] == "import-user"
            assert imported_artifact["artifact_type"] == "backtest"
            assert imported_artifact["result_uri"].startswith("by_user/import-user/task_")
            assert imported_artifact["result_bytes"] > 0

            imported_dir = result_dir / imported_artifact["result_uri"]
            assert (imported_dir / "equity_curve.parquet").exists()
            assert (imported_dir / "trades.parquet").exists()
            assert (imported_dir / "positions_daily.parquet").exists()
        finally:
            await db.close_pool()

    import asyncio
    asyncio.run(run())

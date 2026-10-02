import json
import tempfile
import zipfile
from pathlib import Path

from backend.scheduler.result_store import (
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

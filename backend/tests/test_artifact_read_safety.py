import asyncio
import json
import math

from scheduler import routes


def test_artifact_read_paths_are_json_safe_and_schema_tolerant(monkeypatch):
    async def run():
        row = {
            "id": 7,
            "user_id": "u1",
            "artifact_type": "backtest",
            "task_id": 17,
            "title": "回测成果 #17",
            "status": "ready",
            "metadata": json.dumps(
                {"payload": {"ratio": float("nan")}},
                allow_nan=True,
                ensure_ascii=False,
            ),
            "summary": '{"sharpe": NaN, "nested": {"beta": Infinity}}',
            "result_json": '{"value": -Infinity}',
            "result_uri": "by_user/u1/task_17",
            "result_bytes": 1234,
            "created_at": "2026-09-30 08:00:00",
            "finished_at": "2026-09-30 08:01:00",
            # Deliberately omit updated_at to model an older scheduler DB.
        }

        async def fake_fetchrow(query, *args):
            if "COUNT(*) AS total" in query:
                return {
                    "total": 1,
                    "used_bytes": 1234,
                    "selection_total": 0,
                    "backtest_total": 1,
                }
            assert "FROM artifacts WHERE id = ?" in query
            return dict(row)

        async def fake_fetch(query, *args):
            assert "FROM artifacts WHERE" in query
            listed = dict(row)
            listed["task_exists"] = 1
            return [listed]

        async def fake_fetchval(query, *args):
            return 1

        monkeypatch.setattr(routes, "fetchrow", fake_fetchrow)
        monkeypatch.setattr(routes, "fetch", fake_fetch)
        monkeypatch.setattr(routes, "fetchval", fake_fetchval)

        detail = await routes.get_artifact(7, user_id="u1", role=None)
        assert detail["updated_at"] == row["finished_at"]
        assert detail["summary"]["sharpe"] is None
        assert detail["summary"]["nested"]["beta"] is None
        assert detail["result"]["value"] is None
        assert detail["payload"]["ratio"] is None

        listing = await routes.list_artifacts(user_id="u1")
        item = listing["artifacts"][0]
        assert item["updated_at"] == row["finished_at"]
        assert item["summary"]["sharpe"] is None
        assert item["result"]["value"] is None
        assert item["payload"]["ratio"] is None
        assert math.isfinite(float(item["result_bytes"]))

    asyncio.run(run())

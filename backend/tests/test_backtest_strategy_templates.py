import asyncio

from scheduler import db


def test_backtest_strategy_template_persists_in_scheduler_sqlite(tmp_path, monkeypatch):
    async def _run():
        db_path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(db_path))
        await db.close_pool()
        await db.init_pool()
        try:
            config = {
                "strategy": {
                    "universe": {"type": "all_a"},
                    "entry": {"condition": "CLOSE > MA(CLOSE,20)", "trigger": "condition", "timeframe": "D"},
                    "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
                    "rebalance": {"frequency": "weekly"},
                    "mode": "target_portfolio",
                },
                "fee_policy": {"mode": "historical"},
                "benchmark": {"enabled": True, "type": "index", "index_id": "000300"},
                "min_listing_days": 60,
                "exclude_st": True,
            }
            await db.execute(
                "INSERT INTO backtest_strategy_templates (user_id,name,description,config) VALUES ($1,$2,$3,$4)",
                "user-a", "MA20 Weekly", "P4.1", db.json_dumps(config),
            )
            row = await db.fetchrow(
                "SELECT user_id,name,config FROM backtest_strategy_templates WHERE user_id=$1 AND name=$2",
                "user-a", "MA20 Weekly",
            )
            assert row["user_id"] == "user-a"
            assert row["name"] == "MA20 Weekly"
            assert db.json_loads(row["config"]) == config

            other = await db.fetch(
                "SELECT id FROM backtest_strategy_templates WHERE user_id=$1",
                "user-b",
            )
            assert other == []
        finally:
            await db.close_pool()

    asyncio.run(_run())


def test_template_auth_requires_session_cookie():
    from fastapi import HTTPException
    from api.routes import _template_user_id

    async def _run():
        try:
            await _template_user_id(None)
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("missing session cookie must be rejected")

    asyncio.run(_run())


def test_template_auth_uses_vercel_session_user(monkeypatch):
    from api import routes

    class FakeResponse:
        status = 200

        def __init__(self, payload):
            import json
            self._body = json.dumps(payload).encode("utf-8")

        def read(self):
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        assert request.get_header("Cookie") == "valid-session"
        assert timeout == 5
        return FakeResponse({
            "user": {
                "id": "user-from-vercel",
                "email": "user@example.com",
                "role": "user",
            }
        })

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr(
        routes,
        "TEMPLATE_AUTH_SESSION_URL",
        "https://blinkquant.de5.net/api/auth/session",
    )

    async def _run():
        user_id = await routes._template_user_id("valid-session")
        assert user_id == "user-from-vercel"

    asyncio.run(_run())

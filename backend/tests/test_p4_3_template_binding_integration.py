import asyncio
import json

from fastapi import HTTPException
from scheduler import db


def test_template_binding_match_and_user_isolation(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import _resolve_strategy_template, TaskCreate

            cfg = {
                "strategy": {"universe": {"type": "all_a"},
                             "entry": {"condition": "CLOSE > MA(CLOSE,20)",
                                       "trigger": "condition", "timeframe": "D"},
                             "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
                             "rebalance": {"frequency": "daily"},
                             "mode": "target_portfolio"},
                "fee_policy": {"mode": "historical"},
                "benchmark": {"enabled": True, "type": "index", "index_id": "000300"},
                "min_listing_days": 0,
                "exclude_st": False,
            }
            await db.execute(
                "INSERT INTO backtest_strategy_templates (user_id,name,config) VALUES ($1,$2,$3)",
                "u1", "tpl", json.dumps(cfg),
            )

            task = TaskCreate(user_id="u1", task_type="backtest",
                              payload={**cfg}, strategy_template_id=1)
            bound = await _resolve_strategy_template(task)
            assert bound[0] == 1
            assert bound[1] == "tpl"

            other = TaskCreate(user_id="u2", task_type="backtest",
                               payload={**cfg}, strategy_template_id=1)
            try:
                await _resolve_strategy_template(other)
            except HTTPException as exc:
                assert exc.status_code == 400
            else:
                raise AssertionError("cross-user template must be rejected")
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_template_binding_drops_stale_config(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import _resolve_strategy_template, TaskCreate

            cfg = {
                "strategy": {"universe": {"type": "all_a"},
                             "entry": {"condition": "CLOSE > MA(CLOSE,20)",
                                       "trigger": "condition", "timeframe": "D"},
                             "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
                             "rebalance": {"frequency": "daily"},
                             "mode": "target_portfolio"},
                "fee_policy": {"mode": "historical"},
                "benchmark": {"enabled": True, "type": "index", "index_id": "000300"},
                "min_listing_days": 0,
                "exclude_st": False,
            }
            await db.execute(
                "INSERT INTO backtest_strategy_templates (user_id,name,config) VALUES ($1,$2,$3)",
                "u1", "tpl2", json.dumps(cfg),
            )
            stale = dict(cfg)
            stale["min_listing_days"] = 30
            task = TaskCreate(user_id="u1", task_type="backtest",
                              payload=stale, strategy_template_id=1)
            bound = await _resolve_strategy_template(task)
            assert bound == (None, None, None)
        finally:
            await db.close_pool()

    asyncio.run(run())

import pytest

from scheduler import db


@pytest.mark.asyncio
async def test_backtest_strategy_template_persists_in_scheduler_sqlite(tmp_path, monkeypatch):
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

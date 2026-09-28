"""P4.3 strategy-template binding helper tests."""
import asyncio
import json

from scheduler.routes import _canonical_json, _task_template_config


def test_task_template_config_extracts_canonical_fields():
    payload = {
        "formula": "legacy",
        "strategy": {
            "universe": {"type": "all_a"},
            "entry": {"condition": "CLOSE > MA(CLOSE,20)", "trigger": "condition", "timeframe": "D"},
            "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
            "rebalance": {"frequency": "daily"},
            "mode": "target_portfolio",
        },
        "fee_policy": {"mode": "historical"},
        "benchmark": {"enabled": True, "type": "index", "index_id": "000300"},
        "min_listing_days": 0,
        "exclude_st": False,
    }
    config = _task_template_config(payload)
    assert config is not None
    assert config["strategy"]["entry"]["condition"] == "CLOSE > MA(CLOSE,20)"
    assert config["fee_policy"]["mode"] == "historical"


def test_task_template_config_requires_canonical_strategy():
    assert _task_template_config({"formula": "CLOSE > MA(CLOSE,20)"}) is None


def test_canonical_json_is_order_independent():
    left = {"strategy": {"a": 1, "b": 2}, "exclude_st": False}
    right = {"exclude_st": False, "strategy": {"b": 2, "a": 1}}
    assert _canonical_json(left) == _canonical_json(right)

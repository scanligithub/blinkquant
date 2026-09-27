import datetime as dt

import pytest

from api.routes import BacktestRequest, _normalize_strategy_request


def strategy_payload():
    return {
        "universe": {"type": "index", "index_id": "000300"},
        "entry": {
            "condition": "CLOSE > MA(CLOSE,20)",
            "trigger": "condition",
            "timeframe": "D",
        },
        "sizing": {
            "method": "top_n_equal_weight",
            "max_positions": 100,
        },
        "rebalance": {"frequency": "weekly"},
        "mode": "target_portfolio",
    }


def base_request(**overrides):
    data = {
        "start_date": dt.date(2024, 1, 2),
        "end_signal_date": dt.date(2024, 12, 30),
        "initial_cash": 1_000_000,
        "strategy": strategy_payload(),
    }
    data.update(overrides)
    return BacktestRequest(**data)


def test_strategy_is_canonical_source_for_top_n_and_rebalance():
    strategy = _normalize_strategy_request(
        base_request(top_n=100, rebalance_freq="weekly")
    )

    assert strategy.sizing.max_positions == 100
    assert strategy.rebalance.frequency == "weekly"


def test_conflicting_legacy_top_n_is_rejected():
    with pytest.raises(ValueError, match="top_n conflicts"):
        _normalize_strategy_request(base_request(top_n=20))


def test_conflicting_legacy_rebalance_is_rejected():
    with pytest.raises(ValueError, match="rebalance_freq conflicts"):
        _normalize_strategy_request(base_request(rebalance_freq="daily"))


def test_legacy_flat_request_keeps_backward_compatible_defaults():
    req = BacktestRequest(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 1, 31),
        initial_cash=100_000,
        formula="CLOSE > MA(CLOSE,20)",
    )
    strategy = _normalize_strategy_request(req)

    assert strategy.sizing.max_positions == 20
    assert strategy.rebalance.frequency == "daily"

import pytest

from core.backtest_types import MVP_EXECUTION_CONFIG
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


def test_target_portfolio_strategy_defaults():
    strategy = StrategyDefinition(
        universe=UniverseDefinition(),
        entry=SignalDefinition("CLOSE > MA(CLOSE,20)"),
    )

    assert strategy.mode == "target_portfolio"
    assert strategy.universe.type == "all_a"
    assert strategy.sizing.method == "equal_weight"
    assert strategy.rebalance.frequency == "daily"
    assert strategy.execution == MVP_EXECUTION_CONFIG


def test_index_universe_requires_index_id():
    with pytest.raises(ValueError, match="index_id"):
        UniverseDefinition(type="index")

    with pytest.raises(ValueError, match="index_id"):
        UniverseDefinition(type="all_a", index_id="000300")


def test_event_driven_requires_exit():
    entry = SignalDefinition(
        "MA(CLOSE,5) > MA(CLOSE,60)",
        trigger="cross_above",
    )
    with pytest.raises(ValueError, match="exit"):
        StrategyDefinition(
            universe=UniverseDefinition(type="index", index_id="000300"),
            entry=entry,
            mode="event_driven",
        )


def test_signal_trigger_validation():
    with pytest.raises(ValueError):
        SignalDefinition("CLOSE > 1", trigger="unknown")

    assert SignalDefinition(
        "MA(CLOSE,5) > MA(CLOSE,60)",
        trigger="cross_above",
    ).to_dict()["trigger"] == "cross_above"


def test_top_n_requires_positive_limit():
    with pytest.raises(ValueError, match="max_positions"):
        PositionSizingDefinition(method="top_n_equal_weight")

    with pytest.raises(ValueError, match="max_positions"):
        PositionSizingDefinition(method="top_n_equal_weight", max_positions=0)


def test_strategy_round_trip_from_dict():
    source = {
        "name": "CSI300 MA Cross",
        "mode": "event_driven",
        "universe": {"type": "index", "index_id": "000300"},
        "entry": {
            "condition": "MA(CLOSE,5) > MA(CLOSE,60)",
            "trigger": "cross_above",
        },
        "exit": {
            "condition": "MA(CLOSE,5) < MA(CLOSE,20)",
            "trigger": "cross_below",
        },
        "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
        "rebalance": {"frequency": "weekly"},
    }

    strategy = StrategyDefinition.from_dict(source)
    assert strategy.to_dict()["universe"] == {"type": "index", "index_id": "000300"}
    assert strategy.entry.trigger == "cross_above"
    assert strategy.exit.trigger == "cross_below"
    assert strategy.sizing.max_positions == 20
    assert strategy.rebalance.frequency == "weekly"

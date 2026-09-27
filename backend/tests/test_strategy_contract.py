"""P2-B strategy contract tests.

These tests freeze the canonical strategy semantics used by the generic
backtest configuration layer. They intentionally test the public contract,
not implementation details of BacktestEngine/SelectionEngine.
"""

import pytest

from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


def _strategy(**overrides):
    data = {
        "universe": {"type": "all_a"},
        "entry": {
            "condition": "MA(CLOSE,5) > MA(CLOSE,60)",
            "trigger": "cross_above",
            "timeframe": "D",
        },
        "exit": {
            "condition": "MA(CLOSE,5) < MA(CLOSE,20)",
            "trigger": "cross_below",
            "timeframe": "D",
        },
        "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
        "rebalance": {"frequency": "weekly"},
        "mode": "event_driven",
        "name": "ma-cross-event",
    }
    data.update(overrides)
    return StrategyDefinition.from_dict(data)


class TestSignalContract:
    @pytest.mark.parametrize("trigger", ["condition", "cross_above", "cross_below"])
    @pytest.mark.parametrize("timeframe", ["D", "W", "M"])
    def test_supported_signal_contract(self, trigger, timeframe):
        signal = SignalDefinition(
            condition="CLOSE > MA(CLOSE,20)",
            trigger=trigger,
            timeframe=timeframe,
        )
        assert signal.to_dict() == {
            "condition": "CLOSE > MA(CLOSE,20)",
            "trigger": trigger,
            "timeframe": timeframe,
        }

    def test_empty_condition_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            SignalDefinition(condition="")

    def test_unknown_trigger_rejected(self):
        with pytest.raises(ValueError, match="unsupported signal trigger"):
            SignalDefinition(condition="CLOSE > 10", trigger="cross")  # type: ignore[arg-type]

    def test_unknown_timeframe_rejected(self):
        with pytest.raises(ValueError, match="unsupported signal timeframe"):
            SignalDefinition(condition="CLOSE > 10", timeframe="H")


class TestStrategyModes:
    def test_event_driven_requires_exit(self):
        with pytest.raises(ValueError, match="requires an exit signal"):
            StrategyDefinition(
                universe=UniverseDefinition(),
                entry=SignalDefinition(condition="CLOSE > 10"),
                mode="event_driven",
            )

    def test_target_portfolio_exit_is_optional(self):
        strategy = StrategyDefinition(
            universe=UniverseDefinition(),
            entry=SignalDefinition(condition="CLOSE > 10"),
            mode="target_portfolio",
        )
        assert strategy.exit is None

    def test_target_portfolio_can_carry_explicit_exit_signal(self):
        strategy = _strategy(mode="target_portfolio")
        assert strategy.exit is not None
        assert strategy.mode == "target_portfolio"

    def test_invalid_mode_rejected(self):
        with pytest.raises(ValueError, match="unsupported strategy mode"):
            StrategyDefinition(
                universe=UniverseDefinition(),
                entry=SignalDefinition(condition="CLOSE > 10"),
                mode="unknown",  # type: ignore[arg-type]
            )


class TestSizingAndRebalanceContract:
    def test_top_n_requires_positive_limit(self):
        with pytest.raises(ValueError, match="requires max_positions"):
            PositionSizingDefinition(
                method="top_n_equal_weight",
                max_positions=0,
            )

    def test_equal_weight_does_not_require_limit(self):
        sizing = PositionSizingDefinition(method="equal_weight")
        assert sizing.max_positions is None

    def test_supported_rebalance_frequencies(self):
        assert RebalanceDefinition(frequency="daily").frequency == "daily"
        assert RebalanceDefinition(frequency="weekly").frequency == "weekly"

    def test_unknown_rebalance_rejected(self):
        with pytest.raises(ValueError, match="unsupported rebalance frequency"):
            RebalanceDefinition(frequency="monthly")  # type: ignore[arg-type]


class TestCanonicalRoundTrip:
    def test_event_driven_entry_exit_strategy_round_trips(self):
        strategy = _strategy()
        restored = StrategyDefinition.from_dict(strategy.to_dict())

        assert restored == strategy
        assert restored.universe.type == "all_a"
        assert restored.entry.trigger == "cross_above"
        assert restored.exit is not None
        assert restored.exit.trigger == "cross_below"
        assert restored.sizing.method == "top_n_equal_weight"
        assert restored.sizing.max_positions == 20
        assert restored.rebalance.frequency == "weekly"
        assert restored.mode == "event_driven"

    def test_index_strategy_requires_index_id(self):
        with pytest.raises(ValueError, match="requires index_id"):
            UniverseDefinition(type="index")

    def test_all_a_rejects_index_id(self):
        with pytest.raises(ValueError, match="must not specify index_id"):
            UniverseDefinition(type="all_a", index_id="000300")

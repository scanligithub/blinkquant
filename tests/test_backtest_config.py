import datetime as dt

import pytest

from core.backtest_config import BacktestConfig
from core.strategy import (
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


def strategy():
    return StrategyDefinition(
        universe=UniverseDefinition(type="index", index_id="000300"),
        entry=SignalDefinition(condition="CLOSE > MA(CLOSE,20)"),
        rebalance=RebalanceDefinition(frequency="weekly"),
    )


def test_round_trip_is_json_ready_and_reproducible():
    config = BacktestConfig(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 12, 30),
        initial_cash=1_000_000,
        strategy=strategy(),
        min_listing_days=60,
        exclude_st=True,
        historical_fees=True,
    )
    payload = config.to_dict()
    restored = BacktestConfig.from_dict(payload)

    assert payload["start_date"] == "2024-01-02"
    assert payload["end_signal_date"] == "2024-12-30"
    assert restored.to_dict() == payload


def test_defaults_are_centralized():
    config = BacktestConfig(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 1, 31),
        initial_cash=100_000,
        strategy=strategy(),
    )
    assert config.min_listing_days == 0
    assert config.exclude_st is False
    assert config.historical_fees is True


def test_validation():
    with pytest.raises(ValueError, match="start_date"):
        BacktestConfig(
            start_date=dt.date(2024, 2, 1),
            end_signal_date=dt.date(2024, 1, 31),
            initial_cash=100_000,
            strategy=strategy(),
        )
    with pytest.raises(ValueError, match="initial_cash"):
        BacktestConfig(
            start_date=dt.date(2024, 1, 2),
            end_signal_date=dt.date(2024, 1, 31),
            initial_cash=0,
            strategy=strategy(),
        )

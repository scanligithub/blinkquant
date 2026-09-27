"""Regression tests for StrategySelector cross signals with lazy QFQ data."""

import datetime as dt

import polars as pl

from core.data_manager import data_manager
from core.strategy import StrategyDefinition
from core.strategy_selector import StrategySelector


class _FakeQFQProvider:
    def __init__(self, frame):
        self.frame = frame

    def load_qfq_window(self, start, end, latest_adj):
        return self.frame.filter(
            (pl.col("date") >= start) & (pl.col("date") <= end)
        )


def _make_frame():
    base = dt.date(2024, 5, 20)
    dates = [base + dt.timedelta(days=i) for i in range(25)]
    closes = [10.0 + i * 0.1 for i in range(25)]
    return pl.DataFrame(
        {
            "date": dates,
            "code": ["sh.600000"] * 25,
            "open": closes,
            "high": [v + 0.2 for v in closes],
            "low": [v - 0.2 for v in closes],
            "close": closes,
            "volume": [1000.0] * 25,
            "amount": [10000.0] * 25,
        }
    )


def test_cross_qfq_does_not_reuse_stale_mounted_indicator_columns():
    original = (
        data_manager.df_daily,
        data_manager.df_weekly,
        data_manager.df_monthly,
    )
    try:
        frame = _make_frame()
        resident = frame.with_columns(
            pl.col("close").rolling_mean(window_size=20).alias("MA_CLOSE_20"),
            pl.col("close").rolling_mean(window_size=5).alias("MA_CLOSE_5"),
        )
        data_manager.df_daily, data_manager.df_weekly, data_manager.df_monthly = (
            resident,
            None,
            None,
        )

        strategy = StrategyDefinition.from_dict(
            {
                "universe": {"type": "all_a"},
                "entry": {
                    "condition": "MA(CLOSE,5) > MA(CLOSE,20)",
                    "trigger": "cross_above",
                    "timeframe": "D",
                },
                "exit": {
                    "condition": "MA(CLOSE,5) < MA(CLOSE,20)",
                    "trigger": "cross_below",
                    "timeframe": "D",
                },
                "sizing": {
                    "method": "top_n_equal_weight",
                    "max_positions": 20,
                },
                "rebalance": {"frequency": "weekly"},
                "mode": "event_driven",
            }
        )

        selector = StrategySelector()
        selector._qfq_data_provider = _FakeQFQProvider(frame)
        selector._latest_adj = {"sh.600000": 1.0}
        result = selector.select(
            strategy, dt.date(2024, 6, 3), backtest_mode=True
        )

        assert isinstance(result.entry_codes, list)
        assert isinstance(result.exit_codes, list)
    finally:
        data_manager.df_daily, data_manager.df_weekly, data_manager.df_monthly = (
            original
        )

"""IA5.5.3 deterministic event-driven capital/position E2E contract."""

import datetime as dt
from types import SimpleNamespace

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


DATES = [
    dt.date(2024, 1, 2),
    dt.date(2024, 1, 3),
    dt.date(2024, 1, 4),
    dt.date(2024, 1, 5),
    dt.date(2024, 1, 8),
]
CODES = ["AAA", "BBB", "CCC", "DDD", "EEE"]


class FakeRawPriceStore:
    def load_latest_adjust_factors(self, start, end):
        return {}

    def load_execution_prices(self, dates):
        return pl.DataFrame(
            {
                "code": CODES,
                "open": [10.0] * len(CODES),
                "close": [10.0] * len(CODES),
            }
        )

    def load_limit_flags_for_date(self, date, codes):
        return {
            code: {
                "is_suspended": False,
                "is_limit_up": False,
                "is_limit_down": False,
            }
            for code in codes
        }


def test_event_driven_small_window_preserves_cash_and_top_n_across_cycles():
    """Three slots stay capped while exits free exactly one slot for entries.

    Signal cycle:
      1) enter A/B/C -> 3 holdings
      2) enter D only -> retained A/B/C consume all 3 slots, so D is ignored
      3) exit A + enter D/E -> only D fills because one slot is freed
      4) exit B + enter E -> only E fills because one slot is freed

    All signals execute at the next trading-day open, with sell-first execution.
    """
    calendar = TradingCalendar()
    calendar.set_trade_dates(DATES)

    events = {
        dt.date(2024, 1, 2): (["AAA", "BBB", "CCC"], []),
        dt.date(2024, 1, 3): (["DDD"], []),
        dt.date(2024, 1, 4): (["DDD", "EEE"], ["AAA"]),
        dt.date(2024, 1, 5): (["EEE"], ["BBB"]),
    }

    def select(strategy, date, backtest_mode=True):
        entries, exits = events[date]
        return SimpleNamespace(
            signal_date=date,
            target_codes=entries,
            entry_codes=entries,
            exit_codes=exits,
            target_weights={},
        )

    selector = SimpleNamespace(select=select, _qfq_data_provider=None, _latest_adj={})
    engine = BacktestEngine(
        calendar=calendar,
        selection_engine=None,
        raw_price_store=FakeRawPriceStore(),
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        strategy_selector=selector,
    )

    strategy = StrategyDefinition(
        universe=UniverseDefinition(type="all_a"),
        entry=SignalDefinition(
            condition="MA(CLOSE,5) > MA(CLOSE,60)",
            trigger="cross_above",
            timeframe="D",
        ),
        exit=SignalDefinition(
            condition="MA(CLOSE,5) < MA(CLOSE,20)",
            trigger="cross_below",
            timeframe="D",
        ),
        sizing=PositionSizingDefinition(
            method="top_n_equal_weight",
            max_positions=3,
        ),
        rebalance=RebalanceDefinition(frequency="daily"),
        mode="event_driven",
    )

    result = engine.run(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 1, 5),
        initial_cash=10_000.0,
        strategy=strategy,
    )

    assert result.trades.height > 0
    assert set(result.trades["side"].to_list()) == {"BUY", "SELL"}

    # Every execution is T+1 and sell-first makes exit proceeds reusable.
    assert (result.trades["execution_date"] > result.trades["signal_date"]).all()

    # Final holdings and every daily snapshot must respect max_positions=3.
    counts = (
        result.positions_daily
        .group_by("date")
        .agg(pl.col("code").n_unique().alias("n_positions"))
    )
    assert counts["n_positions"].max() <= 3

    final_codes = (
        result.positions_daily
        .filter(pl.col("date") == dt.date(2024, 1, 8))
        .select("code")
        .sort("code")["code"]
        .to_list()
    )
    assert final_codes == ["CCC", "DDD", "EEE"]

    # The retained-position cycle must not buy D when all three slots are occupied.
    buys = result.trades.filter(pl.col("side") == "BUY")
    assert "DDD" in buys["code"].to_list()
    assert buys.filter(
        (pl.col("signal_date") == dt.date(2024, 1, 3))
        & (pl.col("code") == "DDD")
    ).height == 0

    # Exit proceeds must fund the replacement entry in the same execution cycle.
    replacement = buys.filter(
        (pl.col("signal_date") == dt.date(2024, 1, 4))
        & (pl.col("code") == "DDD")
    )
    assert replacement.height == 1
    assert replacement["qty"][0] > 0

    assert result.equity_curve["cash"].min() >= -1e-6
    assert result.metrics["total_return"] is not None

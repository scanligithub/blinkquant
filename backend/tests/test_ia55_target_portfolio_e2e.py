"""IA5.5.4 deterministic target-portfolio capital/rebalance contract."""

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
    dt.date(2024, 2, 1),
    dt.date(2024, 2, 2),
    dt.date(2024, 2, 5),
    dt.date(2024, 2, 6),
]
CODES = ["AAA", "BBB", "CCC"]


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


def test_target_portfolio_rebalances_sell_first_and_reuses_proceeds():
    """A 90% A -> 90% B rebalance must reuse sale proceeds without overspending.

    The signal on Feb 1 executes on Feb 2. The existing A position is sold
    before the B purchase, so the sale proceeds become available to the
    subsequent buy in the same execution cycle. The 10% cash buffer also
    leaves room for execution fees.
    """
    calendar = TradingCalendar()
    calendar.set_trade_dates(DATES)

    events = {
        dt.date(2024, 2, 1): {"AAA": 0.9},
        dt.date(2024, 2, 2): {"BBB": 0.9},
    }

    def select(strategy, date, backtest_mode=True):
        return SimpleNamespace(
            signal_date=date,
            target_codes=list(events[date]),
            entry_codes=[],
            exit_codes=[],
            target_weights=events[date],
        )

    selector = SimpleNamespace(
        select=select,
        _qfq_data_provider=None,
        _latest_adj={},
    )
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
        sizing=PositionSizingDefinition(method="equal_weight", max_positions=None),
        rebalance=RebalanceDefinition(frequency="daily"),
        mode="target_portfolio",
    )

    result = engine.run(
        start_date=dt.date(2024, 2, 1),
        end_signal_date=dt.date(2024, 2, 2),
        initial_cash=1_000_000.0,
        strategy=strategy,
    )

    assert result.trades.height > 0
    assert set(result.trades["side"].to_list()) == {"BUY", "SELL"}
    assert (result.trades["execution_date"] > result.trades["signal_date"]).all()

    # Feb 1 signal executes on Feb 2 (BUY AAA); Feb 2 signal executes on
    # Feb 5 (SELL AAA first, then BUY BBB with the sale proceeds).
    feb2 = result.trades.filter(pl.col("execution_date") == dt.date(2024, 2, 2))
    assert feb2["side"].to_list() == ["BUY"]
    assert feb2["code"].to_list() == ["AAA"]

    feb5 = result.trades.filter(pl.col("execution_date") == dt.date(2024, 2, 5))
    assert feb5["side"].to_list() == ["SELL", "BUY"]
    assert feb5["code"].to_list() == ["AAA", "BBB"]

    # The next cycle switches to BBB; no capital is created by planning.
    final = (
        result.positions_daily
        .filter(pl.col("date") == dt.date(2024, 2, 6))
        .select("code")
        .sort("code")["code"]
        .to_list()
    )
    assert final == ["BBB"]

    assert result.equity_curve["cash"].min() >= -1e-6
    assert result.metrics["total_return"] is not None

    # The executed Feb 2 target order must not survive as a pending checkpoint event.
    assert engine.export_state()["pending"] is None


def test_target_portfolio_planner_is_deterministic_under_cash_limited_buys():
    """Multiple target buys must be planned in stable code order."""
    engine = BacktestEngine(
        calendar=None,
        selection_engine=None,
        raw_price_store=None,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
    )

    # The planner sees 1,000 cash and two 50% targets at equal prices.
    # Sorting is part of the capital-allocation contract.
    engine.portfolio.cash = 1_000.0
    intents = engine._generate_intents(
        target_weights={"CCC": 0.5, "BBB": 0.5},
        execution_prices={
            "BBB": {"open": 10.0, "close": 10.0},
            "CCC": {"open": 10.0, "close": 10.0},
        },
    )

    assert [(i.code, i.side, i.target_qty) for i in intents] == [
        ("BBB", "BUY", 50),
        ("CCC", "BUY", 50),
    ]
    assert sum(i.target_qty * 10.0 for i in intents) <= 1_000.0

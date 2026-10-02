"""IA5.5.6: cross-year target-portfolio checkpoint/resume contract."""

import datetime as dt
import tempfile
from types import SimpleNamespace

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.checkpoint import load_checkpoint
from core.raw_price_store import RawPriceStore
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


DATES = [
    dt.date(2025, 12, 29),
    dt.date(2025, 12, 30),
    dt.date(2025, 12, 31),
    dt.date(2026, 1, 2),
    dt.date(2026, 1, 5),
]

CODES = ["AAA", "BBB"]


def _write_year_files(root: str) -> None:
    """Write separate 2025/2026 parquet shards so the test crosses a real year boundary."""
    rows = []
    closes = {
        DATES[0]: {"AAA": 10.0, "BBB": 10.0},
        DATES[1]: {"AAA": 10.0, "BBB": 10.0},
        DATES[2]: {"AAA": 10.0, "BBB": 10.0},
        DATES[3]: {"AAA": 10.0, "BBB": 10.0},
        DATES[4]: {"AAA": 10.0, "BBB": 10.0},
    }
    for date in DATES:
        for code in CODES:
            close = closes[date][code]
            rows.append(
                {
                    "date": date,
                    "code": code,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1_000_000.0,
                    "amount": 10_000_000.0,
                    "adjustFactor": 1.0,
                }
            )

    frame = pl.DataFrame(rows).sort(["code", "date"])
    for year in (2025, 2026):
        frame.filter(pl.col("date").dt.year() == year).write_parquet(
            f"{root}/stock_kline_{year}.parquet"
        )


def _build_engine(root: str, calendar: TradingCalendar, events: dict):
    def select(strategy, date, backtest_mode=True):
        weights = events.get(date, {})
        return SimpleNamespace(
            signal_date=date,
            target_codes=list(weights),
            entry_codes=[],
            exit_codes=[],
            target_weights=weights,
        )

    selector = SimpleNamespace(
        select=select,
        _qfq_data_provider=None,
        _latest_adj={},
    )

    return BacktestEngine(
        calendar=calendar,
        selection_engine=None,
        raw_price_store=RawPriceStore(root),
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        strategy_selector=selector,
    )


def _strategy():
    return StrategyDefinition(
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


def test_target_portfolio_checkpoint_resume_across_year_boundary():
    """Checkpoint on 2025-12-31 must restore and execute its 2026-01-02 T+1 order identically."""
    events = {
        DATES[0]: {"AAA": 0.9},
        DATES[1]: {"AAA": 0.9},
        DATES[2]: {"BBB": 0.9},
    }

    calendar = TradingCalendar()
    calendar.set_trade_dates(DATES)
    strategy = _strategy()

    with tempfile.TemporaryDirectory() as root:
        _write_year_files(root)

        store = RawPriceStore(root)
        boundary_prices = store.load_execution_prices([DATES[2], DATES[3]])
        assert boundary_prices.height == 4
        assert set(boundary_prices["date"].to_list()) == {DATES[2], DATES[3]}

        # C1: continuous run through the 2025-12-31 signal and 2026-01-02 execution.
        continuous_engine = _build_engine(root, calendar, events)
        continuous = continuous_engine.run(
            start_date=DATES[0],
            end_signal_date=DATES[2],
            initial_cash=1_000_000.0,
            strategy=strategy,
        )
        c1_day4 = continuous.trades.filter(
            (pl.col("execution_date") == DATES[3])
            & (pl.col("signal_date") == DATES[2])
        ).sort(["code", "side"])
        assert c1_day4.height == 2
        assert c1_day4["code"].to_list() == ["AAA", "BBB"]
        assert c1_day4["side"].to_list() == ["SELL", "BUY"]

        # Save immediately after the 2025-12-31 CHECKPOINT, while the Jan-2 order is pending.
        source_engine = _build_engine(root, calendar, events)
        with tempfile.TemporaryDirectory() as cp_root:
            checkpoint_dir = f"{cp_root}/cross-year"
            seen = {"saved": False}

            def save_after_year_end(progress):
                if (
                    progress["stage"] == "running"
                    and progress["current_date"] == DATES[2].isoformat()
                    and not seen["saved"]
                ):
                    source_engine.save_checkpoint(
                        checkpoint_dir,
                        DATES[2],
                        "IA5.5.6 cross-year 2025-12-31 pending 2026-01-02 T+1",
                    )
                    seen["saved"] = True

            source_engine.run(
                start_date=DATES[0],
                end_signal_date=DATES[2],
                initial_cash=1_000_000.0,
                strategy=strategy,
                on_progress=save_after_year_end,
            )
            assert seen["saved"]

            checkpoint = load_checkpoint(checkpoint_dir)
            assert checkpoint.current_date == DATES[2].isoformat()
            assert checkpoint.pending_signal_date == DATES[2].isoformat()
            assert checkpoint.pending_execution_date == DATES[3].isoformat()
            assert checkpoint.pending_intents
            assert any(
                item["code"] == "BBB" and item["side"] == "BUY"
                for item in checkpoint.pending_intents
            )

            # C2: restore in a fresh engine on 2026-01-02 and execute the restored T+1 order.
            resume_engine = _build_engine(root, calendar, events)
            resumed = resume_engine.run(
                start_date=DATES[3],
                end_signal_date=DATES[3],
                initial_cash=1_000_000.0,
                initial_state=checkpoint,
                strategy=strategy,
            )

        c2_day4 = resumed.trades.filter(
            (pl.col("execution_date") == DATES[3])
            & (pl.col("signal_date") == DATES[2])
        ).sort(["code", "side"])
        assert c2_day4.to_dicts() == c1_day4.to_dicts()

        c1_pos = continuous.positions_daily.filter(
            pl.col("date") == DATES[3]
        ).sort("code")
        c2_pos = resumed.positions_daily.filter(
            pl.col("date") == DATES[3]
        ).sort("code")
        assert c2_pos.to_dicts() == c1_pos.to_dicts()

        c1_eq = continuous.equity_curve.filter(pl.col("date") == DATES[3]).drop("signal_date")
        c2_eq = resumed.equity_curve.filter(pl.col("date") == DATES[3]).drop("signal_date")
        assert c2_eq.to_dicts() == c1_eq.to_dicts()
        assert c2_eq["cash"].min() >= -1e-6

        # The restored Dec-31 order must be consumed after the Jan-2 execution.
        state = resume_engine.export_state()["pending"]
        assert state is None or state["signal_date"] != DATES[2].isoformat()

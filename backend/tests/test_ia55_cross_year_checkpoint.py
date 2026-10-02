"""IA5.5.6: cross-year target-portfolio checkpoint/resume contract."""

import datetime as dt
import tempfile
from types import SimpleNamespace

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.checkpoint import load_checkpoint
from core.portfolio import Position
from core.raw_price_store import RawPriceStore
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


DATES = [
    dt.date(2025, 12, 31),
    dt.date(2026, 1, 2),
    dt.date(2026, 1, 5),
]

CODES = ["AAA", "BBB"]


def _write_year_files(root: str) -> None:
    """Write separate year shards for the 2025-12-31 -> 2026-01-02 boundary."""
    frame_2025 = pl.DataFrame(
        {
            "date": [DATES[0], DATES[0]],
            "code": CODES,
            "open": [10.0, 10.0],
            "high": [10.0, 10.0],
            "low": [10.0, 10.0],
            "close": [10.0, 10.0],
            "volume": [1_000_000.0, 1_000_000.0],
            "amount": [10_000_000.0, 10_000_000.0],
            "adjustFactor": [1.0, 1.0],
        }
    )
    frame_2026 = pl.DataFrame(
        {
            "date": [DATES[1], DATES[1], DATES[2], DATES[2]],
            "code": CODES + CODES,
            "open": [10.0, 10.0, 10.0, 10.0],
            "high": [10.0, 10.0, 10.0, 10.0],
            "low": [10.0, 10.0, 10.0, 10.0],
            "close": [10.0, 10.0, 10.0, 10.0],
            "volume": [1_000_000.0] * 4,
            "amount": [10_000_000.0] * 4,
            "adjustFactor": [1.0] * 4,
        }
    )
    frame_2025.write_parquet(f"{root}/stock_kline_2025.parquet")
    frame_2026.write_parquet(f"{root}/stock_kline_2026.parquet")


def _build_engine(root: str, calendar: TradingCalendar):
    def select(strategy, date, backtest_mode=True):
        weights = {"BBB": 0.9} if date in (DATES[0], DATES[1]) else {}
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


def _initial_positions():
    return {
        "AAA": Position(
            code="AAA",
            total_qty=90_000,
            available_qty=90_000,
            frozen_qty=0,
            avg_cost=10.0,
            market_value=900_000.0,
        )
    }


def test_target_portfolio_checkpoint_resume_across_year_boundary():
    """A Dec-31 checkpoint must preserve and resume the Jan-2 T+1 order exactly."""
    calendar = TradingCalendar()
    calendar.set_trade_dates(DATES)
    strategy = _strategy()

    with tempfile.TemporaryDirectory() as root:
        _write_year_files(root)

        store = RawPriceStore(root)
        boundary = store.load_execution_prices(DATES)
        assert boundary.height == 6
        assert set(boundary["date"].to_list()) == set(DATES)

        # C1: continuous execution. The 2025-12-31 target switch executes on 2026-01-02.
        continuous_engine = _build_engine(root, calendar)
        continuous = continuous_engine.run(
            start_date=DATES[0],
            end_signal_date=DATES[0],
            initial_cash=1_000_000.0,
            initial_positions=_initial_positions(),
            strategy=strategy,
        )
        c1_day2 = continuous.trades.filter(
            (pl.col("execution_date") == DATES[1])
            & (pl.col("signal_date") == DATES[0])
        ).sort(["code", "side"])
        assert c1_day2.height == 2
        assert c1_day2["code"].to_list() == ["AAA", "BBB"]
        assert c1_day2["side"].to_list() == ["SELL", "BUY"]

        # Save immediately after the Dec-31 CHECKPOINT while the Jan-2 order is pending.
        source_engine = _build_engine(root, calendar)
        with tempfile.TemporaryDirectory() as cp_root:
            checkpoint_dir = f"{cp_root}/cross-year"
            seen = {"saved": False}

            def save_after_dec31(progress):
                if (
                    progress["stage"] == "running"
                    and progress["current_date"] == DATES[0].isoformat()
                    and not seen["saved"]
                ):
                    source_engine.save_checkpoint(
                        checkpoint_dir,
                        DATES[0],
                        "IA5.5.6 cross-year Dec-31 pending Jan-2 T+1",
                    )
                    seen["saved"] = True

            source_engine.run(
                start_date=DATES[0],
                end_signal_date=DATES[0],
                initial_cash=1_000_000.0,
                initial_positions=_initial_positions(),
                strategy=strategy,
                on_progress=save_after_dec31,
            )
            assert seen["saved"]

            checkpoint = load_checkpoint(checkpoint_dir)
            assert checkpoint.current_date == DATES[0].isoformat()
            assert checkpoint.pending_signal_date == DATES[0].isoformat()
            assert checkpoint.pending_execution_date == DATES[1].isoformat()
            assert checkpoint.pending_intents
            assert any(
                item["code"] == "BBB" and item["side"] == "BUY"
                for item in checkpoint.pending_intents
            )

            # C2: fresh process resumes on the cross-year T+1 execution date.
            # The extra 2026-01-05 calendar day keeps resumed signal scheduling
            # within a valid T+1 window.
            resume_engine = _build_engine(root, calendar)
            resumed = resume_engine.run(
                start_date=DATES[1],
                end_signal_date=DATES[1],
                initial_cash=1_000_000.0,
                initial_state=checkpoint,
                strategy=strategy,
            )

        c2_day2 = resumed.trades.filter(
            (pl.col("execution_date") == DATES[1])
            & (pl.col("signal_date") == DATES[0])
        ).sort(["code", "side"])
        assert c2_day2.to_dicts() == c1_day2.to_dicts()

        c1_pos = continuous.positions_daily.filter(pl.col("date") == DATES[1]).sort("code")
        c2_pos = resumed.positions_daily.filter(pl.col("date") == DATES[1]).sort("code")
        assert c2_pos.to_dicts() == c1_pos.to_dicts()

        c1_eq = continuous.equity_curve.filter(pl.col("date") == DATES[1]).drop("signal_date")
        c2_eq = resumed.equity_curve.filter(pl.col("date") == DATES[1]).drop("signal_date")
        assert c2_eq.to_dicts() == c1_eq.to_dicts()
        assert c2_eq["cash"].min() >= -1e-6

        # The restored Dec-31 order is consumed after the Jan-2 execution.
        state = resume_engine.export_state()["pending"]
        assert state is None or state["signal_date"] != DATES[0].isoformat()

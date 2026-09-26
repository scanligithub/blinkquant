"""P4.2-2 production-data end-to-end validation.

This test is intentionally opt-in because it downloads real stockA data from HF.
Run with:
    RUN_P4_2_PRODUCTION=1 HF_TOKEN=... pytest tests/test_p4_2_production.py -v
"""

import datetime as dt
import os

import polars as pl
import pytest

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig
from core.data_manager import data_manager
from core.engine import SelectionEngine
from core.raw_price_store import RawPriceStore
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)
from core.universe_resolver import UniverseResolver


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_P4_2_PRODUCTION") != "1",
    reason="production-data test is opt-in",
)


HISTORY_START = dt.date(2022, 1, 1)
BACKTEST_START = dt.date(2024, 1, 2)
BACKTEST_END = dt.date(2024, 12, 27)


def _load_real_data():
    """Load the minimum real stockA window needed by MA60 weekly signals."""
    store = RawPriceStore(
        hf_repo_id=os.getenv("STOCKA_HF_REPO", "scanli/stocka-data"),
        hf_token=os.getenv("HF_TOKEN"),
    )

    # Use the same latest-adjust-factor mechanism as BacktestEngine, so the
    # SelectionEngine and BacktestEngine see the same QFQ price convention.
    latest_adj = store.load_latest_adjust_factors(
        start=BACKTEST_START,
        end=BACKTEST_END,
    )
    assert latest_adj, "stockA latest adjust factors are empty"

    daily = store.load_qfq_window(
        HISTORY_START,
        BACKTEST_END,
        latest_adj,
    )
    assert not daily.is_empty(), "stockA K-line window is empty"

    # SelectionEngine reads the shared DataManager.  Populate it from the
    # real RawPriceStore, then use the production resampling implementation.
    original_daily = data_manager.df_daily
    original_weekly = data_manager.df_weekly
    original_monthly = data_manager.df_monthly
    data_manager.df_daily = daily.sort(["code", "date"])
    data_manager.df_weekly = None
    data_manager.df_monthly = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()

    return store, original_daily, original_weekly, original_monthly


def test_p4_2_2_real_stocka_csi300_weekly_cross_top20_e2e():
    """Real HF data: PIT CSI300 -> real SelectionEngine -> BacktestEngine."""
    store, old_daily, old_weekly, old_monthly = _load_real_data()

    try:
        resolver = UniverseResolver.from_huggingface(
            repo_id=os.getenv("STOCKA_HF_REPO", "scanli/stocka-data"),
            filename="index_membership/index_membership_history.parquet",
            token=os.getenv("HF_TOKEN"),
        )

        # Calendar is derived from the same real K-line window used by the
        # backtest, so execution dates are real market dates.
        trade_dates = (
            data_manager.df_daily
            .filter(
                (pl.col("date") >= BACKTEST_START)
                & (pl.col("date") <= BACKTEST_END)
            )
            .select("date")
            .unique()
            .sort("date")["date"]
            .to_list()
        )
        assert len(trade_dates) > 200

        strategy = StrategyDefinition(
            name="P4.2-2 CSI300 MA Cross Top20",
            universe=UniverseDefinition(type="index", index_id="000300"),
            entry=SignalDefinition(
                condition="MA(CLOSE,5) > MA(CLOSE,60)",
                trigger="cross_above",
                timeframe="W",
            ),
            exit=SignalDefinition(
                condition="MA(CLOSE,5) < MA(CLOSE,20)",
                trigger="cross_below",
                timeframe="W",
            ),
            sizing=PositionSizingDefinition(
                method="top_n_equal_weight",
                max_positions=20,
            ),
            rebalance=RebalanceDefinition(frequency="weekly"),
            mode="event_driven",
        )

        selector_engine = SelectionEngine()
        engine = BacktestEngine(
            calendar=TradingCalendar(),
            selection_engine=selector_engine,
            raw_price_store=store,
            fee_config=FeeConfig(),
            universe_resolver=resolver,
        )
        engine.calendar.set_trade_dates(trade_dates)

        result = engine.run(
            strategy=strategy,
            start_date=BACKTEST_START,
            end_signal_date=BACKTEST_END,
            initial_cash=1_000_000,
        )

        # Basic end-to-end invariants.
        assert not result.equity_curve.is_empty()
        assert result.equity_curve["date"].min() == BACKTEST_START
        assert result.equity_curve["date"].max() > BACKTEST_END
        assert result.equity_curve["equity"].is_not_null().all()

        # At least one real signal/trade must have been produced; otherwise
        # the test could pass while the strategy pipeline is disconnected.
        assert not result.trades.is_empty(), "real strategy produced no trades"

        # Every execution must be strictly after its signal date (next-open).
        bad_t1 = result.trades.filter(
            pl.col("execution_date") <= pl.col("signal_date")
        )
        assert bad_t1.is_empty(), bad_t1

        # BUY signals must be members of the CSI300 universe on the
        # signal date.  SELL signals may legitimately be generated from the
        # previous-period universe when a constituent leaves the index.
        checked_buys = 0
        checked_sells = 0
        for row in result.trades.iter_rows(named=True):
            members = set(resolver.members("000300", row["signal_date"]))
            if row["side"] == "BUY":
                assert row["code"] in members, (
                    f"{row['code']} not in CSI300 PIT universe at "
                    f"{row['signal_date']}"
                )
                checked_buys += 1
            else:
                prior_dates = (
                    data_manager.df_daily
                    .filter(pl.col("date") < row["signal_date"])
                    .select(pl.col("date").max())
                    .item()
                )
                previous_members = set(
                    resolver.members("000300", prior_dates)
                )
                assert row["code"] in members or row["code"] in previous_members, (
                    f"{row['code']} not in current/previous CSI300 PIT universe "
                    f"for exit at {row['signal_date']}"
                )
                checked_sells += 1
        assert checked_buys > 0
        assert checked_buys + checked_sells == result.trades.height

        # Top-N contract: no execution batch may contain >20 distinct codes.
        for signal_date, batch in result.trades.group_by("signal_date"):
            assert batch["code"].n_unique() <= 20

    finally:
        data_manager.df_daily = old_daily
        data_manager.df_weekly = old_weekly
        data_manager.df_monthly = old_monthly
        data_manager._asof_frame_cache.clear()

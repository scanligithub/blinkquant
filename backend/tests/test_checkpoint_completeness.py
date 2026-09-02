"""Checkpoint Completeness Regression Tests.

Prevent regressions like the _restore_from_checkpoint portfolio bug (dbc3e6b).
Verify every BacktestCheckpoint field round-trips through save -> load -> restore.
"""

import datetime
import tempfile
import polars as pl
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.checkpoint import BacktestCheckpoint, save_checkpoint, load_checkpoint
from core.data_manager import data_manager
from core.engine import selection_engine


def _weekdays(n, start=datetime.date(2025, 12, 1)):
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += datetime.timedelta(days=1)
    return days


def _fixture(days):
    rows = []
    for i, d in enumerate(days):
        c = 10.0 + (i % 5)
        rows.append((d, "sh.AAA", c - 0.1, c, c + 0.2, c - 0.2))
    return pl.DataFrame({
        "date": [r[0] for r in rows], "code": [r[1] for r in rows],
        "open": [r[2] for r in rows], "close": [r[3] for r in rows],
        "high": [r[4] for r in rows], "low": [r[5] for r in rows],
        "volume": [1e6] * len(rows), "amount": [1e7] * len(rows),
    }).sort(["code", "date"])


def _install(df):
    data_manager.df_daily = df
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()


def _build_engine(tmp, all_days):
    cal = TradingCalendar()
    cal.set_trade_dates(all_days)
    return BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=RawPriceStore(tmp),
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )


class TestCheckpointCompleteness:
    """Verify every BacktestCheckpoint field survives save -> load -> restore."""

    def setup_method(self):
        self.days = _weekdays(10)
        self.df = _fixture(self.days)
        _install(self.df)

    def teardown_method(self):
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()

    def _run_and_checkpoint(self):
        """Helper: run engine for 5 days, save and load checkpoint."""
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")
            engine = _build_engine(tmp, self.days)
            engine.run(
                formula="CLOSE > 10", start_date=self.days[0],
                end_signal_date=self.days[4], initial_cash=1_000_000,
                rebalance_freq="daily",
            )

            with tempfile.TemporaryDirectory() as cp_dir:
                engine.save_checkpoint(cp_dir, self.days[4])
                cp = load_checkpoint(cp_dir)
                yield engine, cp, tmp

    def test_portfolio_roundtrip(self):
        """Portfolio (cash + positions) must survive save -> load -> restore."""
        for engine, cp, tmp in self._run_and_checkpoint():
            # Checkpoint must have captured portfolio state
            assert cp.cash != 1_000_000 or len(cp.positions) > 0, \
                "Checkpoint should reflect trading activity"

            # Restore into a fresh engine
            fresh_engine = _build_engine(tmp, self.days)
            next_sig = engine.calendar.next_trade_day(self.days[4])
            fresh_engine.run(
                formula="CLOSE > 10", start_date=next_sig,
                end_signal_date=self.days[-2], initial_cash=1_000_000,
                initial_state=cp, rebalance_freq="daily",
            )

            # Fresh engine without checkpoint should differ from restored one
            baseline_engine = _build_engine(tmp, self.days)
            baseline_engine.run(
                formula="CLOSE > 10", start_date=next_sig,
                end_signal_date=self.days[-2], initial_cash=1_000_000,
                rebalance_freq="daily",
            )

            restored_cash = fresh_engine.portfolio.cash
            baseline_cash = baseline_engine.portfolio.cash
            assert restored_cash != baseline_cash, \
                f"Restored portfolio ({restored_cash}) must differ from baseline ({baseline_cash})"

    def test_last_close_roundtrip(self):
        """Checkpoint must carry last_close prices when positions exist."""
        for engine, cp, tmp in self._run_and_checkpoint():
            if cp.positions:
                assert len(cp.last_close) > 0, \
                    "Checkpoint with positions must have last_close entries"
                for entry in cp.last_close:
                    assert "code" in entry and "close" in entry, \
                        f"last_close entry missing fields: {entry}"
            else:
                # No positions is acceptable, just verify field exists
                assert cp.last_close is not None

    def test_thru_thaw_cursor_roundtrip(self):
        """Checkpoint must capture thru_thaw cursor after run."""
        for engine, cp, tmp in self._run_and_checkpoint():
            assert cp.thru_thaw is not None, \
                "thru_thaw cursor must be set after engine run"

    def test_diagnostics_roundtrip(self):
        """Checkpoint must carry diagnostics with intents_total key."""
        for engine, cp, tmp in self._run_and_checkpoint():
            assert isinstance(cp.diagnostics, dict), \
                f"diagnostics should be dict, got {type(cp.diagnostics)}"
            assert "intents_total" in cp.diagnostics, \
                f"diagnostics missing 'intents_total' key: {cp.diagnostics.keys()}"

    def test_all_fields_non_null_after_roundtrip(self):
        """All critical fields must be non-null/non-empty after save -> load."""
        for engine, cp, tmp in self._run_and_checkpoint():
            assert cp.schema_version, "schema_version must be non-empty"
            assert cp.current_date, "current_date must be non-empty"
            assert cp.phase, "phase must be non-empty"
            assert cp.cash is not None, "cash must not be None"
            assert cp.positions is not None, "positions must not be None"
            assert cp.last_close is not None, "last_close must not be None"
            assert cp.diagnostics is not None, "diagnostics must not be None"
            assert cp.thru_thaw is not None, "thru_thaw must not be None"

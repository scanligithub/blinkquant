"""Tests for Event State Machine — phase handler dispatch."""

import datetime
import polars as pl
from unittest.mock import MagicMock

from core.backtest_engine import BacktestEngine, TradingCalendar, EventPhase
from core.portfolio import Portfolio, Position
from core.corporate_actions import CorporateAction, ActionType, CorporateActionStore
from core.backtest_types import (
    FeeConfig, ExecutionConfig, FeeSchedule, MVP_EXECUTION_CONFIG,
)


def _make_engine(trade_dates=None):
    """Create a minimal BacktestEngine with mock dependencies."""
    cal = TradingCalendar()
    cal.set_trade_dates(trade_dates or [datetime.date(2024, 1, 2)])
    engine = BacktestEngine(
        calendar=cal,
        selection_engine=MagicMock(),
        raw_price_store=MagicMock(),
        fee_config=FeeConfig(),
    )
    engine.portfolio = Portfolio(initial_cash=100000.0)
    return engine


class TestEventPhaseEnum:
    """EventPhase enum has all required phases."""

    def test_all_phases_defined(self):
        phases = [p.value for p in EventPhase]
        assert "pre_open" in phases
        assert "post_close_signal" in phases
        assert "post_execution" in phases
        assert "market_close" in phases
        assert "valuation" in phases
        assert "checkpoint" in phases

    def test_six_phases(self):
        assert len(EventPhase) == 6


class TestPhaseHandlerPreOpen:
    """PRE_OPEN phase handler: thaw + corporate actions."""

    def test_thaw_unfreezes_qty(self):
        engine = _make_engine()
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=900,
            frozen_qty=100, avg_cost=20.0, market_value=20000.0)
        engine._thru_thaw = None

        engine._phase_pre_open(
            datetime.date(2024, 1, 2), None, None, {"rej_counters": {}}
        )
        assert engine.portfolio.positions["000001"].frozen_qty == 0
        assert engine.portfolio.positions["000001"].available_qty == 1000

    def test_corporate_actions_applied(self):
        engine = _make_engine([datetime.date(2024, 7, 1)])
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)
        engine._thru_thaw = None

        store = CorporateActionStore([CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5)])
        engine._phase_pre_open(
            datetime.date(2024, 7, 1), store, None, {"rej_counters": {}}
        )
        assert engine.portfolio.cash == 100500.0

    def test_thaw_deduplication(self):
        engine = _make_engine([datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)])
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=900,
            frozen_qty=100, avg_cost=20.0, market_value=20000.0)
        engine._thru_thaw = datetime.date(2024, 1, 2)

        # Same day — should NOT thaw again
        engine._phase_pre_open(
            datetime.date(2024, 1, 2), None, None, {"rej_counters": {}}
        )
        assert engine.portfolio.positions["000001"].frozen_qty == 100

        # Next day — should thaw
        engine._phase_pre_open(
            datetime.date(2024, 1, 3), None, None, {"rej_counters": {}}
        )
        assert engine.portfolio.positions["000001"].frozen_qty == 0


class TestPhaseHandlerValuation:
    """VALUATION phase handler: equity calc + ledger check."""

    def test_equity_calculation(self):
        engine = _make_engine()
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=0.0)
        engine._last_close = {}

        day_px = {"000001": {"close": 22.0}}
        equity, pos_val = engine._phase_valuation(
            datetime.date(2024, 1, 2), day_px, {"carried_events": 0}
        )
        assert abs(equity - 122000.0) < 1e-6
        assert abs(pos_val - 22000.0) < 1e-6

    def test_ledger_invariant_holds_normally(self):
        engine = _make_engine()
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=0.0)
        engine._last_close = {}

        day_px = {"000001": {"close": 22.0}}
        equity, pos_val = engine._phase_valuation(
            datetime.date(2024, 1, 2), day_px, {"carried_events": 0}
        )
        # Equity == cash + positions_value by construction
        assert abs(equity - (engine.portfolio.cash + pos_val)) < 1e-6

    def test_carry_forward_used_when_no_price(self):
        engine = _make_engine()
        engine.portfolio.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=0.0)
        engine._last_close = {"000001": 25.0}

        day_px = {}  # no price today
        diag = {"carried_events": 0}
        equity, pos_val = engine._phase_valuation(
            datetime.date(2024, 1, 2), day_px, diag
        )
        assert diag["carried_events"] == 1
        assert abs(equity - 125000.0) < 1e-6

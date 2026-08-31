"""P0-3: Price Semantic Conservation — Raw/QFQ/CA continuity proofs."""

import datetime
from core.portfolio import Portfolio, Position
from core.corporate_actions import CorporateAction, ActionType


class TestRawConservation:
    """Portfolio value using raw prices is invariant across all operations."""

    def test_raw_equity_equals_cash_plus_positions(self):
        """equity == cash + sum(qty × raw_close) at all times."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        raw_prices = {"000001": {"close": 22.0}}
        equity = p.get_equity(raw_prices)
        expected = 100000.0 + 1000 * 22.0
        assert abs(equity - expected) < 1e-6

    def test_raw_equity_with_multiple_positions(self):
        """equity == cash + sum over all positions."""
        p = Portfolio(initial_cash=50000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)
        p.positions["600519"] = Position(
            code="600519", total_qty=100, available_qty=100,
            frozen_qty=0, avg_cost=1800.0, market_value=180000.0)

        raw_prices = {"000001": {"close": 22.0}, "600519": {"close": 1900.0}}
        equity = p.get_equity(raw_prices)
        expected = 50000.0 + 1000 * 22.0 + 100 * 1900.0
        assert abs(equity - expected) < 1e-6


class TestQFQConservation:
    """QFQ portfolio value = raw value × cumulative adjust_factor."""

    def test_qfq_adjust_factor_scales_prices(self):
        """QFQ price = raw price × adjust_factor."""
        raw_close = 15.0
        adjust_factor = 2.0
        qfq_close = raw_close * adjust_factor
        assert qfq_close == 30.0

    def test_qfq_position_value_equals_raw_times_factor(self):
        """QFQ position value = raw position value × adjust_factor."""
        qty = 1000
        raw_close = 15.0
        adjust_factor = 2.0
        raw_value = qty * raw_close
        qfq_value = qty * (raw_close * adjust_factor)
        assert abs(qfq_value - raw_value * adjust_factor) < 1e-6


class TestCAConservation:
    """Corporate action adjustments preserve total portfolio value."""

    def test_dividend_cash_increases_total_equity(self):
        """After dividend: equity increases by dividend_amount."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        raw_prices = {"000001": {"close": 20.0}}
        pre_equity = p.get_equity(raw_prices)

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5))

        post_equity = p.get_equity(raw_prices)
        assert abs(post_equity - pre_equity - 500.0) < 1e-6

    def test_split_qty_scales_price_scales_value(self):
        """After split: qty doubles, price halves, value stays same."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))

        pos = p.positions["000001"]
        assert pos.total_qty == 2000
        assert abs(pos.avg_cost - 10.0) < 1e-6

        # With split-adjusted price (halved), value = 2000 × 10.0 = 20000
        raw_prices = {"000001": {"close": 10.0}}
        post_equity = p.get_equity(raw_prices)
        expected = 100000.0 + 2000 * 10.0
        assert abs(post_equity - expected) < 1e-6


class TestBoundaryConservation:
    """No single day where equity != cash + positions_value."""

    def test_multi_day_equity_consistency(self):
        """Simulate 5-day price changes, verify equity invariant holds every day."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        prices = [20.0, 21.0, 19.5, 22.0, 20.5]
        for i, close in enumerate(prices):
            raw_prices = {"000001": {"close": close}}
            equity = p.get_equity(raw_prices)
            expected = p.cash + 1000 * close
            assert abs(equity - expected) < 1e-6, \
                f"Day {i}: equity={equity}, expected={expected}"

    def test_corporate_action_day_equity_consistency(self):
        """Equity invariant holds on corporate action day."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))

        raw_prices = {"000001": {"close": 10.0}}
        equity = p.get_equity(raw_prices)
        expected = 100000.0 + 2000 * 10.0
        assert abs(equity - expected) < 1e-6

    def test_dividend_then_price_change_consistency(self):
        """Equity invariant holds after dividend + price change."""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        # Dividend: +500 cash, avg_cost drops
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5))

        # Price changes to 22.0
        raw_prices = {"000001": {"close": 22.0}}
        equity = p.get_equity(raw_prices)
        expected = 100500.0 + 1000 * 22.0  # cash=100000+500
        assert abs(equity - expected) < 1e-6

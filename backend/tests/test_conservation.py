"""P1-5: Raw/QFQ/CorporateAction conservation — 验证不会双重计价。"""
import datetime
from core.portfolio import Portfolio, Position
from core.corporate_actions import CorporateAction, ActionType


class TestDividendNoDoubleCount:
    """分红不应被 qfq 和 corporate action 双重计入。"""

    def test_dividend_cash_only_once(self):
        """分红只增加一次 cash（via corporate action），qfq 不额外加 cash。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        initial_cash = p.cash
        dividend_per_share = 0.5
        expected_dividend = 1000 * dividend_per_share

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=dividend_per_share))

        # cash 应只增加一次（dividend_per_share × total_qty）
        assert abs(p.cash - (initial_cash + expected_dividend)) < 1e-6
        # avg_cost 应降低
        assert p.positions["000001"].avg_cost < 20.0

    def test_dividend_avg_cost_reduction(self):
        """分红后 avg_cost 降低量 = dividend_per_share。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5))

        assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6


class TestSplitConservation:
    """拆股后 market value 应守恒。"""

    def test_split_market_value_conservation(self):
        """拆股前 1000×20=20000 → 拆股后 2000×10=20000。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=20000.0)

        pre_split_value = p.positions["000001"].total_qty * p.positions["000001"].avg_cost

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))

        pos = p.positions["000001"]
        post_split_value = pos.total_qty * pos.avg_cost

        # 拆股后 total_qty × avg_cost 应守恒
        assert abs(pre_split_value - post_split_value) < 1e-6

    def test_split_total_qty_scales(self):
        """拆股后 total_qty 按 ratio 缩放。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=0.0)

        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))

        assert p.positions["000001"].total_qty == 2000
        assert p.positions["000001"].avg_cost == 10.0

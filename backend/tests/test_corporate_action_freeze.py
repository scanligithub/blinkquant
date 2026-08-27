"""P0-2: 公司行为调整后 frozen_qty 不变量验证。"""
import datetime
import pytest
from core.portfolio import Portfolio, Position
from core.corporate_actions import (
    CorporateAction, ActionType, adjust_qty_for_split,
)


class TestSplitFreezeInvariant:
    """拆股/送股后 available + frozen == total 不变量。"""

    def test_split_with_frozen_qty(self):
        """1:2 拆股，frozen 按比例缩放。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=900,
            frozen_qty=100, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))
        pos = p.positions["000001"]
        assert pos.total_qty == 2000
        assert pos.available_qty == 1800
        assert pos.frozen_qty == 200
        assert pos.available_qty + pos.frozen_qty == pos.total_qty

    def test_bonus_with_frozen_qty(self):
        """10送3（1.3x），frozen 按比例缩放并向下取整。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=950,
            frozen_qty=50, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 9, 15), code="000001",
            action_type=ActionType.BONUS_SHARES, split_ratio=1.3))
        pos = p.positions["000001"]
        assert pos.total_qty == 1300
        assert pos.available_qty + pos.frozen_qty == pos.total_qty

    def test_split_no_frozen(self):
        """无 frozen 时，available = total。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=1000,
            frozen_qty=0, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))
        pos = p.positions["000001"]
        assert pos.total_qty == 2000
        assert pos.available_qty == 2000
        assert pos.frozen_qty == 0
        assert pos.available_qty + pos.frozen_qty == pos.total_qty

    def test_split_all_frozen(self):
        """全部冻结时，available = 0。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=0,
            frozen_qty=1000, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT, split_ratio=2.0))
        pos = p.positions["000001"]
        assert pos.total_qty == 2000
        assert pos.available_qty == 0
        assert pos.frozen_qty == 2000
        assert pos.available_qty + pos.frozen_qty == pos.total_qty

    def test_odd_lot_frozen_rounding(self):
        """10送3，frozen 100 → int(100*1.3)=130, available=1300-130=1170。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=900,
            frozen_qty=100, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 9, 15), code="000001",
            action_type=ActionType.BONUS_SHARES, split_ratio=1.3))
        pos = p.positions["000001"]
        assert pos.frozen_qty == int(100 * 1.3)
        assert pos.available_qty == pos.total_qty - pos.frozen_qty

    def test_adjust_qty_for_split_returns_4_tuple(self):
        """adjust_qty_for_split 返回 4-tuple。"""
        total, cost, avail, frozen = adjust_qty_for_split(
            total_qty=1000, avg_cost=20.0, split_ratio=2.0, frozen_qty=100)
        assert total == 2000
        assert frozen == 200
        assert avail == 1800
        assert avail + frozen == total

    def test_adjust_qty_for_split_no_frozen(self):
        """adjust_qty_for_split 无 frozen 时返回 0 frozen。"""
        total, cost, avail, frozen = adjust_qty_for_split(
            total_qty=1000, avg_cost=20.0, split_ratio=2.0)
        assert total == 2000
        assert frozen == 0
        assert avail == 2000

    def test_dividend_does_not_affect_frozen(self):
        """现金分红不影响 frozen_qty。"""
        p = Portfolio(initial_cash=100000.0)
        p.positions["000001"] = Position(
            code="000001", total_qty=1000, available_qty=900,
            frozen_qty=100, avg_cost=20.0, market_value=0.0)
        p.apply_corporate_action(CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5))
        pos = p.positions["000001"]
        assert pos.frozen_qty == 100
        assert pos.available_qty == 900
        assert pos.total_qty == 1000
        assert pos.available_qty + pos.frozen_qty == pos.total_qty
        assert abs(p.cash - (100000.0 + 500.0)) < 1e-6

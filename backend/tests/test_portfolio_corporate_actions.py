"""Portfolio 公司行为调整测试。"""
import datetime
from core.portfolio import Portfolio, Position
from core.corporate_actions import (
    CorporateAction, ActionType, CorporateActionStore,
)


def _make_portfolio():
    """构造测试用 Portfolio：code=000001, 1000 股, 成本 20.0, 现金 50000。"""
    p = Portfolio(initial_cash=50000.0)
    pos = Position(code="000001", total_qty=1000, available_qty=1000,
                   frozen_qty=0, avg_cost=20.0, market_value=0.0)
    p.positions["000001"] = pos
    return p


def test_cash_dividend():
    """现金分红：持股不变，现金增加，avg_cost 下降。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6  # 50000 + 1000 * 0.5


def test_stock_split_2_for_1():
    """10 送 10（2:1 拆股）：qty × 2, avg_cost / 2, 现金不变。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 10.0) < 1e-6
    assert abs(p.cash - 50000.0) < 1e-6


def test_bonus_shares_10_for_3():
    """10 送 3（bonus_ratio=1.3）：qty × 1.3, avg_cost / 1.3。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 9, 15), code="000001",
        action_type=ActionType.BONUS_SHARES,
        split_ratio=1.3,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1300
    assert abs(p.positions["000001"].avg_cost - 20.0 / 1.3) < 1e-6


def test_no_position_ignored():
    """不在持仓中的 code，公司行为应被忽略。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="600000",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=1.0,
    )
    p.apply_corporate_action(action)  # 不应报错
    assert "600000" not in p.positions
    assert abs(p.cash - 50000.0) < 1e-6


def test_multiple_actions_sequential():
    """连续多个公司行为：先分红，再送股。"""
    p = _make_portfolio()
    # 分红 0.5
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5,
    ))
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6

    # 10 送 10
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0,
    ))
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 19.5 / 2.0) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6

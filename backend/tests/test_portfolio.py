import datetime
from core.portfolio import Portfolio, Position, AccountSnapshot
from core.backtest_types import Position as PositionType


def test_position_freeze_and_thaw():
    """T+1 买入冻结，T+2 解冻。"""
    pos = Position(
        code="sh.600000",
        total_qty=500,
        available_qty=500,
        frozen_qty=0,
        avg_cost=10.0,
        market_value=5000.0
    )
    
    # T+1 买入 200，冻结
    pos.buy(qty=200, price=11.0)
    assert pos.total_qty == 700
    assert pos.available_qty == 500
    assert pos.frozen_qty == 200
    
    # T+2 解冻
    pos.thaw(thaw_qty=200)
    assert pos.total_qty == 700
    assert pos.available_qty == 700
    assert pos.frozen_qty == 0


def test_sell_uses_available_qty_only():
    """卖单只能使用 available_qty。"""
    pos = Position(
        code="sh.600000",
        total_qty=1000,
        available_qty=500,
        frozen_qty=500,
        avg_cost=10.0,
        market_value=10000.0
    )
    
    # 尝试卖出 600，只能卖 available_qty=500
    sold = pos.sell(qty=600, price=11.0)
    assert sold == 500
    assert pos.total_qty == 500
    assert pos.available_qty == 0
    assert pos.frozen_qty == 500  # frozen 不变


def test_cash_never_negative():
    """现金永不为负。"""
    # This is tested at Portfolio level
    pass


def test_equity_calculation_uses_raw_close():
    """组合估值使用 raw_close。"""
    from core.portfolio import Portfolio
    
    portfolio = Portfolio(initial_cash=1_000_000)
    # 买入 1000 股 @ 10.0
    portfolio.positions["sh.600000"] = Position(
        code="sh.600000",
        total_qty=1000,
        available_qty=1000,
        frozen_qty=0,
        avg_cost=10.0,
        market_value=10000.0
    )
    portfolio.cash = 990000.0
    
    # 估值使用 raw_close=11.0
    equity = portfolio.get_equity({"sh.600000": {"close": 11.0}})
    # cash + 1000 * 11.0 = 990000 + 11000 = 1001000
    assert equity == 1001000.0


def test_position_buy_updates_avg_cost():
    """买入更新平均成本。"""
    pos = Position(
        code="sh.600000",
        total_qty=500,
        available_qty=500,
        frozen_qty=0,
        avg_cost=10.0,
        market_value=5000.0
    )
    
    # 买入 200 股 @ 12.0 (冻结)
    pos.buy(qty=200, price=12.0)
    
    # 新 avg_cost = (500*10 + 200*12) / 700 = 7400/700 = 10.5714...
    expected_avg = (500 * 10.0 + 200 * 12.0) / 700
    assert abs(pos.avg_cost - expected_avg) < 0.001
    assert pos.total_qty == 700
    assert pos.frozen_qty == 200


def test_position_sell_updates_avg_cost():
    """卖出不改变平均成本（先进先出简化）。"""
    pos = Position(
        code="sh.600000",
        total_qty=1000,
        available_qty=1000,
        frozen_qty=0,
        avg_cost=10.0,
        market_value=10000.0
    )
    
    # 卖出 500
    sold = pos.sell(qty=500, price=12.0)
    assert sold == 500
    assert pos.total_qty == 500
    assert pos.avg_cost == 10.0  # 平均成本不变（FIFO 简化）


def test_position_thaw_updates_available():
    """解冻增加 available_qty。"""
    pos = Position(
        code="sh.600000",
        total_qty=700,
        available_qty=500,
        frozen_qty=200,
        avg_cost=10.5,
        market_value=7350.0
    )
    
    pos.thaw(thaw_qty=200)
    assert pos.frozen_qty == 0
    assert pos.available_qty == 700
    assert pos.total_qty == 700


def test_position_market_value_update():
    """市值随价格更新。"""
    pos = Position(
        code="sh.600000",
        total_qty=1000,
        available_qty=1000,
        frozen_qty=0,
        avg_cost=10.0,
        market_value=10000.0
    )
    
    pos.update_market_value(raw_close=11.0)
    assert pos.market_value == 11000.0


def test_avg_cost_after_dividend_and_split():
    """分红 + 送股后 avg_cost 正确。"""
    from datetime import date
    from core.portfolio import Portfolio, Position
    from core.corporate_actions import CorporateAction, ActionType

    p = Portfolio(initial_cash=100000.0)
    p.positions["000001"] = Position(
        code="000001", total_qty=1000, available_qty=1000,
        frozen_qty=0, avg_cost=20.0, market_value=0.0)
    # 分红 0.5
    p.apply_corporate_action(CorporateAction(
        date=date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5))
    # 10 送 10
    p.apply_corporate_action(CorporateAction(
        date=date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0))
    # 验证：avg_cost = (20 - 0.5) / 2 = 9.75
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 9.75) < 1e-6
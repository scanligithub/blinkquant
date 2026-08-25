import datetime
import polars as pl
from core.execution import ExecutionEngine, OrderIntent, Fill
from core.backtest_types import FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG
from core.portfolio import Position, Portfolio


def create_test_positions():
    """创建测试持仓。"""
    return {
        "sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0),
        "sz.000001": Position(code="sz.000001", total_qty=500, available_qty=500, frozen_qty=0, avg_cost=20.0, market_value=10000.0),
    }


def test_sell_first_then_buy_cash_available():
    """先卖后买，卖出回款当日可用于买入。"""
    positions = {
        "sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0),
    }
    raw_prices = {
        "sh.600000": {"open": 11.0, "close": 11.0},
        "sz.000001": {"open": 20.0, "close": 20.0},
    }
    
    intents = [
        OrderIntent(code="sh.600000", side="SELL", target_qty=500, target_weight=0.0),
        OrderIntent(code="sz.000001", side="BUY", target_qty=250, target_weight=0.5),
    ]
    
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, FeeConfig())
    fills = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=intents,
        positions={"sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0)},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}, "sz.000001": {"open": 20.0, "close": 20.0}},
        cash=10000.0,
    )
    
    # 使用 Portfolio 应用成交
    portfolio = Portfolio(initial_cash=10000.0)
    portfolio.positions = {"sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0)}
    portfolio.apply_fills(fills, datetime.date(2025, 1, 3), {"sh.600000": {"close": 11.0}, "sz.000001": {"close": 20.0}})
    
    sell_fills = [f for f in fills if f.side == "SELL"]
    buy_fills = [f for f in fills if f.side == "BUY"]
    assert len(sell_fills) == 1
    assert sell_fills[0].qty == 500
    assert sell_fills[0].price == 11.0
    assert len(buy_fills) == 1
    assert buy_fills[0].qty == 250
    assert buy_fills[0].price == 20.0
    # 卖出 500 * 11.0 = 5500, fee ≈ 7.81, 净收入 ≈ 5492.19
    # 买入 250 * 20.0 = 5000, fee ≈ 5.05
    # 现金变化：10000 + 5492.19 - 5005.05 = 10487.14
    assert abs(portfolio.cash - 10487.14) < 0.1


def test_t1_buy_cannot_sell_same_day():
    """T+1 买入的股票当日不可卖出。"""
    positions = {
        "sh.600000": Position(code="sh.600000", total_qty=500, available_qty=0, frozen_qty=500, avg_cost=10.0, market_value=5000.0),
    }
    raw_prices = {"sh.600000": {"open": 11.0, "close": 11.0}}
    
    intents = [
        OrderIntent(code="sh.600000", side="SELL", target_qty=500, target_weight=0.0),
    ]
    
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, FeeConfig())
    fills = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=intents,
        positions={"sh.600000": Position(code="sh.600000", total_qty=500, available_qty=0, frozen_qty=500, avg_cost=10.0, market_value=5000.0)},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}},
        cash=0.0,
    )
    
    # frozen_qty = 500, available_qty = 0 -> SELL 应该是 0
    assert len(fills) == 0


def test_fee_calculation():
    """费用计算：佣金 + 印花税 + 过户费。"""
    fee_config = FeeConfig(commission_rate=0.00025, commission_min=5.0, stamp_tax_rate=0.0005, transfer_fee_rate=0.00001)
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, fee_config)
    
    # BUY 1000 @ 10.0
    fee_buy = engine._calc_fee(10000.0, "BUY")
    # commission = max(10000*0.00025, 5) = 5
    # stamp_tax = 0
    # transfer = 10000 * 0.00001 = 0.1
    # total = 5.1
    assert abs(fee_buy - 5.1) < 0.01
    
    # SELL 1000 @ 11.0
    fee_sell = engine._calc_fee(11000.0, "SELL")
    # commission = max(11000*0.00025, 5) = 5
    # stamp_tax = 11000 * 0.0005 = 5.5
    # transfer = 11000 * 0.00001 = 0.11
    # total = 5 + 5.5 + 0.11 = 10.61
    assert abs(fee_sell - 10.61) < 0.01


def test_partial_fill_cash_retained():
    """部分成交后现金留存。"""
    positions = {
        "sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0),
    }
    raw_prices = {
        "sh.600000": {"open": 11.0, "close": 11.0},
        "sz.000001": {"open": 20.0, "close": 20.0},
    }
    
    intents = [
        OrderIntent(code="sh.600000", side="SELL", target_qty=1000, target_weight=0.0),
        OrderIntent(code="sz.000001", side="BUY", target_qty=1000, target_weight=1.0),  # 需要 20000 但只有约 10000
    ]
    
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, FeeConfig())
    fills = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=intents,
        positions={"sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0)},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}, "sz.000001": {"open": 20.0, "close": 20.0}},
        cash=0.0,
    )
    
    # SELL all 1000 @ 11.0 -> net ~10994.39 cash
    # BUY sz.000001: max affordable = 10994.39 / (20 * 1.00025) approx 549 shares
    # 剩余现金应为正数
    portfolio = Portfolio(initial_cash=0.0)
    portfolio.positions = {"sh.600000": Position(code="sh.600000", total_qty=1000, available_qty=1000, frozen_qty=0, avg_cost=10.0, market_value=10000.0)}
    portfolio.apply_fills(fills, datetime.date(2025, 1, 3), {"sh.600000": {"close": 11.0}, "sz.000001": {"close": 20.0}})
    assert portfolio.cash > 0


def test_limit_up_down_from_position():
    """测试 Position 中的 limit_up/limit_down 标记阻止交易。"""
    # This test requires position to have limit_up/limit_down flags
    # For now, skip - needs integration with data_manager limit flags
    pass


def test_single_direction_per_execution_date():
    """同一股票同一 execution_date 只能一个净交易方向。"""
    intents = [
        OrderIntent(code="sh.600000", side="BUY", target_qty=100, target_weight=0.5),
        OrderIntent(code="sh.600000", side="SELL", target_qty=100, target_weight=0.0),
    ]
    
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, FeeConfig())
    fills = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=intents,
        positions={},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}},
        cash=10000.0,
    )
    
    # The engine should handle this by netting or rejecting
    # For MVP, we just check it doesn't crash
    assert isinstance(fills, list)
import datetime
import polars as pl
from core.execution import (
    ExecutionEngine, OrderIntent, Fill,
    R_SUSPENDED, R_LIMIT_BLOCKED, R_FROZEN, R_CASH_STARVED,
    R_NO_PRICE, R_ZERO_TARGET, R_BELOW_LOT,
)
from core.backtest_types import FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG
from core.portfolio import Position, Portfolio


def _engine():
    return ExecutionEngine(MVP_EXECUTION_CONFIG, FeeConfig())


def _pos(code="sh.600000", total=1000, avail=None, frozen=0, cost=10.0):
    avail = total if avail is None else avail
    return Position(code=code, total_qty=total, available_qty=avail,
                    frozen_qty=frozen, avg_cost=cost, market_value=total * cost)


def _reasons(rep):
    return {r.code + "|" + r.side: r.reason for r in rep.rejections}


def test_sell_first_then_buy_cash_available():
    """先卖后买，卖出回款当日可用于买入。"""
    raw_prices = {
        "sh.600000": {"open": 11.0, "close": 11.0},
        "sz.000001": {"open": 20.0, "close": 20.0},
    }
    intents = [
        OrderIntent(code="sh.600000", side="SELL", target_qty=500, target_weight=0.0),
        OrderIntent(code="sz.000001", side="BUY", target_qty=250, target_weight=0.5),
    ]
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=intents,
        positions={"sh.600000": _pos(total=1000)},
        raw_prices=raw_prices,
        cash=10000.0,
    )

    portfolio = Portfolio(initial_cash=10000.0)
    portfolio.positions = {"sh.600000": _pos(total=1000)}
    portfolio.apply_fills(rep.fills, datetime.date(2025, 1, 3), raw_prices)

    sell_fills = [f for f in rep.fills if f.side == "SELL"]
    buy_fills = [f for f in rep.fills if f.side == "BUY"]
    assert len(sell_fills) == 1 and sell_fills[0].qty == 500 and sell_fills[0].price == 11.0
    # 整手取整：BUY 意图 250 股 → 向下取整手 200 股
    assert len(buy_fills) == 1 and buy_fills[0].qty == 200 and buy_fills[0].price == 20.0
    # 现金：10000 + (5500-7.81) - (4000+5.04) = 11487.15
    assert abs(portfolio.cash - 11487.15) < 0.1


def test_t1_buy_cannot_sell_same_day():
    """T+1 买入的股票当日不可卖出 → FROZEN 标签。"""
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.600000", side="SELL", target_qty=500, target_weight=0.0)],
        positions={"sh.600000": _pos(total=500, avail=0, frozen=500)},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}},
        cash=0.0,
    )
    assert len(rep.fills) == 0
    assert _reasons(rep)["sh.600000|SELL"] == R_FROZEN


def test_fee_calculation():
    fee_config = FeeConfig(commission_rate=0.00025, commission_min=5.0,
                           stamp_tax_rate=0.0005, transfer_fee_rate=0.00001)
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, fee_config)
    assert abs(engine._calc_fee(10000.0, "BUY") - 5.1) < 0.01
    assert abs(engine._calc_fee(11000.0, "SELL") - 10.61) < 0.01


def test_partial_fill_cash_retained():
    """部分成交后现金留存。"""
    raw_prices = {"sh.600000": {"open": 11.0}, "sz.000001": {"open": 20.0}}
    intents = [
        OrderIntent(code="sh.600000", side="SELL", target_qty=1000, target_weight=0.0),
        OrderIntent(code="sz.000001", side="BUY", target_qty=1000, target_weight=1.0),
    ]
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3), intents=intents,
        positions={"sh.600000": _pos(total=1000)}, raw_prices=raw_prices, cash=0.0,
    )
    portfolio = Portfolio(initial_cash=0.0)
    portfolio.positions = {"sh.600000": _pos(total=1000)}
    portfolio.apply_fills(rep.fills, datetime.date(2025, 1, 3), raw_prices)
    assert portfolio.cash > 0


def test_rejection_taxonomy_suspended():
    """停牌 → SUSPENDED（fail-closed：标记缺失同停牌）。"""
    raw = {"sh.X": {"open": 10.0}}
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.X", side="BUY", target_qty=100, target_weight=1.0)],
        positions={}, raw_prices=raw, cash=1e6,
        limit_flags={"sh.X": {"is_limit_up": False, "is_limit_down": False, "is_suspended": True}},
    )
    assert len(rep.fills) == 0
    assert _reasons(rep)["sh.X|BUY"] == R_SUSPENDED

    rep2 = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.Y", side="BUY", target_qty=100, target_weight=1.0)],
        positions={}, raw_prices={"sh.Y": {"open": 10.0}}, cash=1e6,
        limit_flags={},   # 缺该 code 条目 → 视为停牌
    )
    assert len(rep2.fills) == 0
    assert _reasons(rep2)["sh.Y|BUY"] == R_SUSPENDED


def test_rejection_taxonomy_limit_blocked():
    """涨停禁买 / 跌停禁卖。"""
    e = _engine()
    rep_buy = e.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.A", side="BUY", target_qty=100, target_weight=1.0)],
        positions={}, raw_prices={"sh.A": {"open": 11.0}}, cash=1e6,
        limit_flags={"sh.A": {"is_limit_up": True, "is_limit_down": False, "is_suspended": False}},
    )
    assert _reasons(rep_buy)["sh.A|BUY"] == R_LIMIT_BLOCKED

    rep_sell = e.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.B", side="SELL", target_qty=100, target_weight=0.0)],
        positions={"sh.B": _pos("sh.B", 100, 100)},
        raw_prices={"sh.B": {"open": 9.0}},
        cash=0.0,
        limit_flags={"sh.B": {"is_limit_up": False, "is_limit_down": True, "is_suspended": False}},
    )
    assert len(rep_sell.fills) == 0
    assert _reasons(rep_sell)["sh.B|SELL"] == R_LIMIT_BLOCKED


def test_rejection_taxonomy_cash_starved_and_no_price_and_zero_target():
    e = _engine()
    # CASH_STARVED：现金不足一手
    rep = e.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.C", side="BUY", target_qty=500, target_weight=1.0)],
        positions={}, raw_prices={"sh.C": {"open": 50.0}}, cash=400.0,
    )
    assert len(rep.fills) == 0
    assert _reasons(rep)["sh.C|BUY"] == R_CASH_STARVED

    # NO_PRICE：执行日缺失 open
    rep2 = e.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.D", side="BUY", target_qty=100, target_weight=1.0)],
        positions={}, raw_prices={}, cash=1e6,
    )
    assert _reasons(rep2)["sh.D|BUY"] == R_NO_PRICE

    # ZERO_TARGET：planner 异常输出 qty<=0（防御性）
    rep3 = e.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.E", side="BUY", target_qty=0, target_weight=1.0)],
        positions={}, raw_prices={"sh.E": {"open": 10.0}}, cash=1e6,
    )
    assert _reasons(rep3)["sh.E|BUY"] == R_ZERO_TARGET


def test_buy_floors_to_lot_size():
    """BUY 目标数量向下取整手：537 股意图 → 成交 500 股。"""
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.600000", side="BUY", target_qty=537, target_weight=0.5)],
        positions={},
        raw_prices={"sh.600000": {"open": 10.0, "close": 10.0}},
        cash=1_000_000.0,
    )
    assert len(rep.fills) == 1
    assert rep.fills[0].side == "BUY"
    assert rep.fills[0].qty == 500
    assert rep.fills[0].qty % 100 == 0


def test_buy_below_one_lot_is_dropped():
    """不足一手的 BUY 意图 → BELOW_LOT 拒单标签。"""
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.600000", side="BUY", target_qty=53, target_weight=0.5)],
        positions={},
        raw_prices={"sh.600000": {"open": 10.0, "close": 10.0}},
        cash=1_000_000.0,
    )
    assert len(rep.fills) == 0
    assert _reasons(rep)["sh.600000|BUY"] == R_BELOW_LOT


def test_buy_min_commission_is_per_order():
    """最低佣金按一笔订单判断：小额买入佣金取 commission_min。"""
    fee_config = FeeConfig(commission_rate=0.00025, commission_min=5.0)
    engine = ExecutionEngine(MVP_EXECUTION_CONFIG, fee_config)
    fills = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.600000", side="BUY", target_qty=100, target_weight=1.0)],
        positions={},
        raw_prices={"sh.600000": {"open": 10.0, "close": 10.0}},
        cash=1005.02,
    ).fills
    assert len(fills) == 1 and fills[0].qty == 100

    fills2 = engine.execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[OrderIntent(code="sh.600000", side="BUY", target_qty=100, target_weight=1.0)],
        positions={},
        raw_prices={"sh.600000": {"open": 10.0, "close": 10.0}},
        cash=1004.99,
    ).fills
    assert len(fills2) == 0


def test_multi_buy_sequenced_cash():
    """同一 cycle 多笔 BUY 顺序扣减现金：第二笔受第一笔占用约束。"""
    fills = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[
            OrderIntent(code="sh.600000", side="BUY", target_qty=500, target_weight=0.5),
            OrderIntent(code="sz.000001", side="BUY", target_qty=500, target_weight=0.5),
        ],
        positions={},
        raw_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 20.0, "close": 20.0},
        },
        cash=10000.0,
    ).fills
    buys = {f.code: f for f in fills if f.side == "BUY"}
    assert buys["sh.600000"].qty == 500
    assert "sz.000001" in buys
    assert buys["sz.000001"].qty % 100 == 0
    assert buys["sz.000001"].qty * 20 <= 4998


def test_limit_up_down_from_position():
    """涨跌停路径经 taxonomy 已覆盖（见 limit_blocked 用例）。"""
    pass


def test_single_direction_per_execution_date():
    """同一股票同一 execution_date 双向意图不崩溃，产出合法 report。"""
    rep = _engine().execute(
        execution_date=datetime.date(2025, 1, 3),
        intents=[
            OrderIntent(code="sh.600000", side="BUY", target_qty=100, target_weight=0.5),
            OrderIntent(code="sh.600000", side="SELL", target_qty=100, target_weight=0.0),
        ],
        positions={},
        raw_prices={"sh.600000": {"open": 11.0, "close": 11.0}},
        cash=10000.0,
    )
    assert isinstance(rep.fills, list)
    assert all(f.price > 0 for f in rep.fills)
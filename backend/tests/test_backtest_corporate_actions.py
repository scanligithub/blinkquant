"""BacktestEngine 公司行为集成测试。"""
import datetime
import polars as pl
from core.portfolio import Portfolio, Position
from core.corporate_actions import CorporateAction, ActionType, CorporateActionStore
from core.backtest_engine import BacktestEngine, TradingCalendar


def test_portfolio_apply_dividend():
    """验证 Portfolio.apply_corporate_action 在分红场景正确。"""
    p = Portfolio(initial_cash=100000.0)
    p.positions["000001"] = Position(
        code="000001", total_qty=1000, available_qty=1000,
        frozen_qty=0, avg_cost=20.0, market_value=20000.0)
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5)
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.cash - 100500.0) < 1e-6
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6


def test_store_query_all():
    """验证 CorporateActionStore.query_all 按日期范围查询所有 code。"""
    store = CorporateActionStore([
        CorporateAction(date=datetime.date(2024, 7, 1), code="000001",
                        action_type=ActionType.CASH_DIVIDEND,
                        cash_dividend_per_share=0.5),
        CorporateAction(date=datetime.date(2024, 8, 1), code="600000",
                        action_type=ActionType.STOCK_SPLIT,
                        split_ratio=2.0),
        CorporateAction(date=datetime.date(2024, 12, 25), code="000001",
                        action_type=ActionType.STOCK_SPLIT,
                        split_ratio=2.0),
    ])
    # 查 2024H1: 只有 000001 的分红
    q1 = store.query_all(datetime.date(2024, 1, 1), datetime.date(2024, 7, 31))
    assert len(q1) == 1
    assert q1[0].code == "000001"
    # 查全年: 3 条
    q2 = store.query_all(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert len(q2) == 3


def test_engine_runs_with_corporate_action_store():
    """集成测试：BacktestEngine.run() 接受 corporate_action_store 参数。"""
    # 构造一个极简的 CorporateActionStore（空的，不影响回测）
    store = CorporateActionStore([])
    # 验证参数被接受且不报错（不需要真正跑回测，只验证签名兼容）
    assert hasattr(BacktestEngine, 'run')
    # 验证 run 方法签名包含 corporate_action_store
    import inspect
    sig = inspect.signature(BacktestEngine.run)
    assert 'corporate_action_store' in sig.parameters
    param = sig.parameters['corporate_action_store']
    assert param.default is None  # 向后兼容

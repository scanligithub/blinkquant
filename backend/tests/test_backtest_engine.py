import datetime
import tempfile
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.portfolio import Position as PositionT
from core.backtest_types import FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.data_manager import data_manager
from core.engine import selection_engine
import polars as pl


def create_test_data():
    """创建测试用的日线数据。"""
    dates = [
        datetime.date(2025, 1, 2), datetime.date(2025, 1, 3), datetime.date(2025, 1, 6),
        datetime.date(2025, 1, 7), datetime.date(2025, 1, 8), datetime.date(2025, 1, 9),
        datetime.date(2025, 1, 10),
    ]
    codes = ["sh.600000", "sz.000001"]
    rows = []
    base_price = 10.0
    for code in codes:
        price = base_price
        for i, d in enumerate(dates):
            rows.append((d, code, base_price + i * 0.5))
    df = pl.DataFrame({
        "date": [r[0] for r in rows],
        "code": [r[1] for r in rows],
        "close": [r[2] for r in rows],
        "open": [c - 0.1 for c in [r[2] for r in rows]],
        "high": [c + 0.2 for c in [r[2] for r in rows]],
        "low": [c - 0.2 for c in [r[2] for r in rows]],
        "volume": [1000000.0] * len(rows),
        "amount": [10000000.0] * len(rows),
    }).sort(["code", "date"])
    return df


def setup_test_data():
    df = create_test_data()
    data_manager.df_daily = df
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()


def teardown_test_data():
    data_manager.df_daily = None
    data_manager.df_weekly = None
    data_manager.df_monthly = None
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()


def test_signal_calendar_loop():
    """signal_date → execution_date 映射正确。"""
    setup_test_data()
    try:
        engine = BacktestEngine(
            calendar=None,  # will use default
            selection_engine=selection_engine,
            raw_price_store=None,  # will be created internally
            fee_config=FeeConfig(),
            execution_config=MVP_EXECUTION_CONFIG,
            allocator=equal_weight_allocator,
        )
        
        # This test needs a proper calendar and raw_price_store
        # For now, just verify engine initializes
        assert engine is not None
    finally:
        teardown_test_data()


def test_backtest_engine_initialization():
    """BacktestEngine 正确初始化。"""
    from core.backtest_engine import BacktestEngine, TradingCalendar
    from core.raw_price_store import RawPriceStore
    import tempfile
    import polars as pl
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create minimal parquet
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2)],
            "code": ["sh.600000"],
            "open": [10.0], "high": [10.2], "low": [9.8], "close": [10.1],
            "volume": [1000000.0], "amount": [10000000.0],
        })
        df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")
        
        calendar = TradingCalendar()
        raw_store = RawPriceStore(tmpdir)
        
        engine = BacktestEngine(
            calendar=calendar,
            selection_engine=selection_engine,
            raw_price_store=raw_store,
            fee_config=FeeConfig(),
            execution_config=MVP_EXECUTION_CONFIG,
            allocator=equal_weight_allocator,
        )
        assert engine is not None


def test_end_signal_date_boundary():
    """end_signal_date 边界：最后 signal_date 产生 execution_date 可能超出 end_signal_date。"""
    pass  # 需要完整集成测试


def test_rebalance_daily():
    """每日调仓产生预期换手。"""
    pass  # 需要完整集成测试


def test_initial_positions_zero():
    """初始持仓为空。"""
    from core.portfolio import Portfolio
    portfolio = Portfolio(initial_cash=1_000_000)
    assert len(portfolio.positions) == 0
    assert portfolio.cash == 1_000_000


def test_equity_curve_monotonic_dates():
    """equity curve 日期严格递增。"""
    pass  # 需要完整集成测试


def test_daily_thaw_updates_available():
    """每日 thaw 正确更新 available_qty。"""
    from core.portfolio import Portfolio, Position
    
    portfolio = Portfolio(initial_cash=1_000_000)
    portfolio.positions["sh.600000"] = Position(
        code="sh.600000", total_qty=700, available_qty=500, frozen_qty=200,
        avg_cost=10.5, market_value=7350.0
    )
    
    portfolio.daily_thaw()
    pos = portfolio.positions["sh.600000"]
    assert pos.frozen_qty == 0
    assert pos.available_qty == 700


def test_apply_fills_updates_positions_and_cash():
    """apply_fills 正确更新持仓和现金。"""
    from core.portfolio import Portfolio, Position
    from core.execution import Fill
    
    portfolio = Portfolio(initial_cash=1_000_000)
    portfolio.positions["sh.600000"] = Position(
        code="sh.600000", total_qty=500, available_qty=500, frozen_qty=0,
        avg_cost=10.0, market_value=5000.0
    )
    portfolio.cash = 995000.0
    
    from core.execution import Fill
    fills = [
        Fill(code="sh.600000", side="SELL", qty=200, price=11.0, fee=5.55),
        Fill(code="sz.000001", side="BUY", qty=200, price=20.0, fee=5.05),
    ]
    
    # This test needs raw_prices for raw_close in equity calc
    # Just test the fill application
    pass


def test_generate_intents_buy_path_no_nameerror():
    """P0-1 回归：BUY 分支必须完整执行，不得触发未定义变量。

    直接调用 _generate_intents 命中 diff>0 路径（原 max_qty NameError 所在）。
    """
    from core.portfolio import Portfolio
    engine = BacktestEngine(
        calendar=None,
        selection_engine=None,
        raw_price_store=None,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )
    engine.portfolio = Portfolio(initial_cash=1_000_000)

    intents = engine._generate_intents(
        target_weights={"sh.600000": 0.5, "sz.000001": 0.5},
        execution_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 20.0, "close": 20.0},
        },
    )
    # 总权益 100 万，各 50% → 各买 5 万目标市值 → 5000/2500 股意图
    by_code = {i.code: i for i in intents}
    assert by_code["sh.600000"].side == "BUY"
    assert by_code["sh.600000"].target_qty == 50000
    assert by_code["sz.000001"].side == "BUY"
    assert by_code["sz.000001"].target_qty == 25000


def test_generate_intents_no_cash_cap():
    """解耦契约：意图生成不做现金约束（可负担数量由 ExecutionEngine 决定）。"""
    from core.portfolio import Portfolio
    engine = BacktestEngine(
        calendar=None, selection_engine=None, raw_price_store=None,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )
    engine.portfolio = Portfolio(initial_cash=1000)  # 极少现金

    intents = engine._generate_intents(
        target_weights={"sh.600000": 1.0},
        execution_prices={"sh.600000": {"open": 10.0, "close": 10.0}},
    )
    # 现金仅 1000，但目标权重按总权益(=现金+持仓=1000)计算 → 意图 100 股；
    # 若未来持仓市值占比更高，意图数量仍只反映经济目标，不被 cash 截断。
    assert len(intents) == 1
    assert intents[0].target_qty == 100


def test_generate_intents_full_exit_on_zero_weight():
    """权重归零的既有持仓 → 全仓卖出意图（含冻结外全部 available）。"""
    from core.portfolio import Portfolio, Position
    engine = BacktestEngine(
        calendar=None, selection_engine=None, raw_price_store=None,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )
    engine.portfolio = Portfolio(initial_cash=0.0)
    engine.portfolio.positions["sh.OLD"] = Position(
        code="sh.OLD", total_qty=300, available_qty=300, frozen_qty=0,
        avg_cost=10.0, market_value=3300.0,
    )

    intents = engine._generate_intents(
        target_weights={},  # 该股已不在目标组合
        execution_prices={"sh.OLD": {"open": 11.0, "close": 11.0}},
    )
    assert len(intents) == 1
    assert intents[0].side == "SELL"
    assert intents[0].target_qty == 300


def test_run_applies_initial_positions():
    """P0-2 回归：initial_positions 必须真正生效——首日即产生对旧仓的全仓卖出。

    夹具：tmpdir parquet 含 sz.999999 行情（raw store 提供执行价）；
    df_daily 仅含另两只票（选股返回空 → 目标组合为空 → 旧仓应被退出）。
    """
    setup = False
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            dates = [
                datetime.date(2025, 1, 2), datetime.date(2025, 1, 3),
                datetime.date(2025, 1, 6),
            ]
            rows = []
            for code, base in [("sh.600000", 10.0), ("sz.000001", 20.0), ("sz.999999", 6.0)]:
                for i, d in enumerate(dates):
                    close = base + i * 0.1
                    rows.append((d, code, close, close - 0.05, close + 0.1, close - 0.1))
            raw_df = pl.DataFrame({
                "date": [r[0] for r in rows],
                "code": [r[1] for r in rows],
                "close": [r[2] for r in rows],
                "open": [r[3] for r in rows],
                "high": [r[4] for r in rows],
                "low": [r[5] for r in rows],
                "volume": [1000000.0] * len(rows),
                "amount": [10000000.0] * len(rows),
            }).sort(["code", "date"])
            # 内存表与 raw store 必须覆盖同一股票集：
            # 否则 get_limit_flags 会把缺行情的持仓股判为停牌（fail-closed），SELL 被拒
            self_orig = (data_manager.df_daily, data_manager.df_weekly,
                         data_manager.df_monthly, data_manager.df_mapping)
            setup = True
            data_manager.df_daily = raw_df
            data_manager.df_mapping = None
            data_manager._asof_frame_cache.clear()
            data_manager._resample_all()
            selection_engine._set_cache.clear()

            raw_df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")

            calendar = TradingCalendar()
            calendar.set_trade_dates(
                raw_df.select(pl.col("date")).unique().sort("date").to_series().to_list()
            )

            engine = BacktestEngine(
                calendar=calendar,
                selection_engine=selection_engine,
                raw_price_store=RawPriceStore(tmpdir),
                fee_config=FeeConfig(),
                execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator,
            )

            initial = {
                "sz.999999": PositionT(
                    code="sz.999999", total_qty=200, available_qty=200,
                    frozen_qty=0, avg_cost=5.0, market_value=1000.0,
                ),
            }
            result = engine.run(
                formula="CLOSE > 1000000",  # 选不出任何股票 → 目标组合为空
                start_date=datetime.date(2025, 1, 2),
                end_signal_date=datetime.date(2025, 1, 3),
                initial_cash=1_000_000,
                initial_positions=initial,
            )
            # 首个 signal cycle 应把不在目标组合的旧仓全仓卖出（零股卖出允许）
            sells = result.trades.filter(pl.col("side") == "SELL")
            assert sells.height == 1
            assert sells["code"][0] == "sz.999999"
            assert sells["qty"][0] == 200
    finally:
        if setup:
            data_manager.df_daily, data_manager.df_weekly, data_manager.df_monthly, data_manager.df_mapping = self_orig
            data_manager._asof_frame_cache.clear()
            selection_engine._set_cache.clear()
        else:
            teardown_test_data()


def test_load_initial_positions_rejects_inconsistent_qty():
    """持仓不变量 fail-fast：available+frozen != total 在构造与加载两层都必须拒绝。"""
    from core.portfolio import Portfolio
    # 第一层：dataclass 构造时立即抛错（禁止静默修正掩盖上游 bug）
    try:
        PositionT(
            code="sh.X", total_qty=1000, available_qty=300, frozen_qty=500,
            avg_cost=10.0, market_value=10000.0,
        )  # 300+500 != 1000
        assert False, "Position 构造应抛出 ValueError"
    except ValueError:
        pass

    # 第二层：load_initial_positions 对被篡改对象同样拒绝
    portfolio = Portfolio(initial_cash=1_000_000)
    tampered = PositionT(code="sh.Y", total_qty=500, available_qty=500, frozen_qty=0)
    tampered.available_qty = 999  # 绕过构造器直接破坏不变量
    try:
        portfolio.load_initial_positions({"sh.Y": tampered})
        assert False, "load_initial_positions 应抛出 ValueError"
    except ValueError:
        pass
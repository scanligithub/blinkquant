"""Deterministic micro backtest：2 股 × 2 日，全部数字可手算验证。

覆盖链路：SelectionEngine(signal) → RawPriceStore(raw open/close)
→ ExecutionEngine(整手/费用/现金) → Portfolio.apply_fills → 严格估值 → equity curve。
"""
import datetime
import tempfile
import polars as pl
import pytest

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.portfolio import Position
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator, BacktestDataIntegrityError
from core.data_manager import data_manager
from core.engine import selection_engine

D1 = datetime.date(2025, 1, 2)
D2 = datetime.date(2025, 1, 3)


def _build_frames():
    """D1: A close=16（入选），B close=9；D2: A open=10 / close=12。"""
    rows = [
        (D1, "sh.A", 15.9, 16.0, 16.2, 15.8),
        (D1, "sz.B", 9.0, 9.0, 9.2, 8.8),
        (D2, "sh.A", 10.0, 12.0, 12.2, 9.8),
        (D2, "sz.B", 10.0, 10.0, 10.1, 9.9),
    ]
    df = pl.DataFrame({
        "date": [r[0] for r in rows],
        "code": [r[1] for r in rows],
        "open": [r[2] for r in rows],
        "close": [r[3] for r in rows],
        "high": [r[4] for r in rows],
        "low": [r[5] for r in rows],
        "volume": [1_000_000.0] * len(rows),
        "amount": [10_000_000.0] * len(rows),
    }).sort(["code", "date"])
    return df


def _install(df: pl.DataFrame):
    data_manager.df_daily = df
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()


def test_micro_end_to_end_hand_computed():
    """信号(D1 CLOSE>15 → A) → D2 开盘买入 → D2 raw_close 估值，全链路数字手算对齐。

    手算：
      intent qty = floor(1_000_000 / 10) = 100_000 → 整手不变
      可负担下探：q=100_000 amount=1_000_000 > cash ✗；
                  q=99_900 amount=999_000,
                  fee = max(999000*0.00025,5)=249.75 + transfer 9.99 = 259.74
                  cost = 999_259.74 ≤ 1_000_000 ✓ → 成交 99_900 股 @10.0
      cash_after = 1_000_000 − 999_000 − 259.74 = 740.26
      positions_value(D2 close=12) = 99_900 × 12 = 1_198_800
      equity = 740.26 + 1_198_800 = 1_199_540.26
    """
    df = _build_frames()
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")
            calendar = TradingCalendar()
            calendar.set_trade_dates([D1, D2])

            engine = BacktestEngine(
                calendar=calendar,
                selection_engine=selection_engine,
                raw_price_store=RawPriceStore(data_root=tmpdir),
                fee_config=FeeConfig(),
                execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator,
            )
            result = engine.run(
                formula="CLOSE > 15",
                start_date=D1,
                end_signal_date=D1,
                initial_cash=1_000_000,
            )

            # ---- trades ----
            assert result.trades.height == 1
            t = result.trades.to_dicts()[0]
            assert t["signal_date"] == D1
            assert t["execution_date"] == D2
            assert t["code"] == "sh.A"
            assert t["side"] == "BUY"
            assert t["qty"] == 99_900          # 整手 + 现金约束下探
            assert t["price"] == 10.0          # raw open（非 qfq、非 signal 收盘）
            assert abs(t["fee"] - 259.74) < 0.01

            # ---- equity curve：单行，估值日 = execution_date ----
            assert result.equity_curve.height == 1
            row = result.equity_curve.to_dicts()[0]
            assert row["date"] == D2
            assert abs(row["cash"] - 740.26) < 1e-6
            assert abs(row["positions_value"] - 1_198_800.0) < 1e-6
            assert abs(row["equity"] - 1_199_540.26) < 1e-6

            # ---- 恒等式与风控断言 ----
            assert abs(row["equity"] - (row["cash"] + row["positions_value"])) < 1e-9
            assert row["cash"] >= 0

            # ---- metrics 一致性（单行曲线：total_return 为曲线内首尾相对，恒为 0；
            #      绝对收益水平已由上方 equity 精确断言）----
            assert result.metrics["total_days"] == 1
            assert result.metrics["total_return"] == 0.0

            # ---- positions_daily 审计输出 ----
            assert result.positions_daily.height == 1
            pd_row = result.positions_daily.to_dicts()[0]
            assert pd_row["date"] == D2 and pd_row["code"] == "sh.A" and pd_row["qty"] == 99_900

            # ---- MetricsCalculator 集成（纯后处理，手算数字复验）----
            from core.metrics import compute_metrics
            m = compute_metrics(result, initial_cash=1_000_000)
            assert abs(m.performance.total_return - 0.0) < 1e-12      # 单行曲线首尾相对
            assert m.trading.trade_count == 1 and m.trading.buy_count == 1
            assert m.trading.trade_days == 1
            assert abs(m.trading.gross_buy - 999_000.0) < 1e-6
            assert abs(m.trading.total_fees - 259.74) < 0.01
            assert abs(m.exposure.deployment_mean - 1198800.0 / 1199540.26) < 1e-9
            assert abs(m.exposure.cash_drag - (740.26 / 1199540.26)) < 1e-9
            assert m.execution_quality.dust_reject_count == 0
            assert m.execution_quality.carried_events == 0
            flat = m.to_flat_dict()
            assert "integrity.negative_cash_count" in flat
    finally:
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()


def test_suspended_holding_carries_forward_last_close():
    """停牌持仓（曾有价、当日缺行情）→ 沿用最后可用价估值（derived 规则），不报错。"""
    D3 = datetime.date(2025, 1, 6)
    rows = [
        (D1, "sh.X", 10.0, 10.0, 10.2, 9.8),
        (D2, "sh.X", 10.0, 10.0, 10.2, 9.8),
        (D1, "sz.SUSP", 5.0, 5.0, 5.2, 4.8),   # 仅 D1 有行情 → 之后视为停牌
    ]
    df = pl.DataFrame({
        "date": [r[0] for r in rows],
        "code": [r[1] for r in rows],
        "open": [r[2] for r in rows],
        "close": [r[3] for r in rows],
        "high": [r[4] for r in rows],
        "low": [r[5] for r in rows],
        "volume": [1_000_000.0] * len(rows),
        "amount": [10_000_000.0] * len(rows),
    }).sort(["code", "date"])
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")
            calendar = TradingCalendar()
            calendar.set_trade_dates(sorted({D1, D2}))

            engine = BacktestEngine(
                calendar=calendar,
                selection_engine=selection_engine,
                raw_price_store=RawPriceStore(data_root=tmpdir),
                fee_config=FeeConfig(),
                execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator,
            )
            result = engine.run(
                formula="CLOSE > 1000000",   # 无新信号
                start_date=D1,
                end_signal_date=D1,
                initial_cash=1_000_000,
                initial_positions={
                    "sz.SUSP": Position(
                        code="sz.SUSP", total_qty=100, available_qty=100,
                        frozen_qty=0, avg_cost=5.0, market_value=500.0,
                    ),
                },
            )
            # D2 估值：sz.SUSP 停牌 → 沿用 D1 close=5.0
            row = result.equity_curve.to_dicts()[0]
            assert row["date"] == D2
            assert abs(row["cash"] - 1_000_000) < 1e-9
            assert abs(row["positions_value"] - 500.0) < 1e-9
            assert abs(row["equity"] - 1_000_500.0) < 1e-9
    finally:
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()


def test_never_priced_holding_raises_integrity_error():
    """未知资产（窗口内从未有任何价格）→ BacktestDataIntegrityError（fail-fast）。"""
    rows = [
        (D1, "sh.X", 10.0, 10.0, 10.2, 9.8),
        (D2, "sh.X", 10.0, 10.0, 10.2, 9.8),
        # 注意：sz.GHOST 无任何行情行
    ]
    df = pl.DataFrame({
        "date": [r[0] for r in rows],
        "code": [r[1] for r in rows],
        "open": [r[2] for r in rows],
        "close": [r[3] for r in rows],
        "high": [r[4] for r in rows],
        "low": [r[5] for r in rows],
        "volume": [1_000_000.0] * len(rows),
        "amount": [10_000_000.0] * len(rows),
    }).sort(["code", "date"])
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")
            calendar = TradingCalendar()
            calendar.set_trade_dates(sorted({D1, D2}))

            engine = BacktestEngine(
                calendar=calendar,
                selection_engine=selection_engine,
                raw_price_store=RawPriceStore(data_root=tmpdir),
                fee_config=FeeConfig(),
                execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator,
            )
            with pytest.raises(BacktestDataIntegrityError) as ei:
                engine.run(
                    formula="CLOSE > 1000000",
                    start_date=D1,
                    end_signal_date=D1,
                    initial_cash=1_000_000,
                    initial_positions={
                        "sz.GHOST": Position(
                            code="sz.GHOST", total_qty=100, available_qty=100,
                            frozen_qty=0, avg_cost=5.0, market_value=500.0,
                        ),
                    },
                )
            assert "sz.GHOST" in str(ei.value)
    finally:
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()
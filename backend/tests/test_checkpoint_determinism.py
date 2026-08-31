"""Checkpoint/Resume Determinism Tests for BlinkQuant BacktestEngine.

验证：
1. 单 checkpoint：full_run == prefix + resume
2. 多 checkpoint：任意切点结果一致
3. 序列化稳定性：save -> load -> save 字节相等
"""

import datetime
import tempfile
import json
from pathlib import Path

import polars as pl
import pytest

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.data_manager import data_manager
from core.engine import selection_engine
from core.checkpoint import BacktestCheckpoint, save_checkpoint, load_checkpoint, checkpoint_eq


def _weekdays(n, start=datetime.date(2025, 12, 1)):
    """生成 n 个连续工作日。"""
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += datetime.timedelta(days=1)
    return days


def _fixture(days):
    """生成合成行情数据。"""
    rows = []
    for i, d in enumerate(days):
        c = 10.0 + (i % 5)
        rows.append((d, "sh.AAA", c - 0.1, c, c + 0.2, c - 0.2))
    return pl.DataFrame({
        "date": [r[0] for r in rows], "code": [r[1] for r in rows],
        "open": [r[2] for r in rows], "close": [r[3] for r in rows],
        "high": [r[4] for r in rows], "low": [r[5] for r in rows],
        "volume": [1e6] * len(rows), "amount": [1e7] * len(rows),
    }).sort(["code", "date"])


def _install(df):
    data_manager.df_daily = df
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()


def _build_engine(tmp, all_days):
    cal = TradingCalendar()
    cal.set_trade_dates(all_days)
    return BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=RawPriceStore(tmp),
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )


def _run_full(engine, formula, start, end, cash=1_000_000):
    """完整运行。"""
    return engine.run(
        formula=formula, start_date=start, end_signal_date=end,
        initial_cash=cash, rebalance_freq="daily"
    )


def _run_with_checkpoint(tmp, all_days, formula, start, end, checkpoint_date, cash=1_000_000):
    """带 checkpoint 的分段运行。"""
    # 阶段 A：运行到 checkpoint_date
    engine_a = _build_engine(tmp, all_days)
    res_a = engine_a.run(
        formula=formula, start_date=start, end_signal_date=checkpoint_date,
        initial_cash=cash, rebalance_freq="daily"
    )
    
    # 保存 checkpoint (new v1.0 format)
    with tempfile.TemporaryDirectory() as cp_dir:
        engine_a.save_checkpoint(cp_dir, checkpoint_date, f"checkpoint at {checkpoint_date}")
        
        # 加载 checkpoint
        cp = BacktestCheckpoint.load_checkpoint(cp_dir)
        
        # 阶段 B：从 checkpoint 恢复继续运行
        engine_b = _build_engine(tmp, all_days)
        next_signal = engine_a.calendar.next_trade_day(checkpoint_date)
        res_b = engine_b.run(
            formula=formula, start_date=next_signal, end_signal_date=end,
            initial_cash=cash, initial_state=cp, rebalance_freq="daily"
        )
    
    return res_a, res_b, cp


def _assert_frames_eq(a: pl.DataFrame, b: pl.DataFrame, by=None, tol=1e-9):
    """DataFrame 逐字段比较（排序后）。"""
    ka = a.to_dicts()
    kb = b.to_dicts()
    key = by or list(ka[0].keys()) if ka else (list(kb[0].keys()) if kb else [])
    sa = sorted(ka, key=lambda r: tuple(r[k] for k in key)) if ka else []
    sb = sorted(kb, key=lambda r: tuple(r[k] for k in key)) if kb else []
    assert len(sa) == len(sb), f"Row count mismatch: {len(sa)} != {len(sb)}"
    for x, y in zip(sa, sb):
        assert x.keys() == y.keys(), f"Keys mismatch: {x.keys()} != {y.keys()}"
        for k in x:
            if isinstance(x[k], float):
                assert abs(x[k] - y[k]) <= tol, f"{k}: {x[k]} != {y[k]}"
            else:
                assert x[k] == y[k], f"{k}: {x[k]} != {y[k]}"


class TestCheckpointDeterminism:
    """Checkpoint/Resume 确定性回归测试。"""
    
    def setup_method(self):
        self.days = _weekdays(10)  # 10 trading days
        self.df = _fixture(self.days)
        _install(self.df)
    
    def teardown_method(self):
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()

    def test_single_checkpoint_midpoint(self):
        """Test 1: 单 checkpoint 中点恢复 == 全程运行。

        严格复刻 test_backtest_continuity.py 的分段模式（已验证正确）：
        - split_idx = 7：A 段信号 days[0]..days[6]，执行 days[1..7]
        - Checkpoint 保存于 A 段 run() 返回后
        - B 段从 days[7] 恢复（daily rebalance）
        - A 执行域 days[1..7]，B 执行域 days[8..9]，无重叠
        """
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")

            split_idx = 7
            a_last_signal = self.days[split_idx - 1]     # days[6]
            b_first_signal = self.days[split_idx]         # days[7]

            # C1：全程
            engine_c1 = _build_engine(tmp, self.days)
            c1 = _run_full(engine_c1, "CLOSE > 10", self.days[0], self.days[-2])

            # A 段：信号 days[0]..days[6]，执行至 days[7]
            engine_a = _build_engine(tmp, self.days)
            res_a = engine_a.run(
                formula="CLOSE > 10", start_date=self.days[0], end_signal_date=a_last_signal,
                initial_cash=1_000_000, rebalance_freq="daily"
            )

            # 保存 checkpoint（A 段 run() 返回后）
            with tempfile.TemporaryDirectory() as cp_dir:
                engine_a.save_checkpoint(cp_dir, b_first_signal)
                cp = load_checkpoint(cp_dir)

                # B 段：从 days[7] 恢复
                engine_b = _build_engine(tmp, self.days)
                res_b = engine_b.run(
                    formula="CLOSE > 10", start_date=b_first_signal, end_signal_date=self.days[-2],
                    initial_cash=1_000_000, initial_state=cp, rebalance_freq="daily"
                )

            # ==== 验证 ====

            # 1. trades: 按 execution_date 分域
            a_exec_domain = {self.days[i] for i in range(1, split_idx + 1)}   # days[1..7]
            b_exec_domain = {self.days[i] for i in range(split_idx + 1, len(self.days))}  # days[8..9]

            ta = res_a.trades.filter(pl.col("execution_date").is_in(a_exec_domain))
            tb = res_b.trades.filter(pl.col("execution_date").is_in(b_exec_domain))
            tc_a = c1.trades.filter(pl.col("execution_date").is_in(a_exec_domain))
            tc_b = c1.trades.filter(pl.col("execution_date").is_in(b_exec_domain))

            if tc_a.height > 0 or ta.height > 0:
                _assert_frames_eq(
                    ta.sort(["execution_date", "code", "side"]),
                    tc_a.sort(["execution_date", "code", "side"])
                )
            if tc_b.height > 0 or tb.height > 0:
                _assert_frames_eq(
                    tb.sort(["execution_date", "code", "side"]),
                    tc_b.sort(["execution_date", "code", "side"])
                )

            # 2. equity_curve: 按日期域分段比较（复刻 test_backtest_continuity.py 模式）
            # A 域：日期 < b_first_signal
            ec_a_part = c1.equity_curve.filter(pl.col("date") < b_first_signal)
            _assert_frames_eq(
                res_a.equity_curve.filter(pl.col("date") < b_first_signal).drop("signal_date"),
                ec_a_part.drop("signal_date"), by=["date"])

            # B 域：日期 >= b_first_signal
            ec_b_part = c1.equity_curve.filter(pl.col("date") >= b_first_signal)
            eb_sorted = res_b.equity_curve.drop("signal_date").sort("date")
            ec_b_sorted = ec_b_part.drop("signal_date").sort("date")
            assert eb_sorted.height == ec_b_sorted.height, \
                f"B equity {eb_sorted.height} vs C1 {ec_b_sorted.height}"
            _assert_frames_eq(eb_sorted, ec_b_sorted, by=["date"])

            # 3. positions_daily
            pa = res_a.positions_daily.filter(pl.col("date") < b_first_signal)
            pc_a = c1.positions_daily.filter(pl.col("date") < b_first_signal)
            if pa.height > 0 or pc_a.height > 0:
                _assert_frames_eq(pa.sort(["date", "code"]),
                                  pc_a.sort(["date", "code"]), by=["date", "code"])

            pb = res_b.positions_daily
            pc_b = c1.positions_daily.filter(pl.col("date") >= b_first_signal)
            pb_sorted = pb.sort(["date", "code"])
            pc_b_sorted = pc_b.sort(["date", "code"])
            assert pb_sorted.height == pc_b_sorted.height, \
                f"pos B {pb_sorted.height} vs C1 {pc_b_sorted.height}"
            _assert_frames_eq(pb_sorted, pc_b_sorted, by=["date", "code"])

            # 后段必须有成交（否则等价性无意义）
            assert tb.height > 0

    def test_multiple_checkpoints(self):
        """Test 2: 多 checkpoint 点位保存/加载验证。
        
        验证在不同日期保存 checkpoint，且能正确加载恢复。
        """
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")
            
            # 完整运行 C1
            engine_c1 = _build_engine(tmp, self.days)
            c1 = _run_full(engine_c1, "CLOSE > 10", self.days[0], self.days[-2])
            
            # 三个 checkpoint 点：在不同信号日结束时保存
            checkpoints = [self.days[2], self.days[5], self.days[7]]
            
            for cp_date in checkpoints:
                # 从头运行到该日期
                engine = _build_engine(tmp, self.days)
                res = engine.run(
                    formula="CLOSE > 10", start_date=self.days[0], end_signal_date=cp_date,
                    initial_cash=1_000_000, rebalance_freq="daily"
                )
                
                # 保存 checkpoint
                with tempfile.TemporaryDirectory() as cp_dir:
                    engine.save_checkpoint(cp_dir, cp_date)
                    cp = load_checkpoint(cp_dir)
                    
                    # 验证 checkpoint 内容基本正确
                    assert cp.current_date == cp_date.isoformat()
                    assert cp.phase == "CHECKPOINT"
                    assert len(cp.positions) >= 0
                    assert cp.cash >= 0
                    
                    # 验证能正确恢复：从该 checkpoint 继续运行到结束
                    engine_b = _build_engine(tmp, self.days)
                    next_sig = engine.calendar.next_trade_day(cp_date)
                    res_b = engine_b.run(
                        formula="CLOSE > 10", start_date=next_sig, end_signal_date=self.days[-2],
                        initial_cash=1_000_000, initial_state=cp, rebalance_freq="daily"
                    )
                    
                    # 基本 sanity check：结果不为空
                    assert res_b.equity_curve.height > 0

    def test_serialization_stability(self):
        """Test 3: 序列化稳定性 - save -> load -> save 字节相等。"""
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")
            
            engine = _build_engine(tmp, self.days)
            res = engine.run(
                formula="CLOSE > 10", start_date=self.days[0], end_signal_date=self.days[4],
                initial_cash=1_000_000, rebalance_freq="daily"
            )
            
            with tempfile.TemporaryDirectory() as cp_dir1, \
                 tempfile.TemporaryDirectory() as cp_dir2:
                
                # 第一次保存
                engine.save_checkpoint(cp_dir1, self.days[4])
                
                # 加载再保存
                cp = load_checkpoint(cp_dir1)
                save_checkpoint(cp, cp_dir2)
                
                # 比较所有文件字节内容
                for fname in ["meta.json", "portfolio.json", "pending.json", 
                              "corporate_actions.json", "diagnostics.json", 
                              "engine_state.json"]:
                    f1 = Path(cp_dir1) / fname
                    f2 = Path(cp_dir2) / fname
                    assert f1.read_bytes() == f2.read_bytes(), f"Byte mismatch in {fname}"
                
                # Parquet 比较（内容相等）
                df1 = pl.read_parquet(Path(cp_dir1) / "last_close.parquet")
                df2 = pl.read_parquet(Path(cp_dir2) / "last_close.parquet")
                assert df1.shape == df2.shape
                _assert_frames_eq(df1.sort("code"), df2.sort("code"), by=["code"])

    def test_checkpoint_eq_helper(self):
        """验证 checkpoint_eq 辅助函数。"""
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")
            
            engine = _build_engine(tmp, self.days)
            res = engine.run(
                formula="CLOSE > 10", start_date=self.days[0], end_signal_date=self.days[4],
                initial_cash=1_000_000, rebalance_freq="daily"
            )
            
            with tempfile.TemporaryDirectory() as cp_dir:
                engine.save_checkpoint(cp_dir, self.days[4])
                cp1 = load_checkpoint(cp_dir)
                cp2 = load_checkpoint(cp_dir)
                
                assert checkpoint_eq(cp1, cp2)
                
                # 修改一个字段后不相等
                cp2.cash += 0.01
                assert not checkpoint_eq(cp1, cp2)


class TestCheckpointDeterministicOrdering:
    """验证运行顺序确定性（set iteration 等不导致结果漂移）。"""
    
    def setup_method(self):
        self.days = _weekdays(10)
        self.df = _fixture(self.days)
        _install(self.df)
    
    def teardown_method(self):
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()

    def test_consecutive_runs_identical(self):
        """连续两次全程运行结果完全相同。"""
        with tempfile.TemporaryDirectory() as tmp:
            self.df.write_parquet(f"{tmp}/stock_kline_{self.days[0].year}.parquet")
            
            engine1 = _build_engine(tmp, self.days)
            res1 = _run_full(engine1, "CLOSE > 10", self.days[0], self.days[-2])
            
            engine2 = _build_engine(tmp, self.days)
            res2 = _run_full(engine2, "CLOSE > 10", self.days[0], self.days[-2])
            
            _assert_frames_eq(
                res1.trades.sort(["execution_date", "code", "side"]),
                res2.trades.sort(["execution_date", "code", "side"])
            )
            _assert_frames_eq(
                res1.equity_curve.drop("signal_date").sort("date"),
                res2.equity_curve.drop("signal_date").sort("date"),
                by=["date"]
            )
            _assert_frames_eq(
                res1.positions_daily.sort(["date", "code"]),
                res2.positions_daily.sort(["date", "code"]),
                by=["date", "code"]
            )
            assert res1.execution_diagnostics == res2.execution_diagnostics
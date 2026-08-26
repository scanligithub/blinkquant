"""Layer 3：全链路 No-Lookahead 契约测试。

边界定义（signal boundary 与 settlement boundary 严格分开）：

L3-1  Signal Poisoning
      投毒 date > signal_date 的 raw 行情
      → SelectionResult(T) 与意图的 signal 派生部分（code/side/target_weight）不变；
        qty/price 属执行域（由 T+1 open 定尺），允许变化——这正是"execution sensitivity"
        与 "signal no-lookahead" 的分界。若要求 qty 也不变，等于要求按信号日信息定尺，
        属于另一设计取舍，不在本契约内。

L3-2  Settlement Poisoning
      投毒 date > valuation_date(V) 的行情
      → 截至 V 的全部已结算状态逐字段不变：
        trades(execution_date<=V 含 price/fee/qty)、cash、positions、equity(V)。

L3-3  Truncation Equivalence
      截断数据集后的完整回测 == 全量回测的前缀（prefix-deterministic，
      为 walk-forward / 分段回测 / 增量回测背书）。

L3-4  Suspension Carry-forward Poisoning
      停牌期间估值沿用最后可用价；未来复牌价（甚至整段删除复牌行）
      不得影响停牌期历史 valuation。

附加不变量（引擎每 cycle 已强制，测试再显式断言一次）：
      cash >= -eps；equity == cash + positions_value。
"""
import datetime
import tempfile

import polars as pl
import pytest

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.portfolio import Position
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.data_manager import data_manager
from core.engine import selection_engine

EPS = 1e-6


# ---------------------------------------------------------------- fixtures ----

def _weekdays(n: int, start=datetime.date(2026, 2, 2)):
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += datetime.timedelta(days=1)
    return days


def _ohlcv_frame(rows):
    return pl.DataFrame({
        "date": [r[0] for r in rows],
        "code": [r[1] for r in rows],
        "open": [r[2] for r in rows],
        "close": [r[3] for r in rows],
        "high": [r[4] for r in rows],
        "low": [r[5] for r in rows],
        "volume": [1_000_000.0] * len(rows),
        "amount": [10_000_000.0] * len(rows),
    }).sort(["code", "date"])


def _install_dm(df: pl.DataFrame):
    data_manager.df_daily = df
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()


def _teardown_dm():
    data_manager.df_daily = None
    data_manager.df_weekly = None
    data_manager.df_monthly = None
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()


def _write_raw(tmpdir: str, df: pl.DataFrame):
    df.write_parquet(f"{tmpdir}/stock_kline_{df['date'].min().year}.parquet")


def _make_engine(tmpdir: str, dates):
    cal = TradingCalendar()
    cal.set_trade_dates(dates)
    return BacktestEngine(
        calendar=cal,
        selection_engine=selection_engine,
        raw_price_store=RawPriceStore(data_root=tmpdir),   # 每跑新建 → 绕过计划缓存
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )


def _run(tmpdir, dates, formula="CLOSE > 15", start=None, end=None, initial_positions=None):
    eng = _make_engine(tmpdir, dates)
    return eng.run(
        formula=formula,
        start_date=start or dates[0],
        end_signal_date=end or dates[-1],
        initial_cash=1_000_000,
        initial_positions=initial_positions,
    )


def _poison_after(df: pl.DataFrame, cutoff: datetime.date, factor: float = 100.0):
    """严格晚于 cutoff 的所有 OHLC × factor。"""
    return df.with_columns([
        pl.when(pl.col("date") > cutoff)
        .then(pl.col(c) * factor)
        .otherwise(pl.col(c)).alias(c)
        for c in ("open", "high", "low", "close")
    ])


def _assert_invariants(result):
    ec = result.equity_curve.to_dicts()
    for row in ec:
        assert row["cash"] >= -EPS, f"现金为负 @ {row['date']}"
        assert abs(row["equity"] - (row["cash"] + row["positions_value"])) < 1e-4


def _sorted_rows(df: pl.DataFrame, by):
    return sorted(df.to_dicts(), key=lambda r: tuple(r[k] for k in by))


def _assert_frames_equal(a: pl.DataFrame, b: pl.DataFrame, by, tol=1e-9):
    ra, rb = _sorted_rows(a, by), _sorted_rows(b, by)
    assert len(ra) == len(rb), f"row count {len(ra)} != {len(rb)}"
    for x, y in zip(ra, rb):
        for k in x:
            if isinstance(x[k], float):
                assert abs(x[k] - y[k]) <= tol, f"{k}: {x[k]} != {y[k]}"
            else:
                assert x[k] == y[k], f"{k}: {x[k]} != {y[k]}"


# 共享基础行情：AAA 在 t1/t3/t5 入选（close>15），t2/t4/t6 落选；BBB 永不入选
DAYS = _weekdays(8)                       # t1..t8（均为工作日）
T1, T2, T3, T4, T5, T6, T7, T8 = DAYS
AAA_CLOSE = [16.0, 14.0, 17.0, 13.0, 18.0, 12.0, 19.0, 11.0]


def _base_rows():
    rows = []
    for i, d in enumerate(DAYS):
        c = AAA_CLOSE[i]
        rows.append((d, "sh.AAA", c - 0.1, c, c + 0.2, c - 0.2))
        rows.append((d, "sz.BBB", 8.9, 9.0, 9.2, 8.8))
    return _ohlcv_frame(rows)


# ------------------------------------------------------------------ L3-1 ----

def test_L3_1_signal_poisoning_decision_layer_invariant():
    """投毒 date>T1：Selection(T1) 与意图决策层(code/side/weight) 不变；qty 允许变。"""
    base = _base_rows()
    poisoned = _poison_after(base, T1)     # 严格晚于 signal_date=T1

    _install_dm(base)                      # 选股世界两次运行完全相同（不被投毒）
    try:
        with tempfile.TemporaryDirectory() as tmp:
            # --- Baseline ---
            _write_raw(tmp, base)
            res_a = _run(tmp, DAYS[:4], start=T1, end=T3)          # exec T2..T4
            sel_a = selection_engine.execute_selector("CLOSE > 15", "D", None, target_date=T1)

            # --- Poisoned raw world（df_daily 保持 baseline）---
            import os
            for f in os.listdir(tmp):
                os.remove(os.path.join(tmp, f))
            _write_raw(tmp, poisoned)
            res_b = _run(tmp, DAYS[:4], start=T1, end=T3)
            sel_b = selection_engine.execute_selector("CLOSE > 15", "D", None, target_date=T1)

            # ① SelectionResult 不变
            assert sel_a.codes == sel_b.codes == ["sh.AAA"]
            assert sel_a.signal_date == sel_b.signal_date == T1

            # ② 意图决策层不变：每个 signal_date 的 (code,side) 集合一致
            da = {(r["code"], r["side"]) for _, r in
                  [(t["signal_date"], t) for t in res_a.trades.to_dicts()]}
            db = {(r["code"], r["side"]) for _, r in
                  [(t["signal_date"], t) for t in res_b.trades.to_dicts()]}
            assert da == db, f"方向集合漂移: {da} vs {db}"
            assert ("sh.AAA", "BUY") in da

            # ③ 执行域允许变化：T2 开盘被 ×100 → 首笔 BUY qty 必然不同（锁死边界语义）
            q_a = (res_a.trades.filter(pl.col("side") == "BUY")
                   .sort("execution_date")["qty"][0])
            q_b = (res_b.trades.filter(pl.col("side") == "BUY")
                   .sort("execution_date")["qty"][0])
            assert q_a != q_b, "预期 execution 敏感：T+1 open 投毒应改变定尺数量"

            # ④ 信号日之前的成交不存在（无时间倒挂）
            assert res_a.trades.filter(pl.col("execution_date") <= T1).height == 0

            _assert_invariants(res_a)
            _assert_invariants(res_b)
    finally:
        _teardown_dm()


# ------------------------------------------------------------------ L3-2 ----

def test_L3_2_settlement_poisoning_full_state_invariant():
    """投毒 date>V(T4)：截至 V 的 trades/cash/positions/equity 逐字段不变。"""
    base = _base_rows()
    V = T4
    poisoned = _poison_after(base, V)

    _install_dm(base)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            _write_raw(tmp, base)
            res_a = _run(tmp, DAYS[:7], start=T1, end=T6)          # exec T2..T7

            import os
            for f in os.listdir(tmp):
                os.remove(os.path.join(tmp, f))
            _write_raw(tmp, poisoned)
            res_b = _run(tmp, DAYS[:7], start=T1, end=T6)

            # 已结算 trades（exec<=V）：全字段含 price/fee/qty 完全一致
            ta = res_a.trades.filter(pl.col("execution_date") <= V)
            tb = res_b.trades.filter(pl.col("execution_date") <= V)
            assert ta.height > 0
            _assert_frames_equal(ta, tb, by=("execution_date", "code", "side"))

            # V 日估值行：cash / positions_value / equity 一致
            ea = res_a.equity_curve.filter(pl.col("date") == V)
            eb = res_b.equity_curve.filter(pl.col("date") == V)
            assert ea.height == 1 and eb.height == 1
            _assert_frames_equal(ea, eb, by=("date",))

            # V 日持仓快照一致
            pa = res_a.positions_daily.filter(pl.col("date") == V)
            pb = res_b.positions_daily.filter(pl.col("date") == V)
            _assert_frames_equal(pa, pb, by=("code",))

            _assert_invariants(res_a)
            _assert_invariants(res_b)
    finally:
        _teardown_dm()


# ------------------------------------------------------------------ L3-3 ----

def test_L3_3_truncation_equivalence_prefix_deterministic():
    """截断到 T7 的完整回测 == 全量(至 T8)回测中 exec<=T7 的前缀。"""
    full = _base_rows()                    # t1..t8
    truncated = full.filter(pl.col("date") <= T7)

    _install_dm(full)
    try:
        with tempfile.TemporaryDirectory() as tmp_full, \
             tempfile.TemporaryDirectory() as tmp_trunc:
            _write_raw(tmp_full, full)
            _write_raw(tmp_trunc, truncated)

            # Run A：全量数据，信号 t1..t6 → exec t2..t7（不触及 t8）
            res_a = _run(tmp_full, DAYS, start=T1, end=T6)
            # Run B：截断数据，同一信号区间
            res_b = _run(tmp_trunc, DAYS[:7], start=T1, end=T6)

            _assert_frames_equal(res_a.equity_curve, res_b.equity_curve,
                                 by=("date",), tol=1e-9)
            _assert_frames_equal(res_a.trades, res_b.trades,
                                 by=("execution_date", "code", "side"), tol=1e-9)
            _assert_frames_equal(res_a.positions_daily, res_b.positions_daily,
                                 by=("date", "code"), tol=1e-9)
            assert res_a.metrics["total_days"] == res_b.metrics["total_days"]

            _assert_invariants(res_a)
            _assert_invariants(res_b)
    finally:
        _teardown_dm()


# ------------------------------------------------------------------ L3-4 ----

def test_L3_4_suspension_carryforward_future_price_no_backward_leak():
    """停牌期估值沿 d0 close=5；删除未来复牌行（比投毒更严酷）→ 历史 valuation 不变。"""
    d_last_trade, s1, s2, s3, resume = DAYS[0], DAYS[1], DAYS[2], DAYS[3], DAYS[4]
    rows = [
        (d_last_trade, "sh.MKT", 10.0, 10.0, 10.2, 9.8),
        (s1, "sh.MKT", 10.0, 10.0, 10.2, 9.8),
        (s2, "sh.MKT", 10.0, 10.0, 10.2, 9.8),
        (d_last_trade, "sz.SUSP", 4.9, 5.0, 5.2, 4.8),          # 最后成交日
        (resume, "sz.SUSP", 20.0, 20.0, 20.5, 19.5),            # 复牌跳空
    ]
    base = _ohlcv_frame(rows)

    _install_dm(base)                 # dm 世界保持不变（limit flags 判 SUSP 停牌→禁交易）
    try:
        with tempfile.TemporaryDirectory() as tmp:
            init_pos = {
                "sz.SUSP": Position(code="sz.SUSP", total_qty=200, available_qty=200,
                                    frozen_qty=0, avg_cost=5.0, market_value=1000.0),
            }

            def run_with(frame):
                import os
                for f in os.listdir(tmp):
                    os.remove(os.path.join(tmp, f))
                _write_raw(tmp, frame)
                eng = _make_engine(tmp, DAYS[:5])
                return eng.run(
                    formula="CLOSE > 1000000",           # 无新信号
                    start_date=d_last_trade,
                    end_signal_date=s2,                  # 覆盖停牌期估值 s1,s2
                    initial_cash=500_000,
                    initial_positions=init_pos,
                )

            res_base = run_with(base)

            # 比删除更狠：复牌价改成天价 —— 历史停牌估值仍不得变
            future_poisoned = base.with_columns(
                pl.when((pl.col("code") == "sz.SUSP") & (pl.col("date") >= resume))
                .then(pl.col("close") * 100).otherwise(pl.col("close")).alias("close")
            )
            res_pois = run_with(future_poisoned)

            # 停牌期(s1,s2 为其 execution/valuation 日)两行必须逐字段一致
            hist_dates = [s1, s2]
            ha = res_base.equity_curve.filter(pl.col("date").is_in(hist_dates)).sort("date")
            hb = res_pois.equity_curve.filter(pl.col("date").is_in(hist_dates)).sort("date")
            _assert_frames_equal(ha, hb, by=("date",), tol=1e-9)

            # 且估值确实用的是 carry-forward 的 5.0（而非复牌价 20/2000）
            for row in ha.to_dicts():
                assert abs(row["positions_value"] - 200 * 5.0) < EPS
                assert abs(row["equity"] - (row["cash"] + 1000.0)) < EPS

            _assert_invariants(res_base)
            _assert_invariants(res_pois)
    finally:
        _teardown_dm()
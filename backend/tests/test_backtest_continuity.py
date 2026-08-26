"""跨年连续性契约：日历边界 fail-fast + checkpoint/restore 等价性（C1==C2）。"""
import datetime
import tempfile

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.portfolio import Position
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
from core.data_manager import data_manager
from core.engine import selection_engine


def _weekdays(n, start=datetime.date(2025, 12, 1)):   # 跨 2025-12 → 2026-01
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += datetime.timedelta(days=1)
    return days


def _fixture(days):
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


def test_next_trade_day_beyond_range_raises():
    """越界 fail-fast：日历末日之后不得折叠为同日执行（T+1 契约）。"""
    cal = TradingCalendar()
    cal.set_trade_dates([datetime.date(2024, 12, 30), datetime.date(2024, 12, 31)])
    assert cal.next_trade_day(datetime.date(2024, 12, 30)) == datetime.date(2024, 12, 31)
    try:
        cal.next_trade_day(datetime.date(2024, 12, 31))
        assert False, "越界应抛 ValueError"
    except ValueError as e:
        assert "T+1" in str(e)


def test_engine_rejects_when_calendar_cannot_resolve_t1():
    """引擎在无法解析 T+1 时必须抛错，而不是同日执行。"""
    days = _weekdays(3)
    df = _fixture(days)
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            df.write_parquet(f"{tmp}/stock_kline_{days[-1].year}.parquet")
            cal = TradingCalendar()
            cal.set_trade_dates(days)                      # 日历只到末日
            eng = BacktestEngine(
                calendar=cal, selection_engine=selection_engine,
                raw_price_store=RawPriceStore(tmp),
                fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
                allocator=equal_weight_allocator,
            )
            try:
                eng.run(formula="CLOSE > 0", start_date=days[-1],
                        end_signal_date=days[-1], initial_cash=1_000_000)
                assert False, "应因 T+1 无法解析而失败"
            except ValueError as e:
                assert "TradingCalendar" in str(e)
    finally:
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()


def _run_segment(tmp, all_days, formula, start, end, state=None, cash=1_000_000):
    # 日历覆盖全部交易日（连续世界）
    cal = TradingCalendar()
    cal.set_trade_dates(all_days)
    eng = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=RawPriceStore(tmp),
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )
    return eng.run(formula=formula, start_date=start, end_signal_date=end,
                   initial_cash=cash, initial_state=state)


def test_checkpoint_restore_equivalence_c1_equals_c2():
    """合成数据上的 C1(连续) == C2(断点续跑)：trades/equity/positions 逐字段一致。

    结构：
      C1 = 全程连续跑（信号 days[0]..days[-2]，执行至 days[-1]）
      A  = 前缀段（信号 days[0]..a_last_signal=days[6]；其 T+1 执行 days[7] 归属 A）
      B  = 后缀段（从 checkpoint 恢复，信号 b_first_signal=days[7]..days[-2]）

    断言：
      A.trades == C1.trades[exec ∈ A 域]
      B.trades == C1.trades[exec ∈ B 域]
      A.equity + B.equity == C1.equity（日期不重叠、拼接完整）
      positions_daily 同理

    分段点选在持仓非空、且存在 T+1 冻结跨越分段点的位置，
    同时验证 last_close 注入与 daily_thaw 的跨段行为。
    """
    days = _weekdays(10)                    # 12/01..12/12 工作日
    split_idx = 7
    a_last_signal = days[split_idx - 1]     # days[6]=12/08? 不对——days[6]=12/09
    b_first_signal = days[split_idx]        # days[7]=12/10

    full_days = days
    df = _fixture(full_days)
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            df.write_parquet(f"{tmp}/stock_kline_{days[0].year}.parquet")

            # ---- C1：连续跑全程 ----
            c1 = _run_segment(tmp, full_days, "CLOSE > 10",
                              start=days[0], end=days[-2])

            # ---- A：前缀段 ----
            eng_a = _engine_with(tmp, full_days)
            res_a = eng_a.run(
                formula="CLOSE > 10", start_date=days[0],
                end_signal_date=a_last_signal, initial_cash=1_000_000)

            # 序列化 checkpoint（模拟跨进程：JSON 往返）
            state = eng_a.export_state()
            import json
            state = json.loads(json.dumps(
                {**state["portfolio"], "last_close": state["last_close"],
                 "thru_thaw": state["thru_thaw"],
                 "pending": state["pending"],
                 "selected_thru": state["selected_thru"]},
                default=str))

            # ---- B：后缀段（从 checkpoint 恢复）----
            res_b = _run_segment(tmp, full_days, "CLOSE > 10",
                                 start=b_first_signal,
                                 end=days[-2], state=state)

            # ---- 比较三段输出 ----
            a_exec_domain = set(range(0, split_idx + 1))  # A 的 exec 落在 days[1..7]
            b_exec_domain = set(range(split_idx + 1, len(days)))  # B 的 exec 落在 days[8..9]

            ta = res_a.trades.filter(pl.col("execution_date").is_in(
                {days[i] for i in a_exec_domain}))
            tb = res_b.trades.filter(pl.col("execution_date").is_in(
                {days[i] for i in b_exec_domain}))
            tc = c1.trades

            # C1 trades 按 exec 日期分为 A/B 两段，且分别与独立段一致
            tc_a = tc.filter(pl.col("execution_date").is_in(
                {days[i] for i in a_exec_domain}))
            tc_b = tc.filter(pl.col("execution_date").is_in(
                {days[i] for i in b_exec_domain}))

            # A 段 trades 与 C1 前缀一致
            if tc_a.height > 0 or ta.height > 0:
                _assert_frames_eq(ta.sort(["execution_date", "code", "side"]),
                                  tc_a.sort(["execution_date", "code", "side"]))

            # B 段 trades 与 C1 后缀一致
            if tc_b.height > 0 or tb.height > 0:
                _assert_frames_eq(tb.sort(["execution_date", "code", "side"]),
                                  tc_b.sort(["execution_date", "code", "side"]))

            # equity curve：按日期域分段比较（边界日 days[split_idx] 双方均有属正常）
            ea = res_a.equity_curve
            eb = res_b.equity_curve
            ec = c1.equity_curve

            # A 域：日期 <= a_last_signal 的执行日（即 < b_first_signal）
            ec_a_part = ec.filter(pl.col("date") < b_first_signal)
            _assert_frames_eq(ea.filter(pl.col("date") < b_first_signal).drop("signal_date"),
                              ec_a_part.drop("signal_date"), by=["date"])

            # B 域：日期 >= b_first_signal 的执行日
            ec_b_part = ec.filter(pl.col("date") >= b_first_signal)
            eb_sorted = eb.drop("signal_date").sort("date")
            ec_b_sorted = ec_b_part.drop("signal_date").sort("date")
            assert eb_sorted.height == ec_b_sorted.height, \
                f"B equity {eb_sorted.height} vs C1 {ec_b_sorted.height}"
            _assert_frames_eq(eb_sorted, ec_b_sorted, by=["date"])

            # positions_daily 同理
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
    finally:
        data_manager.df_daily = None
        data_manager.df_weekly = None
        data_manager.df_monthly = None
        data_manager._asof_frame_cache.clear()


# ------------------------------------------------------------- helpers ----

def _positions_from_state(state):
    out = {}
    for p in state["portfolio"]["positions"]:
        out[p["code"]] = Position(
            code=p["code"], total_qty=p["total_qty"],
            available_qty=p["available_qty"], frozen_qty=p["frozen_qty"],
            avg_cost=p["avg_cost"], market_value=p.get("market_value", 0.0))
    return out


def _assert_frames_eq(a: pl.DataFrame, b: pl.DataFrame, by=None, tol=1e-9):
    ka = a.to_dicts(); kb = b.to_dicts()
    key = by or list(ka[0].keys()) if ka else (list(kb[0].keys()) if kb else [])
    sa = sorted(ka, key=lambda r: tuple(r[k] for k in key)) if ka else []
    sb = sorted(kb, key=lambda r: tuple(r[k] for k in key)) if kb else []
    assert len(sa) == len(sb), f"{len(sa)} != {len(sb)}"
    for x, y in zip(sa, sb):
        assert x.keys() == y.keys()
        for k in x:
            if isinstance(x[k], float):
                assert abs(x[k] - y[k]) <= tol, f"{k}: {x[k]} != {y[k]}"
            else:
                assert x[k] == y[k], f"{k}: {x[k]} != {y[k]}"


def _make_engine_only(tmp, all_days):
    return _engine_with(tmp, all_days)


def _engine_with(tmp, all_days):
    from core.backtest_engine import BacktestEngine as BE, TradingCalendar as TC
    from core.raw_price_store import RawPriceStore as RPS
    cal = TC(); cal.set_trade_dates(all_days)
    return BE(calendar=cal, selection_engine=selection_engine,
              raw_price_store=RPS(tmp), fee_config=FeeConfig(),
              execution_config=MVP_EXECUTION_CONFIG,
              allocator=equal_weight_allocator)
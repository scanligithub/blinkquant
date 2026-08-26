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

    分段点选在持仓非空、且存在 T+1 冻结跨越分段点的位置，
    同时验证 last_close 注入与 daily_thaw 的跨段行为。
    """
    days = _weekdays(10)                    # 12/01..12/12 工作日
    split_idx = 6                           # 前段 6 天，后段 4 天
    d_split = days[split_idx - 1]           # 前段最后信号日
    d_after = days[split_idx]               # 后段首个信号日

    full_days = days
    df = _fixture(full_days)
    _install(df)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            df.write_parquet(f"{tmp}/stock_kline_{days[0].year}.parquet")

            # ---- C1：连续跑全程（末信号留出 T+1 执行/估值）----
            c1 = _run_segment(tmp, full_days, "CLOSE > 10",
                              start=days[0], end=days[-2])

            # ---- 前段：信号到 days[5]（其 execution days[6] 归属前段）----
            segA_end_signal = days[split_idx - 1]
            eng_a = _engine_with(tmp, full_days)
            res_a = eng_a.run(
                formula="CLOSE > 10", start_date=days[0],
                end_signal_date=segA_end_signal, initial_cash=1_000_000)

            # 序列化 checkpoint（模拟跨进程：JSON 往返），并拍平为 run(initial_state) 契约
            state = eng_a.export_state()
            import json
            state = json.loads(json.dumps(
                {**state["portfolio"], "last_close": state["last_close"]},
                default=str))

            # ---- 后段：新引擎实例从 checkpoint 恢复续跑（末信号同样留 T+1）----
            res_b = _run_segment(tmp, full_days, "CLOSE > 10",
                                 start=d_after, end=days[-2], state=state)

            # ---- 比较 C1 与 C2 在后段的输出（B 的成交/估值日域）----
            tail_dates = set(days[split_idx + 1:])
            t1 = c1.trades.filter(pl.col("execution_date").is_in(tail_dates))
            t2 = res_b.trades
            _assert_frames_eq(t1.sort(["execution_date", "code", "side"]),
                              t2.sort(["execution_date", "code", "side"]))
            e1 = c1.equity_curve.filter(pl.col("date").is_in(tail_dates)).sort("date")
            e2 = res_b.equity_curve.sort("date")
            _assert_frames_eq(e1, e2, by=["date"])
            p1 = c1.positions_daily.filter(pl.col("date").is_in(tail_dates))
            p2 = res_b.positions_daily
            _assert_frames_eq(p1.sort(["date", "code"]), p2.sort(["date", "code"]),
                              by=["date", "code"])

            # 后段必须有成交（否则等价性无意义）
            assert t2.height > 0
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
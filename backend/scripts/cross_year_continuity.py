"""2024→2025 跨年连续性验证（真实数据 · C1/C2 · 跨进程 checkpoint）。

路径：
  A  = 2024 单年（信号 01-02..12-31；末笔执行/估值落 2025-01-02）
  B  = 2025 单年 fresh（新开户）
  C  = 2024-01-02 → 2025-12-31 连续
  C2 = 跑 A → export_state → **子进程**恢复 → 只跑 2025 信号段

判定：
  V1  A.valuation_end == 2025-01-02            （T+1 跨年边界正确）
  V2  C[2024 部分] == A                        （前缀确定）
  V3  C[2025 部分] == C2[2025 部分]             （checkpoint 等价，核心）
  V4  C[2025 部分] != B                        （带仓入场 ≠ 新开户，允许不等）
用法：
    python scripts/cross_year_continuity.py            # 主流程
    python scripts/cross_year_continuity.py --segB st.json out.json   # 子进程
"""
import gc
import json
import os
import sys
import time
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from backtest_quality_2024_2025 import build_df_daily, log

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
D2024 = (datetime.date(2024, 1, 2), datetime.date(2024, 12, 31))
D2025 = (datetime.date(2025, 1, 2), datetime.date(2025, 12, 31))


def _setup(year=2025):
    dm = build_df_daily(year)
    dates = (dm.df_daily.select(pl.col("date")).unique()
             .sort("date").to_series().to_list())
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping

    def make():
        return BacktestEngine(
            calendar=cal, selection_engine=selection_engine,
            raw_price_store=store, fee_config=FeeConfig(),
            execution_config=MVP_EXECUTION_CONFIG,
            allocator=equal_weight_allocator,
        )
    return dm, make


def _run(make, start, end, state=None, cash=1_000_000):
    eng = make()
    r = eng.run(formula="CLOSE > MA(CLOSE, 20)", start_date=start,
                end_signal_date=end, initial_cash=cash, initial_state=state)
    return eng, r


def _slice_frames(r, lo, hi):
    dts = pl.date_range(lo, hi).to_list() if False else None
    t = r.trades.filter((pl.col("execution_date") >= lo) &
                        (pl.col("execution_date") <= hi))
    e = r.equity_curve.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))
    p = r.positions_daily.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))
    return t, e, p


def _assert_eq(a: pl.DataFrame, b: pl.DataFrame, by, tol=1e-9, tag=""):
    ka = sorted(a.to_dicts(), key=lambda r: tuple(r[k] for k in by))
    kb = sorted(b.to_dicts(), key=lambda r: tuple(r[k] for k in by))
    assert len(ka) == len(kb), f"[{tag}] rows {len(ka)} != {len(kb)}"
    for x, y in zip(ka, kb):
        for k in x:
            if isinstance(x[k], float):
                assert abs(x[k] - y[k]) <= tol, f"[{tag}] {k}: {x[k]} != {y[k]}"
            else:
                assert x[k] == y[k], f"[{tag}] {k}: {x[k]} != {y[k]}"


def main_segB(payload_path: str, out_path: str):
    """子进程：恢复 checkpoint，从边界信号（含）续跑 2025 段。"""
    payload = json.load(open(payload_path, encoding="utf-8"))
    state = payload["state"]
    seg_start = datetime.date.fromisoformat(payload["seg_start"])
    seg_end = datetime.date.fromisoformat(payload["seg_end"])
    dm, make = _setup(2025)
    try:
        _, r = _run(make, seg_start, seg_end, state=state)
        payload = {
            "trades": r.trades.to_dicts(),
            "equity": r.equity_curve.to_dicts(),
            "positions": r.positions_daily.to_dicts(),
        }
        json.dump(payload, open(out_path, "w", encoding="utf-8"),
                  default=str, ensure_ascii=False)
        log(f"segB done -> {out_path}")
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved if False else (
            None, None, None, None)


def main():
    t0 = time.time()
    # ---------- 数据一次构建（2024+2025+2026 边界） ----------
    dm, make = _setup(2025)

    # ---------- A：2024 单年 ----------
    log("run A: 2024 standalone ...")
    engA, rA = _run(make, *D2024, cash=1_000_000)
    stateA = engA.export_state()

    # ---------- C：两年连续 ----------
    log("run C: continuous 2024->2025 ...")
    engC, rC = _run(make, D2024[0], D2025[1], cash=1_000_000)

    # ---------- B：2025 fresh ----------
    log("run B: 2025 standalone fresh ...")
    engB, rB = _run(make, *D2025, cash=1_000_000)

    # ---------- C2：子进程 checkpoint 续跑（从边界信号 2024-12-31 含） ----------
    log("run C2: subprocess restore -> 2025 segment (incl boundary signal) ...")
    sp = Path(__file__).resolve()
    state_file, out_file = sp.parent / "_ckpt_tmp.json", sp.parent / "_segB_out.json"
    flat_state = {**stateA["portfolio"], "last_close": stateA["last_close"]}
    payload = {"state": flat_state,
               "seg_start": D2025[0].isoformat(),      # 下一个未消费信号（12-31 已被 A 消费）
               "seg_end": D2025[1].isoformat()}
    json.dump(payload, open(state_file, "w", encoding="utf-8"),
              default=str, ensure_ascii=False)
    import subprocess
    env = dict(os.environ)
    subprocess.run([sys.executable, str(sp), "--segB",
                    str(state_file), str(out_file)],
                   check=True, env=env, timeout=1200)
    c2 = json.load(open(out_file, encoding="utf-8"))

    (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
     dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved = (
        None, None, None, None)

    # ================= 判定 =================
    print("\n============= 跨年连续性判定 =============", flush=True)

    v1_date = rA.equity_curve["date"].max()
    v1 = (v1_date == datetime.date(2025, 1, 2))
    print(f"[V1] {'PASS' if v1 else 'FAIL'}  A.valuation_end={v1_date} "
          f"(期望 2025-01-02，末信号 2024-12-31 的 T+1)")

    # V2：A 的全部输出（其末笔执行合法落在 2025-01-02）== C 的同窗前缀
    a_last = rA.equity_curve["date"].max()
    tA = rA.trades.sort(["execution_date", "code", "side"])
    eA = rA.equity_curve.sort("date")
    tCa = rC.trades.filter(pl.col("execution_date") <= a_last
                           ).sort(["execution_date", "code", "side"])
    eCa = rC.equity_curve.filter(pl.col("date") <= a_last).sort("date")
    try:
        _assert_eq(tCa, tA, by=["execution_date", "code", "side"], tag="V2.trades")
        _assert_eq(eCa, eA, by=["date"], tag="V2.equity")
        v2 = True
    except AssertionError as ex:
        v2 = False; print(f"    detail: {ex}")
    print(f"[V2] {'PASS' if v2 else 'FAIL'}  C[2024部分] == A（trades+equity 逐字段）")

    # V3：C 中「信号日 ≥ 2025-01-02」的输出 == C2
    #    （A 已消费 12-31 信号，其执行/估值 2025-01-02 归属 A 段）
    tC25 = rC.trades.filter(pl.col("signal_date") >= D2025[0])
    eC25 = rC.equity_curve.filter(pl.col("date") > a_last)
    pC25 = rC.positions_daily.filter(pl.col("date") > a_last)
    if c2["trades"]:
        tC2 = pl.DataFrame(c2["trades"]).with_columns(
            pl.col("signal_date").str.to_date("%Y-%m-%d"),
            pl.col("execution_date").str.to_date("%Y-%m-%d"))
    else:
        tC2 = pl.DataFrame(schema={"signal_date": pl.Date, "execution_date": pl.Date,
                                   "code": pl.Utf8, "side": pl.Utf8, "qty": pl.Int64,
                                   "price": pl.Float64, "fee": pl.Float64})
    eC2 = pl.DataFrame([{"date": datetime.date.fromisoformat(x["date"]),
                         **{k: x[k] for k in ("equity", "cash", "positions_value")},
                         "signal_date": datetime.date.fromisoformat(x["signal_date"])}
                        for x in c2["equity"]])
    pC2 = pl.DataFrame([{"date": datetime.date.fromisoformat(x["date"]),
                         "code": x["code"], "qty": x["qty"],
                         "cost": x["cost"], "market_value": x["market_value"]}
                        for x in c2["positions"]]) if c2["positions"] else \
        pl.DataFrame(schema={"date": pl.Date, "code": pl.Utf8, "qty": pl.Int64,
                             "cost": pl.Float64, "market_value": pl.Float64})
    try:
        _assert_eq(tC25.sort(["execution_date", "code", "side"]),
                   tC2.sort(["execution_date", "code", "side"]),
                   by=["execution_date", "code", "side"], tag="V3.trades")
        _assert_eq(eC25.sort("date"), eC2.sort("date"), by=["date"], tag="V3.equity")
        _assert_eq(pC25.sort(["date", "code"]), pC2.sort(["date", "code"]),
                   by=["date", "code"], tag="V3.pos")
        v3 = True
    except AssertionError as ex:
        v3 = False; print(f"    detail: {str(ex)[:300]}")
    print(f"[V3] {'PASS' if v3 else 'FAIL'}  C[2025部分] == C2(checkpoint 子进程续跑)"
          f"  trades {tC25.height}/{len(c2['trades'])}")

    # V4：允许不等，但量化差异规模
    same_first_cash = abs(
        (eC25["cash"][0] if eC25.height else -1) -
        (rB.equity_curve["cash"][0] if rB.equity_curve.height else -2)) < 1e-6
    v4_differs = not (eC25.height == rB.equity_curve.height and
                      all(abs(x["equity"] - y["equity"]) < 1e-9
                          for x, y in zip(eC25.sort('date').to_dicts(),
                                          rB.equity_curve.sort('date').to_dicts())))
    print(f"[V4] {'OK' if v4_differs else 'NOTE'}  带仓入场 ≠ 新开户"
          f"（首日 cash 相等={same_first_cash}）；差异属预期语义")

    print(f"\n[耗时] 总计 {time.time()-t0:.1f}s")
    verdict = v1 and v2 and v3
    print("\nCONTINUITY:", "ALL PASS" if verdict else "FAILED", flush=True)
    sys.exit(0 if verdict else 2)


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "--segB":
        main_segB(sys.argv[2], sys.argv[3])
    else:
        main()
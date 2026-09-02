"""Execution Diagnostics：rejected 分类 / exposure / carried_valuation 抽样审计。

用法：
    $env:HF_TOKEN = "<token>"
    $env:HF_ENDPOINT = "https://hf-mirror.com"
    python scripts/execution_diagnostics_2024_2025.py
"""
import gc
import os
import random
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
SAMPLE_N = 30


def diagnose_year(year: int) -> dict:
    t0 = time.time()
    dm = build_df_daily(year)
    df_full = dm.df_daily

    trade_dates = (df_full.select(pl.col("date")).unique()
                   .sort("date").to_series().to_list())
    sig = [d for d in trade_dates
           if datetime.date(year, 1, 1) <= d <= datetime.date(year, 12, 31)]
    cal = TradingCalendar(); cal.set_trade_dates(trade_dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    # 记录目标权重（target_vs_actual 需要）
    targets_by_exec = {}
    def recording_allocator(codes, signal_date):
        w = equal_weight_allocator(codes, signal_date)
        exec_d = cal.next_trade_day(signal_date)
        targets_by_exec[exec_d] = w
        return w

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine, raw_price_store=store,
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=recording_allocator,
    )

    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = df_full
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping
    try:
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=sig[0], end_signal_date=sig[-1], initial_cash=1_000_000,
        )
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    ec, tr, pdl = result.equity_curve, result.trades, result.positions_daily

    # ---------- exposure ----------
    daily_pv = pdl.group_by("date").agg(
        pl.col("market_value").sum().alias("pv"), pl.len().alias("npos"))
    j = ec.join(daily_pv, on="date", how="left").with_columns([
        ((pl.col("pv")) / pl.col("equity")).alias("gross"),
        (pl.col("cash") / pl.col("equity")).alias("cash_ratio"),
    ])
    gross = j["gross"].drop_nulls().to_list()
    cashr = j["cash_ratio"].drop_nulls().to_list()

    # target_vs_actual（按 execution_date 对齐）
    dev_sum = dev_n = undeployed = unexited = 0
    act = {}
    for r in pdl.iter_rows(named=True):
        act.setdefault(r["date"], {})[r["code"]] = r["market_value"]
    eqm = {r["date"]: r["equity"] for r in ec.to_dicts()}
    for exec_d, tw in targets_by_exec.items():
        equity = eqm.get(exec_d)
        a = act.get(exec_d, {})
        if not equity or equity <= 0:
            continue
        for code, w_target in {c: 1.0 / len(tw) for c, _ in [(k, v) for k, v in tw.items()]}.items() if False else []:
            pass
        union = set(tw.keys()) | set(a.keys())
        for code in union:
            w_a = a.get(code, 0.0) / equity
            w_t = tw.get(code, 0.0)
            dev_sum += abs(w_a - w_t); dev_n += 1
            if w_t > 0 and w_a <= 0:
                undeployed += 1
            if w_t == 0 and w_a > 0:
                unexited += 1
    mad = dev_sum / dev_n if dev_n else 0.0

    # ---------- carried_valuation 抽样审计 ----------
    px_codes = {
        d: set(store.scan_window(d, d).select("code").collect()["code"].to_list())
        for d in ec["date"].unique().to_list()
    }
    events = [
        (r["date"], r["code"])
        for r in pdl.iter_rows(named=True)
        if r["code"] not in px_codes.get(r["date"], set())
    ]
    rng = random.Random(42)
    sample = rng.sample(events, min(SAMPLE_N, len(events)))

    sub_all = df_full.select(["date", "code", "volume"]).sort(["code", "date"])
    cls_counts = {}
    samples_out = []
    for d, code in sample:
        sub = sub_all.filter(pl.col("code") == code)
        dates_sub = sub["date"].to_list()
        if d not in dates_sub:
            later = [x for x in dates_sub if x > d]
            if not later:
                cls = "LISTING_STATUS(无复牌)"
            else:
                gap_cal = (min(later) - d).days
                cls = (f"SUSP_LIKELY(gap={gap_cal}d)"
                       if gap_cal <= 14 else f"LONG_GAP_CHECK(gap={gap_cal}d)")
        else:
            v = sub.filter(pl.col("date") == d)["volume"][0]
            cls = "VENDOR_ZERO_VOL_SUSP" if (v is None or v == 0) else "HAS_ROW_BUT_CARRIED?"
        cls_counts[cls.split("(")[0]] = cls_counts.get(cls.split("(")[0], 0) + 1
        samples_out.append((str(d), code, cls))

    return {
        "year": year,
        "rej": engine.rej_counters,
        "rej_total": engine.rejections_total,
        "gross_p50": sorted(gross)[len(gross)//2] if gross else 0,
        "gross_mean": sum(gross)/len(gross) if gross else 0,
        "gross_min": min(gross) if gross else 0,
        "cashr_p50": sorted(cashr)[len(cashr)//2] if cashr else 0,
        "weight_mad": mad,
        "undeployed": undeployed, "unexited": unexited,
        "carried_total": len(events),
        "cls_counts": cls_counts,
        "samples": samples_out[:8],
        "elapsed": round(time.time() - t0, 1),
    }


if __name__ == "__main__":
    for y in (2024, 2025):
        R = diagnose_year(y)
        print(f"\n================ Execution Diagnostics {y} ================", flush=True)
        print(f"[Rejected 分类] total={R['rej_total']}")
        for k in sorted(R["rej"], key=R["rej"].get, reverse=True):
            print(f"    {k:<16} {R['rej'][k]:>7}")
        print(f"[Exposure] gross p50={R['gross_p50']:.2%} mean={R['gross_mean']:.2%} "
              f"min={R['gross_min']:.2%} | cash_ratio p50={R['cashr_p50']:.2%}")
        print(f"[Exposure] 目标-实际权重 MAD={R['weight_mad']:.2%} "
              f"未建仓事件={R['undeployed']} 未退出事件={R['unexited']}")
        print(f"[Carry 审计] carried 总数={R['carried_total']} 抽样={SAMPLE_N}")
        for k, v in sorted(R["cls_counts"].items(), key=lambda x: -x[1]):
            print(f"    {k:<28} {v}")
        for s in R["samples"]:
            print(f"      e.g. {s[0]} {s[1]} -> {s[2]}")
        print(f"[耗时] {R['elapsed']}s", flush=True)
        gc.collect()
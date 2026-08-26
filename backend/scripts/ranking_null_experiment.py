# -*- coding: utf-8 -*-
"""Ranking Null Experiment：code-asc vs random vs strength × N=20/50/100 × 4半年度。

目的：将 N 的收益效应从 code-asc selection bias 中剥离出来。

Ranking 策略：
  A. code_asc      — eligible 集合按代码升序取前 N（当前基线）
  B. random        — eligible 集合按固定 seed 随机排序取前 N（多 seed 取均值）
  C. strength_desc — CLOSE / MA20 比值降序（趋势强度最高的前 N）

全部固定：
  weekly · 10M cash · CLOSE > MA(CLOSE,20) · FeeConfig 冻结
"""
import gc
import hashlib
import json
import os
import sys
import time
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from backtest_quality_2024_2025 import build_df_daily

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.metrics import compute_metrics
from core.backtest_types import (
    FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator,
)
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
CASH = 10_000_000
FREQ = "weekly"
NS = [20, 50, 100]
SEEDS = [42, 123, 456, 789, 1024]  # random 用 5 个 seed

SEGMENTS = [
    ("2024H1", datetime.date(2024, 1, 2), datetime.date(2024, 6, 30)),
    ("2024H2", datetime.date(2024, 7, 1), datetime.date(2024, 12, 31)),
    ("2025H1", datetime.date(2025, 1, 2), datetime.date(2025, 6, 30)),
    ("2025H2", datetime.date(2025, 7, 1), datetime.date(2025, 12, 31)),
]


def make_code_asc(n):
    def alloc(codes, signal_date):
        picked = sorted(codes)[:n]
        if not picked:
            return {}
        w = 1.0 / len(picked)
        return {c: w for c in picked}
    return alloc


def make_random(n, seed):
    def alloc(codes, signal_date):
        if not codes:
            return {}
        # deterministic random using seed + signal_date for reproducibility
        seed_val = int(hashlib.md5(f"{seed}_{signal_date}".encode()).hexdigest()[:8], 16)
        shuffled = sorted(codes, key=lambda c: hashlib.md5(
            f"{seed_val}_{c}".encode()).hexdigest())
        picked = shuffled[:n]
        if not picked:
            return {}
        w = 1.0 / len(picked)
        return {c: w for c in picked}
    return alloc


def make_strength(n, dm):
    """CLOSE / MA20 比值降序。需要访问 dm.df_daily 获取 MA20 值。"""
    def alloc(codes, signal_date):
        if not codes or dm.df_daily is None:
            return sorted(codes)[:n] and {c: 1.0 / n for c in sorted(codes)[:n]} or {}
        # 获取各股票在 signal_date 的 close 和 MA20
        df = (dm.df_daily
              .filter(pl.col("date") == signal_date)
              .filter(pl.col("code").is_in(codes))
              .select(["code", "close"]))
        if df.is_empty():
            return {c: 1.0 / n for c in sorted(codes)[:n]} if codes else {}

        # 计算 MA20（用最近 20 天数据）
        ma20_df = (
            dm.df_daily
            .filter(pl.col("date") <= signal_date)
            .filter(pl.col("code").is_in(codes))
            .sort(["code", "date"])
            .group_by("code")
            .agg(pl.col("close").tail(20).mean().alias("ma20"))
        )
        scored = df.join(ma20_df, on="code", how="inner")
        if scored.is_empty():
            return {c: 1.0 / n for c in sorted(codes)[:n]} if codes else {}

        scored = scored.with_columns(
            pl.when(pl.col("ma20") > 0)
            .then(pl.col("close") / pl.col("ma20"))
            .otherwise(pl.lit(0.0)).alias("score")
        ).sort("score", descending=True)

        picked = scored["code"].to_list()[:n]
        if not picked:
            return {}
        w = 1.0 / len(picked)
        return {c: w for c in picked}
    return alloc


def run_half(dm, dates, seg_start, seg_end, top_n, allocator):
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)
    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
    )
    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping
    try:
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=seg_start, end_signal_date=seg_end,
            initial_cash=CASH, rebalance_freq=FREQ,
        )
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    m = compute_metrics(result, initial_cash=CASH)
    flat = m.to_flat_dict()
    P = lambda g, k: flat.get(f"{g}.{k}", 0)
    return {
        "total_return": P("performance", "total_return"),
        "max_drawdown": P("performance", "max_drawdown"),
        "deployment_mean": P("exposure", "deployment_mean"),
        "dust_reject_ratio": P("execution_quality", "dust_reject_ratio"),
        "turnover": P("trading", "turnover"),
        "trade_count": P("trading", "trade_count"),
        "total_fees": P("trading", "total_fees"),
    }


if __name__ == "__main__":
    t_all = time.time()

    log_t0 = time.time()
    print("data loading...", flush=True)
    dm = build_df_daily(2025)  # 加载最全的数据窗口
    dates_all = (dm.df_daily.select(pl.col("date")).unique()
                 .sort("date").to_series().to_list())
    print(f"data ready: codes={dm.df_daily['code'].n_unique()} "
          f"range={dates_all[0]}..{dates_all[-1]}", flush=True)

    all_results = []

    for seg_name, seg_start, seg_end in SEGMENTS:
        print(f"\n{'='*70}\n>>> {seg_name}\n{'='*70}", flush=True)

        for ranking_name in ["code_asc", "random_mean5", "strength_desc"]:
            for n in NS:
                tag = f"{seg_name}_{ranking_name}_N{n}"
                t0 = time.time()

                if ranking_name == "code_asc":
                    alloc = make_code_asc(n)
                    runs = [(tag, alloc)]
                elif ranking_name == "random_mean5":
                    runs = []
                    for seed in SEEDS:
                        s_alloc = make_random(n, seed)
                        runs.append((f"{tag}_s{seed}", s_alloc))
                else:  # strength_desc
                    alloc = make_strength(n, dm)
                    runs = [(tag, alloc)]

                run_results = []
                for run_tag, run_alloc in runs:
                    R = run_half(dm, dates_all, seg_start, seg_end, n, run_alloc)
                    run_results.append(R)

                # 对 random 多 seed 取均值
                if ranking_name == "random_mean5" and len(run_results) > 1:
                    mean_R = {}
                    for k in run_results[0]:
                        vals = [r[k] for r in run_results]
                        mean_R[k] = sum(vals) / len(vals)
                    final = {"ranking": ranking_name, "segment": seg_name,
                             "top_n": n, **mean_R}
                else:
                    final = {"ranking": ranking_name, "segment": seg_name,
                             "top_n": n, **run_results[0]}

                all_results.append(final)
                elapsed = round(time.time() - t0, 1)
                print(f"  {ranking_name:>16} N={n:>3}: "
                      f"return={final['total_return']:.1%} "
                      f"dd={final['max_drawdown']:.1%} "
                      f"({elapsed}s)", flush=True)

                gc.collect()

    # ---- 汇总矩阵 ----
    print("\n" + "=" * 90)
    print("Ranking × N × Segment 汇总（weekly · 10M · CLOSE>MA20）\n")

    rankings = ["code_asc", "random_mean5", "strength_desc"]
    for metric in ["total_return", "max_drawdown"]:
        print(f"\n--- {metric} ---")
        header = f"{'Segment':>10}"
        for r_name in rankings:
            header += f" {'code_asc':>10} {'random':>10} {'strength':>10}"
            break
        header += f" {'N':>4}"
        print(header)

        for seg_name, _, _ in SEGMENTS:
            row = f"{seg_name:>10}"
            for n in NS:
                vals = []
                for r_name in rankings:
                    r = next((r for r in all_results
                              if r["segment"] == seg_name and r["top_n"] == n
                              and r["ranking"] == r_name), None)
                    v = r[metric] if r else 0
                    fmt = f"{v:.1%}" if metric == "total_return" else f"{v:.1%}"
                    vals.append(fmt)
                row += f" {'/'.join(vals):>32}"
                row += f" N={n:<3}"

            # 重写为更清晰的输出
            pass
        # 改为逐行打印
        for n in NS:
            print(f"\n  N={n}:")
            for seg_name, _, _ in SEGMENTS:
                row = f"    {seg_name:>10}:"
                for r_name in rankings:
                    r = next((r for r in all_results
                              if r["segment"] == seg_name and r["top_n"] == n
                              and r["ranking"] == r_name), None)
                    v = r[metric] if r else 0
                    row += f"  {v:.1%}"
                print(row)

    # ---- 排序差异分析 ----
    print("\n--- Ranking 差异分析 ---")
    for seg_name, _, _ in SEGMENTS:
        print(f"\n  {seg_name}:")
        for n in NS:
            ca = next((r for r in all_results if r["segment"] == seg_name
                       and r["top_n"] == n and r["ranking"] == "code_asc"), None)
            st = next((r for r in all_results if r["segment"] == seg_name
                       and r["top_n"] == n and r["ranking"] == "strength_desc"), None)
            rd = [r for r in all_results if r["segment"] == seg_name
                  and r["top_n"] == n and r["ranking"] == "random_mean5"]
            rd_ret = sum(r["total_return"] for r in rd) / len(rd) if rd else 0
            if ca and st:
                adv = st["total_return"] - ca["total_return"]
                print(f"    N={n:>3}: code_asc={ca['total_return']:.1%} "
                      f"random≈{rd_ret:.1%} strength={st['total_return']:.1%} "
                      f"(strength advantage={adv:+.1%})")

    # ---- 存档 ----
    out_dir = Path(__file__).resolve().parent / "experiment_results"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "ranking_null_experiment.json").write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved -> {out_dir / 'ranking_null_experiment.json'}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")
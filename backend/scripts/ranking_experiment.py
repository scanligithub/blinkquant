# -*- coding: utf-8 -*-
"""Ranking Experiment（v3 fast）：code_asc vs strength_desc vs strength_asc × N=20/100 × 4半年度。

Rank IC 暂跳过（每段 IC 需逐日加载 frame，耗时 >10min），先跑 portfolio 指标。
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
from backtest_quality_2024_2025 import build_df_daily

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.metrics import compute_metrics
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.ranking import code_asc_ranking, strength_desc_ranking, strength_asc_ranking
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
CASH = 10_000_000
FREQ = "weekly"
NS = [20, 100]

SEGMENTS = [
    ("2024H1", datetime.date(2024, 1, 2), datetime.date(2024, 6, 30)),
    ("2024H2", datetime.date(2024, 7, 1), datetime.date(2024, 12, 31)),
    ("2025H1", datetime.date(2025, 1, 2), datetime.date(2025, 6, 30)),
    ("2025H2", datetime.date(2025, 7, 1), datetime.date(2025, 12, 31)),
]


def run_half(dm, dates, seg_start, seg_end, top_n, ranking_fn):
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)
    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=None,
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
            ranking_fn=ranking_fn, top_n=top_n,
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
        "concentration.hhi_mean": P("concentration", "hhi_mean"),
        "concentration.effective_n_mean": P("concentration", "effective_n_mean"),
        "concentration.actual_n_mean": P("concentration", "actual_n_mean"),
        "concentration.target_weight_mae": P("concentration", "target_weight_mae"),
    }


if __name__ == "__main__":
    t_all = time.time()
    print("data loading...", flush=True)
    dm = build_df_daily(2025)
    dates_all = (dm.df_daily.select(pl.col("date")).unique()
                 .sort("date").to_series().to_list())
    print(f"data ready: codes={dm.df_daily['code'].n_unique()} "
          f"range={dates_all[0]}..{dates_all[-1]}", flush=True)

    ranking_fns = {
        "code_asc": code_asc_ranking,
        "strength_desc": strength_desc_ranking,
        "strength_asc": strength_asc_ranking,
    }
    all_results = []

    for seg_name, seg_start, seg_end in SEGMENTS:
        print(f"\n{'='*70}\n>>> {seg_name}\n{'='*70}", flush=True)
        for ranking_name, ranking_fn in ranking_fns.items():
            for n in NS:
                t0 = time.time()
                R = run_half(dm, dates_all, seg_start, seg_end, n, ranking_fn)
                elapsed = round(time.time() - t0, 1)
                row = {"ranking": ranking_name, "segment": seg_name, "top_n": n, **R}
                all_results.append(row)
                print(f"  {ranking_name:>16} N={n:>3}: "
                      f"ret={R['total_return']:+.1%} dd={R['max_drawdown']:.1%} "
                      f"hhi={R['concentration.hhi_mean']:.4f} "
                      f"effN={R['concentration.effective_n_mean']:.1f} "
                      f"wmae={R['concentration.target_weight_mae']:.4f} "
                      f"({elapsed}s)", flush=True)
                gc.collect()

    print("\n" + "=" * 90)
    print("Ranking × N × Segment 汇总\n")
    for metric in ["total_return", "max_drawdown"]:
        print(f"\n--- {metric} ---")
        for n in NS:
            print(f"\n  N={n}:")
            for seg_name, _, _ in SEGMENTS:
                row_str = f"    {seg_name:>10}:"
                for r_name in ranking_fns:
                    r = next((r for r in all_results
                              if r["segment"] == seg_name and r["top_n"] == n
                              and r["ranking"] == r_name), None)
                    v = r[metric] if r else 0
                    row_str += f"  {r_name}={v:+.1%}"
                print(row_str)

    print("\n--- Ranking Advantage（vs code_asc）---")
    for n in NS:
        print(f"\n  N={n}:")
        for seg_name, _, _ in SEGMENTS:
            ca = next((r for r in all_results if r["segment"] == seg_name
                       and r["top_n"] == n and r["ranking"] == "code_asc"), None)
            sd = next((r for r in all_results if r["segment"] == seg_name
                       and r["top_n"] == n and r["ranking"] == "strength_desc"), None)
            sa = next((r for r in all_results if r["segment"] == seg_name
                       and r["top_n"] == n and r["ranking"] == "strength_asc"), None)
            if ca and sd and sa:
                adv_d = sd["total_return"] - ca["total_return"]
                adv_a = sa["total_return"] - ca["total_return"]
                print(f"    {seg_name:>10}: desc_adv={adv_d:+.1%} asc_adv={adv_a:+.1%}")

    out_dir = Path(__file__).resolve().parent / "experiment_results"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "ranking_experiment.json").write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nSaved -> {out_dir / 'ranking_experiment.json'}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")

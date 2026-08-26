# -*- coding: utf-8 -*-
"""第二阶段：资金规模 × 组合宽度（weekly · 2024 · CLOSE>MA20 · Top-N 等权）。

矩阵：
    N ∈ {20, 50, 100}  ×  initial_cash ∈ {1M, 5M, 10M}  = 9 组

输出：
    - 控制台容量热力图（dust / deployment / return / fee_ratio）
    - scripts/experiment_results/stage2_{tag}.json
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
from backtest_quality_2024_2025 import build_df_daily as build_df

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
YEAR = 2024
FREQ = "weekly"
OUT_DIR = Path(__file__).resolve().parent / "experiment_results"

NS = [20, 50, 100]
CASHES = [1_000_000, 5_000_000, 10_000_000]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run_group(dm, dates, year, cash, top_n):
    t0 = time.time()
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    allocator = top_n_equal_weight_allocator(top_n)

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine, raw_price_store=store,
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=allocator,
    )

    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping
    try:
        sig_start = max(datetime.date(year, 1, 1), dates[0])
        sig_end = min(datetime.date(year, 12, 31), dates[-1])
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=sig_start, end_signal_date=sig_end,
            initial_cash=cash, rebalance_freq=FREQ,
        )
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    m1 = compute_metrics(result, initial_cash=cash)
    m2 = compute_metrics(result, initial_cash=cash)
    deterministic = m1.to_flat_dict() == m2.to_flat_dict()
    flat = m1.to_flat_dict()

    diag = result.execution_diagnostics or {}
    pdl = result.positions_daily

    integ_keys = [
        flat.get("integrity.zero_price_trade_count", 0),
        flat.get("integrity.t1_violation_count", 0),
        flat.get("integrity.negative_cash_count", 0),
        flat.get("integrity.accounting_invariant_violations", 0),
    ]

    P = lambda g, k: flat.get(f"{g}.{k}", 0)
    total_fees = P("trading", "total_fees")

    actual_pos_by_date = pdl.group_by("date").agg(pl.len().alias("n")) if pdl.height else None
    pos_mean = float(actual_pos_by_date["n"].mean()) if actual_pos_by_date is not None and actual_pos_by_date.height else 0.0

    return {
        "config": {
            "commit": os.popen("git rev-parse --short HEAD").read().strip()[:8],
            "year": year, "formula": "CLOSE > MA(CLOSE, 20)", "timeframe": "D",
            "initial_cash": cash, "top_n": top_n, "rebalance": FREQ,
            "target_per_stock": int(cash / top_n) if top_n else 0,
        },
        "metrics": flat,
        "diag": {
            "intents_total": diag.get("intents_total", 0),
            "rej_counters": diag.get("rej_counters", {}),
        },
        "experiment": {
            "deterministic": deterministic,
            "integrity_ok": all(v == 0 for v in integ_keys),
            "fee_ratio": total_fees / cash if cash else 0,
            "actual_position_count_mean": round(pos_mean, 1),
            "elapsed_s": round(time.time() - t0, 1),
        },
    }


if __name__ == "__main__":
    OUT_DIR.mkdir(exist_ok=True)
    t_all = time.time()
    log(f"Stage2 commit={os.popen('git rev-parse --short HEAD').read().strip()[:8]} year={YEAR}")
    dm = build_df(YEAR)
    dates = (dm.df_daily.select(pl.col("date")).unique()
             .sort("date").to_series().to_list())
    log(f"data: codes={dm.df_daily['code'].n_unique()}")

    results = []
    for n in NS:
        for cash in CASHES:
            tag = f"N{n}_cash{cash//1_000_000}M"
            print(f"\n>>> {tag}", flush=True)
            R = run_group(dm, dates, YEAR, cash, n)
            results.append(R)

            f = R["metrics"]; x = R["experiment"]; c = R["config"]
            P = lambda g, k: f.get(f"{g}.{k}", 0)
            print(f"  return={P('performance','total_return'):.2%} "
                  f"maxDD={P('performance','max_drawdown'):.2%}")
            print(f"  dust={P('execution_quality','dust_reject_ratio'):.2%} "
                  f"deploy={P('exposure','deployment_mean'):.2%} "
                  f"cash_drag={P('exposure','cash_drag'):.2%} "
                  f"fee_ratio={x['fee_ratio']:.2%} "
                  f"det={x['deterministic']} integ={x['integrity_ok']}")
            gc.collect()

    # ---- 容量热力图 ----
    lookup = {(R["config"]["top_n"], R["config"]["initial_cash"]): R for R in results}
    P = lambda R, g, k: R["metrics"].get(f"{g}.{k}", 0)

    print("\n" + "=" * 80)
    print("容量热力图 (2024 · weekly · CLOSE>MA20)")
    print("=" * 80)

    for metric_name, group, key, fmt in [
        ("dust_reject_ratio", "execution_quality", "dust_reject_ratio", ".2%"),
        ("deployment_mean", "exposure", "deployment_mean", ".2%"),
        ("cash_drag", "exposure", "cash_drag", ".2%"),
        ("fee_ratio", None, "__fee_ratio__", ".2%"),
        ("total_return", "performance", "total_return", ".2%"),
        ("max_drawdown", "performance", "max_drawdown", ".2%"),
    ]:
        print(f"\n--- {metric_name} ---")
        print(f"{'':>8} {'1M':>10} {'5M':>10} {'10M':>10}")
        for n in NS:
            vals = []
            for c in CASHES:
                R = lookup[(n, c)]
                if key == "__fee_ratio__":
                    v = R["experiment"]["fee_ratio"]
                else:
                    v = P(R, group, key)
                vals.append(f"{v:{fmt}}")
            print(f"N={n:>4} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10}")

    # ---- 存档 ----
    for R in results:
        c = R["config"]
        tag = f"stage2_N{c['top_n']}_cash{c['initial_cash']//1_000_000}M"
        out = OUT_DIR / f"{tag}.json"
        out.write_text(json.dumps(R, ensure_ascii=False, indent=2, default=str),
                       encoding="utf-8")

    all_pass = all(R["experiment"]["deterministic"] and R["experiment"]["integrity_ok"]
                   for R in results)
    print(f"\nSaved {len(results)} results -> {OUT_DIR}")
    print(f"HARD-CHECK: {'ALL PASS' if all_pass else 'FAILURES'}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")
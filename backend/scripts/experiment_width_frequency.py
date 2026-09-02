# -*- coding: utf-8 -*-
"""第一阶段实验：1M × (N=5,20,50,100) × (daily,weekly)，2024 全年真实数据。

用法：
    $env:HF_TOKEN = "<token>"
    $env:HF_ENDPOINT = "https://hf-mirror.com"
    python scripts/experiment_width_frequency.py
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
from core.backtest_types import (
    FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator,
    top_n_equal_weight_allocator,
)
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
YEAR = 2024
OUT_DIR = Path(__file__).resolve().parent / "experiment_results"
COMMIT = os.popen("git rev-parse --short HEAD").read().strip()[:8]

NS = [5, 20, 50, 100]
FREQS = ["daily", "weekly"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build_df(year):
    dm = build_df_daily(year)
    dates = (dm.df_daily.select(pl.col("date")).unique()
             .sort("date").to_series().to_list())
    return dm, dates


def run_group(dm, dates, year, cash, top_n, freq):
    t0 = time.time()
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    allocator = (top_n_equal_weight_allocator(top_n) if top_n > 0
                 else equal_weight_allocator)

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
            initial_cash=cash, rebalance_freq=freq,
        )
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    # determinism check
    m1 = compute_metrics(result, initial_cash=cash)
    m2 = compute_metrics(result, initial_cash=cash)
    deterministic = m1.to_flat_dict() == m2.to_flat_dict()
    flat = m1.to_flat_dict()

    diag = result.execution_diagnostics or {}
    ec = result.equity_curve
    tr = result.trades
    pdl = result.positions_daily

    actual_pos_by_date = pdl.group_by("date").agg(pl.len().alias("n")) if pdl.height else None
    actual_pos_mean = float(actual_pos_by_date["n"].mean()) if actual_pos_by_date is not None and actual_pos_by_date.height else 0.0

    integ_keys = [
        flat.get("integrity.zero_price_trade_count", 0),
        flat.get("integrity.t1_violation_count", 0),
        flat.get("integrity.negative_cash_count", 0),
        flat.get("integrity.accounting_invariant_violations", 0),
    ]
    integrity_ok = all(v == 0 for v in integ_keys)

    P = lambda g, k: flat.get(f"{g}.{k}", 0)

    return {
        "config": {
            "commit": COMMIT, "year": year,
            "formula": "CLOSE > MA(CLOSE, 20)", "timeframe": "D",
            "initial_cash": cash, "top_n": top_n, "rebalance": freq,
            "allocator": f"top_{top_n}_equal_weight" if top_n > 0 else "equal_weight",
            "fee_config": {"commission_rate": 0.00025, "commission_min": 5.0,
                           "stamp_tax_rate": 0.0005, "transfer_fee_rate": 0.00001},
        },
        "metrics": flat,
        "diag": {
            "intents_total": diag.get("intents_total", 0),
            "rej_counters": diag.get("rej_counters", {}),
            "partial_fill_count": diag.get("partial_fill_count", 0),
            "carried_events": diag.get("carried_events", 0),
        },
        "experiment": {
            "actual_position_count_mean": round(actual_pos_mean, 1),
            "valuation_days": ec.height if ec is not None else 0,
            "trade_days": int(P("trading", "trade_days")),
            "deterministic": deterministic,
            "integrity_ok": integrity_ok,
            "elapsed_s": round(time.time() - t0, 1),
        },
    }


def report(R):
    f = R["metrics"]; x = R["experiment"]; c = R["config"]
    P = lambda g, k: f.get(f"{g}.{k}", 0)
    print(f"\n[Config] N={c['top_n']} freq={c['rebalance']} cash={c['initial_cash']:,.0f}")
    print(f"[Perf] return={P('performance','total_return'):.2%} "
          f"ann={P('performance','annualized_return'):.2%} "
          f"maxDD={P('performance','max_drawdown'):.2%}")
    print(f"[Exposure] deploy_mean={P('exposure','deployment_mean'):.2%} "
          f"deploy_med={P('exposure','deployment_median'):.2%} "
          f"cash_drag={P('exposure','cash_drag'):.2%} "
          f"target_fill={P('exposure','target_fill_ratio'):.4f}")
    q = "execution_quality"
    print(f"[ExecQ] dust={P(q,'dust_reject_count'):.0f}({P(q,'dust_reject_ratio'):.2%}) "
          f"partial={P(q,'partial_fill_count'):.0f}({P(q,'partial_fill_ratio'):.2%}) "
          f"limit_blocked={P(q,'limit_blocked_count'):.0f}({P(q,'limit_blocked_ratio'):.2%}) "
          f"carried={P(q,'carried_events'):.0f}")
    print(f"[Trading] trades={P('trading','trade_count'):.0f} "
          f"(B{P('trading','buy_count'):.0f}/S{P('trading','sell_count'):.0f}) "
          f"turnover={P('trading','turnover'):.2f}x fees={P('trading','total_fees'):,.0f} "
          f"trade_days={P('trading','trade_days'):.0f} "
          f"active_pos_days={P('trading','active_position_days'):.0f}")
    print(f"[Experiment] pos_mean={x['actual_position_count_mean']} "
          f"det={x['deterministic']} integ={x['integrity_ok']} elapsed={x['elapsed_s']}s")


if __name__ == "__main__":
    OUT_DIR.mkdir(exist_ok=True)
    t_all = time.time()

    log(f"Experiment commit={COMMIT} year={YEAR}")
    dm, dates = build_df(YEAR)
    log(f"data ready: codes={dm.df_daily['code'].n_unique()} "
        f"range={dates[0]}..{dates[-1]}")

    results = []
    for n in NS:
        for fq in FREQS:
            tag = f"N{n}_{fq}"
            print(f"\n{'='*60}\n>>> {tag}\n{'='*60}", flush=True)
            R = run_group(dm, dates, YEAR, 1_000_000, n, fq)
            results.append(R)
            report(R)
            gc.collect()

    # ---- 汇总表 ----
    print("\n" + "=" * 95)
    print(f"{'N':>4} {'Freq':>8} {'return':>9} {'maxDD':>8} {'deploy':>7} "
          f"{'dust%':>7} {'turnover':>9} {'fees':>10} {'trades':>7} "
          f"{'pos_avg':>7} {'det':>4} {'integ':>5}")
    print("-" * 95)
    for R in results:
        f = R["metrics"]; x = R["experiment"]; c = R["config"]
        integ = "PASS" if x["integrity_ok"] else "FAIL"
        det = "PASS" if x["deterministic"] else "FAIL"
        P = lambda g, k: f.get(f"{g}.{k}", 0)
        print(f"{c['top_n']:>4} {c['rebalance']:>8} "
              f"{P('performance','total_return'):>8.2%} "
              f"{P('performance','max_drawdown'):>7.2%} "
              f"{P('exposure','deployment_mean'):>6.2%} "
              f"{P('execution_quality','dust_reject_ratio'):>6.2%} "
              f"{P('trading','turnover'):>8.2f}x "
              f"{P('trading','total_fees'):>9,.0f} "
              f"{P('trading','trade_count'):>6.0f} "
              f"{x['actual_position_count_mean']:>6.1f} "
              f"{det:>4} {integ:>5}")

    for R in results:
        tag = f"Y{R['config']['year']}_N{R['config']['top_n']}_{R['config']['rebalance']}"
        out = OUT_DIR / f"{tag}.json"
        out.write_text(json.dumps(R, ensure_ascii=False, indent=2, default=str),
                       encoding="utf-8")
    print(f"\nSaved {len(results)} results -> {OUT_DIR}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")
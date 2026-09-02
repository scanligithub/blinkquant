# -*- coding: utf-8 -*-
"""2025 Out-of-Sample 验证：5 组 · weekly · CLOSE>MA20 · Top-N 等权。

组别：
    N=20  / 1M   （低宽度控制）
    N=50  / 1M   （中间基线）
    N=100 / 1M   （高粉尘低资金基线）
    N=100 / 5M   （中间容量改善）
    N=100 / 10M  （高宽度高容量改善）

验证：
    结构单调性（cash↑→dust↓/deploy↑/fee↓）是否跨年复现。
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
YEAR = 2025
FREQ = "weekly"
OUT_DIR = Path(__file__).resolve().parent / "experiment_results"

GROUPS = [
    (20, 1_000_000),
    (50, 1_000_000),
    (100, 1_000_000),
    (100, 5_000_000),
    (100, 10_000_000),
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run_group(dm, dates, year, cash, top_n):
    t0 = time.time()
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine, raw_price_store=store,
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=top_n_equal_weight_allocator(top_n),
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
    pos_by = pdl.group_by("date").agg(pl.len().alias("n")) if pdl.height else None
    pos_mean = float(pos_by["n"].mean()) if pos_by is not None and pos_by.height else 0.0

    return {
        "config": {
            "commit": os.popen("git rev-parse --short HEAD").read().strip()[:8],
            "year": year, "formula": "CLOSE > MA(CLOSE, 20)", "timeframe": "D",
            "initial_cash": cash, "top_n": top_n, "rebalance": FREQ,
            "target_per_stock": int(cash / top_n) if top_n else 0,
        },
        "metrics": flat,
        "diag": {"rej_counters": diag.get("rej_counters", {})},
        "experiment": {
            "deterministic": deterministic,
            "integrity_ok": all(v == 0 for v in integ_keys),
            "fee_ratio": total_fees / cash if cash else 0,
            "actual_position_count_mean": round(pos_mean, 1),
        },
    }


if __name__ == "__main__":
    OUT_DIR.mkdir(exist_ok=True)
    t_all = time.time()
    log(f"OOS year={YEAR} freq={FREQ}")
    dm = build_df(YEAR)
    dates = (dm.df_daily.select(pl.col("date")).unique()
             .sort("date").to_series().to_list())
    log(f"data: codes={dm.df_daily['code'].n_unique()} "
        f"range={dates[0]}..{dates[-1]}")

    results = []
    for n, cash in GROUPS:
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
              f"fee_ratio={x['fee_ratio']:.2%} "
              f"det={x['deterministic']} integ={x['integrity_ok']}")
        gc.collect()

    # ---- 双年结构稳定性对比 ----
    s24_path = OUT_DIR.parent / "experiment_results"
    stage2 = {}
    for n in {20, 50, 100}:
        for cm in [1, 5, 10]:
            p = s24_path / f"stage2_N{n}_cash{cm}M.json"
            if p.exists():
                stage2[(n, cm * 1_000_000)] = json.load(open(p, encoding="utf-8"))

    def _v(R, g, k):
        return R["metrics"].get(f"{g}.{k}", 0)

    print("\n" + "=" * 80)
    print("双年结构稳定性对比（2024 vs 2025 OOS）")
    print("=" * 80)
    for n, cash in GROUPS:
        key24 = (n, cash)
        if key24 not in stage2:
            continue
        r24 = stage2[key24]; r25 = next(
            (R for R in results if R["config"]["top_n"] == n
             and R["config"]["initial_cash"] == cash), None)
        if not r25:
            continue
        m24 = r24["metrics"]; m25 = r25["metrics"]
        x24 = r24["experiment"]; x25 = r25["experiment"]
        print(f"\nN={n} cash={cash//1_000_000}M:")
        for label, k in [("dust", "execution_quality.dust_reject_ratio"),
                         ("deploy", "exposure.deployment_mean"),
                         ("fee_ratio", None),
                         ("return", "performance.total_return"),
                         ("maxDD", "performance.max_drawdown")]:
            if k:
                v24 = m24.get(k, 0); v25 = m25.get(k, 0)
                print(f"  {label:>10}: 2024={v24:.4%}  2025={v25:.4%}")
            elif label == "fee_ratio":
                print(f"  {'fee_ratio':>10}: 2024={x24['fee_ratio']:.4%}"
                      f"  2025={x25['fee_ratio']:.4%}")

    # 单调性检查
    print("\n--- 单调性检查 ---")
    for metric_label, g, k, expect in [
        ("dust desc", "execution_quality", "dust_reject_ratio", "desc"),
        ("deploy asc", "exposure", "deployment_mean", "asc"),
        ("fee_ratio desc", None, "__fee__", "desc"),
        ("return asc", "performance", "total_return", "asc"),
    ]:
        vals_25 = []
        for n in [20, 50, 100]:
            R = next((R for R in results if R["config"]["top_n"] == n
                      and R["config"]["initial_cash"] == 1_000_000), None)
            if R:
                if k == "__fee__":
                    vals_25.append(R["experiment"]["fee_ratio"])
                else:
                    vals_25.append(R["metrics"].get(f"{g}.{k}", 0))
        if len(vals_25) < 3:
            continue
        if expect == "desc":
            mono = all(vals_25[i] >= vals_25[i+1] for i in range(len(vals_25)-1))
        else:
            mono = all(vals_25[i] <= vals_25[i+1] for i in range(len(vals_25)-1))
        trend = " -> ".join(f"{v:.4f}" for v in vals_25)
        mark = "OK" if mono else "BREAK"
        print(f"  {metric_label:>16} (20->50->100 @1M): {trend}  [{mark}]")

    # ---- 存档 ----
    for R in results:
        c = R["config"]
        tag = f"oos_N{c['top_n']}_cash{c['initial_cash']//1_000_000}M"
        out = OUT_DIR / f"{tag}.json"
        out.write_text(json.dumps(R, ensure_ascii=False, indent=2, default=str),
                       encoding="utf-8")

    all_pass = all(R["experiment"]["deterministic"] and R["experiment"]["integrity_ok"]
                   for R in results)
    print(f"\nSaved {len(results)} results -> {OUT_DIR}")
    print(f"HARD-CHECK: {'ALL PASS' if all_pass else 'FAILURES'}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")
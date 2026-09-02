# -*- coding: utf-8 -*-
"""半年度市场状态敏感性实验：N=20/50/100 × {H1,H2} × {2024,2025} · weekly · 10M。

目的：验证 N 对收益的影响是否依赖市场环境（N × regime 交互效应）。
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
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
CASH = 10_000_000
FREQ = "weekly"
NS = [20, 50, 100]

# 半年区间定义（signal_date 落入该区间即归属该段）
SEGMENTS = [
    ("2024H1", datetime.date(2024, 1, 2), datetime.date(2024, 6, 30), 2024),
    ("2024H2", datetime.date(2024, 7, 1), datetime.date(2024, 12, 31), 2024),
    ("2025H1", datetime.date(2025, 1, 2), datetime.date(2025, 6, 30), 2025),
    ("2025H2", datetime.date(2025, 7, 1), datetime.date(2025, 12, 31), 2025),
]


def run_half(dm, dates, year, seg_start, seg_end, top_n):
    cal = TradingCalendar(); cal.set_trade_dates(dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=top_n_equal_weight_allocator(top_n),
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
        "segment": f"{year}{'H1' if seg_start.month <= 6 else 'H2'}",
        "top_n": top_n,
        "total_return": P("performance", "total_return"),
        "max_drawdown": P("performance", "max_drawdown"),
        "deployment_mean": P("exposure", "deployment_mean"),
        "dust_reject_ratio": P("execution_quality", "dust_reject_ratio"),
        "turnover": P("trading", "turnover"),
        "trade_count": P("trading", "trade_count"),
        "concentration_hhi": round(flat.get("concentration.hhi_mean", 0), 6),
        "effective_n_mean": round(flat.get("concentration.effective_n_mean", 0), 1),
        "weight_deviation": round(flat.get("concentration.weight_deviation_mean", 0), 6),
    }


if __name__ == "__main__":
    t_all = time.time()

    # 数据一次构建：加载 2023..2026 覆盖所有段的 T+1 边界和 MA20 暖机
    log_t0 = time.time()
    dm = build_df_daily(2025)  # 加载 2024+2025+2026
    dates_all = (dm.df_daily.select(pl.col("date")).unique()
                 .sort("date").to_series().to_list())
    print(f"data: codes={dm.df_daily['code'].n_unique()} "
          f"range={dates_all[0]}..{dates_all[-1]}", flush=True)

    results = []
    for seg_name, seg_start, seg_end, year in SEGMENTS:
        for n in NS:
            tag = f"{seg_name}_N{n}"
            print(f">>> {tag}", flush=True)
            R = run_half(dm, dates_all, year, seg_start, seg_end, n)
            R["segment"] = seg_name
            R["top_n"] = n
            results.append(R)
            print(f"  return={R['total_return']:.2%} dd={R['max_drawdown']:.2%} "
                  f"deploy={R['deployment_mean']:.2%} "
                  f"hhi={R['concentration_hhi']:.6f} "
                  f"eff_N={R['effective_n_mean']:.1f}")
            gc.collect()

    # ---- 市场状态敏感性矩阵 ----
    print("\n" + "=" * 90)
    print(f"半年度 × N 敏感性矩阵 ({FREQ} · {CASH//1_000_000}M cash)\n")

    for metric in ["total_return", "max_drawdown", "deployment_mean",
                   "dust_reject_ratio", "effective_n_mean"]:
        print(f"\n--- {metric} ---")
        header = f"{'':>10}"
        for n in NS:
            header += f" {'N='+str(n):>10}"
        print(header)
        for seg_name, _, _, _ in SEGMENTS:
            row = f"{seg_name:>10}"
            for n in NS:
                r = next((r for r in results if r["segment"] == seg_name and r["top_n"] == n), None)
                if r is None:
                    continue
                v = r[metric]
                if metric in ("total_return", "max_drawdown"):
                    fmt = f"{v:.1%}"
                elif metric in ("deployment_mean", "dust_reject_ratio"):
                    fmt = f"{v:.1%}"
                elif metric == "effective_n_mean":
                    fmt = f"{v:.0f}"
                else:
                    fmt = f"{v:.3f}"
                row += f" {fmt:>10}"
            print(row)

    # ---- N 排序跨段反转分析 ----
    print("\n--- N 收益排序跨段变化 ---")
    for seg_name, _, _, _ in SEGMENTS:
        ranked = sorted(
            [r for r in results if r["segment"] == seg_name],
            key=lambda r: r["total_return"], reverse=True)
        order = " > ".join(f"N{r['top_n']}" for r in ranked)
        best = ranked[0]
        worst = ranked[-1]
        spread = best["total_return"] - worst["total_return"]
        print(f"  {seg_name}: {order}  (spread={spread:.1%})")

    # ---- 存档 ----
    out_dir = Path(__file__).resolve().parent / "experiment_results"
    out_dir.mkdir(exist_ok=True)
    for R in results:
        fn = f"semi_{R['segment']}_N{R['top_n']}.json"
        (out_dir / fn).write_text(
            json.dumps(R, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_fn = out_dir / "semi_annual_summary.json"
    summary_fn.write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nSaved {len(results)} results -> {out_dir}")
    print(f"Total elapsed: {time.time()-t_all:.1f}s")
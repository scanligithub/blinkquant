"""真实全年四层 Metrics 报告：2024 / 2025 分跑，纯后处理 compute_metrics。

用法：
    $env:HF_TOKEN = "<token>"
    $env:HF_ENDPOINT = "https://hf-mirror.com"
    python scripts/metrics_report_2024_2025.py

输出：
    - 控制台四层完整报告 + dust→deployment→cash_drag→return 联动链
    - scripts/metrics_reports/{year}.json（BacktestMetrics 基线存档）
确定性：
    compute_metrics 对同一 result 双跑，to_flat_dict 必须逐字段相等。
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
from core.metrics import compute_metrics
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
import core.data_manager as dmm
from core.engine import selection_engine

TOKEN = os.getenv("HF_TOKEN")
REPO = "scanli/stocka-data"
OUT_DIR = Path(__file__).resolve().parent / "metrics_reports"


def run_year(year: int) -> dict:
    t0 = time.time()
    dm = build_df_daily(year)

    trade_dates = (dm.df_daily.select(pl.col("date")).unique()
                   .sort("date").to_series().to_list())
    sig = [d for d in trade_dates
           if datetime.date(year, 1, 1) <= d <= datetime.date(year, 12, 31)]
    cal = TradingCalendar(); cal.set_trade_dates(trade_dates)
    store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine, raw_price_store=store,
        fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )

    saved = (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
             dmm.data_manager.df_monthly, dmm.data_manager.df_mapping)
    dmm.data_manager.df_daily = dm.df_daily
    dmm.data_manager.df_weekly = dm.df_weekly
    dmm.data_manager.df_monthly = dm.df_monthly
    dmm.data_manager.df_mapping = dm.df_mapping
    try:
        log(f"{year}: running backtest ...")
        bt0 = time.time()
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=sig[0], end_signal_date=sig[-1],
            initial_cash=1_000_000,
        )
        bt_s = round(time.time() - bt0, 1)
    finally:
        (dmm.data_manager.df_daily, dmm.data_manager.df_weekly,
         dmm.data_manager.df_monthly, dmm.data_manager.df_mapping) = saved

    # ---- 纯后处理 + 双跑确定性检查 ----
    m1 = compute_metrics(result, initial_cash=1_000_000)
    m2 = compute_metrics(result, initial_cash=1_000_000)
    f1, f2 = m1.to_flat_dict(), m2.to_flat_dict()
    assert f1 == f2, "compute_metrics 非确定性！两次结果不一致"
    log(f"{year}: determinism check OK ({len(f1)} fields)")

    return {
        "year": year,
        "bt_s": bt_s,
        "total_s": round(time.time() - t0, 1),
        "signal_start": sig[0].isoformat(),
        "signal_end": sig[-1].isoformat(),
        "valuation_start": result.equity_curve["date"].min().isoformat(),
        "valuation_end": result.equity_curve["date"].max().isoformat(),
        "flat": f1,
        "diag": {
            "intents_total": result.execution_diagnostics["intents_total"],
            "rej_counters": result.execution_diagnostics["rej_counters"],
            "partial_fill_count": result.execution_diagnostics["partial_fill_count"],
            "carried_events": result.execution_diagnostics["carried_events"],
            "codes_universe": dm.df_daily["code"].n_unique(),
            "df_range": f"{dm.df_daily['date'].min()}..{dm.df_daily['date'].max()}",
        },
    }


def report(R: dict):
    f, y = R["flat"], R["year"]
    P = lambda g, k: f[f"{g}.{k}"]
    print(f"\n================ {y} 四层 Metrics 报告 ================", flush=True)
    print(f"[输入范围] signal {R['signal_start']} .. {R['signal_end']}")
    print(f"           valuation {R['valuation_start']} .. {R['valuation_end']}")
    print(f"[Performance] total_return={P('performance','total_return'):.2%} "
          f"annualized={P('performance','annualized_return'):.2%} "
          f"max_dd={P('performance','max_drawdown'):.2%} "
          f"dd_duration={P('performance','drawdown_duration'):.0f}d")
    print(f"[Trading] trades={P('trading','trade_count'):.0f} "
          f"(B{P('trading','buy_count'):.0f}/S{P('trading','sell_count'):.0f}) "
          f"trade_days={P('trading','trade_days'):.0f} "
          f"turnover={P('trading','turnover'):.2f}x "
          f"fees={P('trading','total_fees'):,.2f} "
          f"avg_trade={P('trading','avg_trade_value'):,.2f}")
    print(f"[Trading] active_position_days={P('trading','active_position_days'):.0f} "
          f"gross_buy={P('trading','gross_buy'):,.2f} gross_sell={P('trading','gross_sell'):,.2f}")
    e = "exposure"
    print(f"[Exposure] deployment min/p10/p25/p50/mean/p75/p90/max = "
          f"{P(e,'deployment_min'):.2%}/{P(e,'deployment_p10'):.2%}/"
          f"{P(e,'deployment_p25'):.2%}/{P(e,'deployment_median'):.2%}/"
          f"{P(e,'deployment_mean'):.2%}/{P(e,'deployment_p75'):.2%}/"
          f"{P(e,'deployment_p90'):.2%}/{P(e,'deployment_max'):.2%}")
    print(f"[Exposure] cash_drag={P(e,'cash_drag'):.2%} "
          f"target_fill_ratio={P(e,'target_fill_ratio'):.4f}")
    q = "execution_quality"
    print(f"[ExecQuality] dust_reject={P(q,'dust_reject_count'):.0f} "
          f"({P(q,'dust_reject_ratio'):.2%}) partial={P(q,'partial_fill_count'):.0f} "
          f"({P(q,'partial_fill_ratio'):.2%}) limit_blocked={P(q,'limit_blocked_count'):.0f} "
          f"({P(q,'limit_blocked_ratio'):.2%}) carried={P(q,'carried_events'):.0f} "
          f"({P(q,'carried_event_ratio'):.4%})")
    ig = "integrity"
    ints = [P(ig, k) for k in ("zero_price_trade_count", "t1_violation_count",
                               "negative_cash_count", "accounting_invariant_violations")]
    print(f"[Integrity] zero_price={ints[0]:.0f} t1_violation={ints[1]:.0f} "
          f"negative_cash={ints[2]:.0f} invariant_violations={ints[3]:.0f} "
          f"{'[全零 PASS]' if sum(ints)==0 else '[非零 FAIL]'})")
    # 联动链
    print(f"[联动链] dust={P(q,'dust_reject_ratio'):.2%} → deploy_mean="
          f"{P(e,'deployment_mean'):.2%} → cash_drag={P(e,'cash_drag'):.2%} "
          f"→ return={P('performance','total_return'):.2%} / dd={P('performance','max_drawdown'):.2%}")
    print(f"[耗时] 回测={R['bt_s']}s 总计={R['total_s']}s", flush=True)


if __name__ == "__main__":
    OUT_DIR.mkdir(exist_ok=True)
    results = []
    for y in (2024, 2025):
        R = run_year(y)
        report(R)
        payload = {
            "meta": {k: R[k] for k in ("year", "signal_start", "signal_end",
                                       "valuation_start", "valuation_end",
                                       "bt_s", "total_s")},
            "universe": R["diag"]["codes_universe"],
            "diagnostics_raw": R["diag"],
            "metrics": R["flat"],
        }
        out = OUT_DIR / f"{y}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        log(f"saved -> {out}")
        results.append(R)
        gc.collect()

    # 双年联动对比
    a, b = results
    print("\n================ 双年联动对比 ================", flush=True)
    rows = [
        ("dust_reject_ratio", "execution_quality.dust_reject_ratio"),
        ("deployment_mean", "exposure.deployment_mean"),
        ("cash_drag", "exposure.cash_drag"),
        ("target_fill_ratio", "exposure.target_fill_ratio"),
        ("turnover", "trading.turnover"),
        ("total_return", "performance.total_return"),
        ("max_drawdown", "performance.max_drawdown"),
    ]
    print(f"{'指标':<22}{'2024':>14}{'2025':>14}")
    for name, key in rows:
        va, vb = a["flat"][key], b["flat"][key]
        fmt = f"{va:.2%}" if abs(va) <= 10 else f"{va:.2f}"
        fmtb = f"{vb:.2%}" if abs(vb) <= 10 else f"{vb:.2f}"
        print(f"{name:<22}{fmt:>14}{fmtb:>14}")
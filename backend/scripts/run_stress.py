"""Full market long-run stress test for BlinkQuant with stage-level profiler.

Usage:
    python backend/scripts/run_stress.py --start 2019 --end 2024 --output benchmarks/B3_2019_2024.json
    python backend/scripts/run_stress.py --start 2010 --end 2024 --output benchmarks/B4_2010_2024.json
"""

import argparse
import datetime
import gc
import json
import os
import sys
import time
import tracemalloc
from pathlib import Path

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from huggingface_hub import hf_hub_download

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

REPO = os.getenv("HF_REPO", "scanli/stocka-data")
TOKEN = os.getenv("HF_TOKEN", os.getenv("HF_TOKEN"))
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_rss_mb():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        try:
            import resource
            usage = resource.getrusage(resource.RUSAGE_SELF)
            return usage.ru_maxrss / 1024
        except (ImportError, AttributeError):
            return 0


def _format_bytes(b):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def load_year_data(year, hf_repo, token):
    path = hf_hub_download(
        repo_id=hf_repo,
        filename=f"stock_kline_{year}.parquet",
        repo_type="dataset",
        token=token,
        endpoint=HF_ENDPOINT,
    )
    scan = pl.scan_parquet(path)
    available = scan.collect_schema().names()
    use_cols = [c for c in KEEP_COLS if c in available]
    df = scan.select(use_cols).collect()
    return df


def _print_stage_report(stage_timings: dict, total_backtest: float, data_time: float):
    """Print stage-level profiler breakdown."""
    _log("=" * 70)
    _log("STAGE PROFILER BREAKDOWN")
    _log("=" * 70)

    # Add Data stage
    all_stages = {"Data": data_time}
    all_stages.update(stage_timings)

    total = sum(all_stages.values())
    _log(f"{'Stage':<15} {'Time (s)':>10} {'%':>8} {'Bar'}")
    _log("-" * 70)
    for name, t in all_stages.items():
        pct = (t / total * 100) if total > 0 else 0
        bar = "â–? * int(pct / 2)
        _log(f"  {name:<13} {t:>10.1f} {pct:>7.1f}% {bar}")
    _log("-" * 70)
    _log(f"  {'TOTAL':<13} {total:>10.1f} 100.0%")
    _log("=" * 70)


def run_stress_test(
    formula: str = "CLOSE > MA(CLOSE, 20)",
    start_date: datetime.date = datetime.date(2010, 1, 4),
    end_signal_date: datetime.date = datetime.date(2024, 12, 31),
    initial_cash: float = 10_000_000,
    rebalance_freq: str = "weekly",
    top_n: int = 20,
    hf_repo: str = "scanli/stocka-data",
    hf_token: str = None,
    output_file: str = None,
) -> dict:
    """Run stress test with stage-level profiler."""

    _log(f"Stress test: {formula}")
    _log(f"Period: {start_date} .. {end_signal_date}")

    tracemalloc.start()

    # Phase 1: Data loading
    _log(f"\nPhase 1: Data loading")
    data_start = time.time()

    dm = DataManager()
    token = hf_token or TOKEN

    # Determine years needed (MA20 needs ~60 days history)
    history_start = start_date - datetime.timedelta(days=60)
    needed_years = list(range(history_start.year, end_signal_date.year + 1))

    parts = []
    memory_per_year = []
    for year in needed_years:
        year_start = time.time()
        df = load_year_data(year, hf_repo, token)
        parts.append(df)
        year_time = time.time() - year_start
        rss_mb = get_rss_mb()
        memory_per_year.append({
            "year": year, "rows": df.height,
            "codes": df["code"].n_unique(),
            "load_time_s": round(year_time, 2),
            "rss_mb": round(rss_mb, 1),
        })
        _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes, "
             f"{year_time:.1f}s, RSS={rss_mb:.0f}MB")

    df = pl.concat(parts, how="diagonal")
    del parts
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))
    df = df.sort(["code", "date"])

    dm.df_daily = df
    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()

    data_manager.df_daily = dm.df_daily
    data_manager.df_weekly = dm.df_weekly
    data_manager.df_monthly = dm.df_monthly
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    selection_engine._set_cache.clear()

    calendar = TradingCalendar()
    trade_dates = sorted(
        dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar.set_trade_dates(trade_dates)

    # Trim end_signal_date
    if trade_dates and end_signal_date >= trade_dates[-1]:
        end_signal_date = trade_dates[-2]
        _log(f"  Trimmed end_signal_date to {end_signal_date}")

    data_time = time.time() - data_start
    n_codes = dm.df_daily["code"].n_unique()
    _log(f"\n  Total universe: {n_codes} codes")
    _log(f"  Trade dates: {len(trade_dates)} ({trade_dates[0]} .. {trade_dates[-1]})")
    _log(f"  Data load time: {data_time:.1f}s")

    # Phase 2: Backtest execution
    _log(f"\nPhase 2: Backtest execution")
    raw_store = RawPriceStore(hf_repo_id=hf_repo, hf_token=token)
    fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
    allocator = top_n_equal_weight_allocator(top_n)

    engine = BacktestEngine(
        calendar=calendar, selection_engine=selection_engine,
        raw_price_store=raw_store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
    )

    bt_start = time.time()

    result = engine.run(
        formula=formula,
        start_date=start_date,
        end_signal_date=end_signal_date,
        initial_cash=initial_cash,
        rebalance_freq=rebalance_freq,
        fee_schedule=fee_schedule,
    )

    bt_time = time.time() - bt_start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Get stage timings from engine
    stage_timings = getattr(engine, '_stage_timings', {})
    final_rss_mb = get_rss_mb()

    valuation_days = result.equity_curve.height
    n_trades = result.trades.height
    n_signals = result.equity_curve.filter(pl.col("signal_date").is_not_null()).height
    final_equity = float(result.equity_curve["equity"].tail(1)[0]) if result.equity_curve.height > 0 else 0
    has_negative_cash = bool(result.equity_curve.filter(pl.col("cash") < 0).height > 0)

    # Print stage profiler
    _print_stage_report(stage_timings, bt_time, data_time)

    # Build benchmark result
    benchmark = {
        "id": f"B{'3' if start_date.year == 2019 else '4'}",
        "period": f"{start_date.year}-{end_signal_date.year}",
        "start_date": str(start_date),
        "end_signal_date": str(end_signal_date),
        "universe": n_codes,
        "valuation_days": valuation_days,
        "n_signals": n_signals,
        "n_trades": n_trades,
        "final_equity": round(final_equity, 2),
        "data_load_sec": round(data_time, 2),
        "backtest_sec": round(bt_time, 2),
        "total_sec": round(data_time + bt_time, 2),
        "peak_tracemalloc": _format_bytes(peak),
        "peak_tracemalloc_bytes": peak,
        "final_rss_mb": round(final_rss_mb, 1),
        "has_negative_cash": has_negative_cash,
        "stage_timings": {k: round(v, 3) for k, v in stage_timings.items()},
        "stage_pcts": {k: round(v / bt_time * 100, 1) if bt_time > 0 else 0
                       for k, v in stage_timings.items()},
        "memory_per_year": memory_per_year,
    }

    # Save
    if output_file:
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(benchmark, indent=2, default=str), encoding="utf-8")
        _log(f"\nSaved: {out_path}")

    _log(f"\n{'='*60}")
    _log(f"Stress test complete")
    _log(f"  Valuation days: {valuation_days}")
    _log(f"  Trades: {n_trades}")
    _log(f"  Final equity: {final_equity:,.2f}")
    _log(f"  Backtest time: {bt_time:.1f}s ({bt_time/60:.1f} min)")
    _log(f"  Peak memory: {_format_bytes(peak)}")
    _log(f"  Final RSS: {final_rss_mb:.0f} MB")

    return benchmark


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlinkQuant Stress Test")
    parser.add_argument("--start", type=str, default="2010-01-04")
    parser.add_argument("--end", type=str, default="2024-12-31")
    parser.add_argument("--formula", type=str, default="CLOSE > MA(CLOSE, 20)")
    parser.add_argument("--rebalance", type=str, default="weekly", choices=["daily", "weekly"])
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--cash", type=float, default=10_000_000)
    parser.add_argument("--output", type=str, default=None)

    args = parser.parse_args()

    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint
        print(f"Using HF mirror: {hf_endpoint}")

    run_stress_test(
        formula=args.formula,
        start_date=datetime.date.fromisoformat(args.start),
        end_signal_date=datetime.date.fromisoformat(args.end),
        initial_cash=args.cash,
        rebalance_freq=args.rebalance,
        top_n=args.top_n,
        output_file=args.output,
    )

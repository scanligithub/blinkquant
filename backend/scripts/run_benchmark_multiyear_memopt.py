"""Memory-optimized multi-year benchmark for BlinkQuant (B4).

Optimizations vs run_benchmark_multiyear.py:
- Float32 for price/volume columns (50% memory reduction)
- Drop unnecessary columns early
- Clear intermediate DataFrames after use
- Force garbage collection between phases

Usage:
    python backend/scripts/run_benchmark_multiyear_memopt.py --start 2010 --end 2024 --output benchmarks/B4_2010_2024.json
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

REPO = os.getenv("HF_REPO", "scanli/stocka-data")
TOKEN = os.getenv("HF_TOKEN", os.getenv("HF_TOKEN"))
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")

# Only keep columns we actually need
KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

# Float32 columns (saves ~50% memory)
FLOAT32_COLS = ["open", "high", "low", "close", "volume", "amount",
                "adjustFactor", "pctChg"]


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _format_bytes(b):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def get_rss_mb():
    """Get current process RSS in MB."""
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


def load_year_data_optimized(year, hf_repo, token):
    """Download and load one year's data with memory optimization."""
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

    # Cast float64 to float32 to save memory
    for col in FLOAT32_COLS:
        if col in df.columns and df[col].dtype == pl.Float64:
            df = df.with_columns(pl.col(col).cast(pl.Float32))

    # Cast isST to bool (saves memory vs string)
    if "isST" in df.columns:
        df = df.with_columns(pl.col("isST").cast(pl.Boolean))

    return df


def run_benchmark_optimized(start_year, end_year, top_n=20, cash=10_000_000):
    """Run multi-year benchmark with memory optimizations."""
    start_date = datetime.date(start_year, 1, 2)
    end_signal_date = datetime.date(end_year, 12, 31)

    _log(f"Benchmark (memopt): {start_year}-{end_year}")
    _log(f"Period: {start_date} .. {end_signal_date}")

    # Phase 1: Load data year by year
    _log(f"\nPhase 1: Data loading (Float32 optimized)")
    load_start = time.time()

    dm = DataManager()
    parts = []
    memory_per_year = []

    for year in range(start_year, end_year + 2):  # +1 for T+1 lookahead
        year_start = time.time()
        try:
            df = load_year_data_optimized(year, REPO, TOKEN)
            parts.append(df)
            year_time = time.time() - year_start

            rss_mb = get_rss_mb()
            memory_per_year.append({
                "year": year,
                "rows": df.height,
                "codes": df["code"].n_unique(),
                "load_time_s": round(year_time, 2),
                "rss_mb": round(rss_mb, 1),
            })
            _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes, "
                 f"{year_time:.1f}s, RSS={rss_mb:.0f}MB")
        except Exception as e:
            _log(f"  {year}: FAILED â€?{type(e).__name__}: {e}")
            raise

    load_time = time.time() - load_start

    # Combine all data
    _log(f"\nPhase 1b: Combining data")
    df = pl.concat(parts, how="diagonal")
    del parts  # Free memory
    gc.collect()

    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))
    df = df.sort(["code", "date"])

    rss_after_load = get_rss_mb()
    _log(f"  Combined: {df.height} rows, RSS={rss_after_load:.0f}MB")

    # Initialize DataManager
    _log(f"\nPhase 1c: DataManager initialization")
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

    gc.collect()
    rss_after_init = get_rss_mb()
    _log(f"  After DataManager init: RSS={rss_after_init:.0f}MB")

    # Trading calendar
    calendar = TradingCalendar()
    trade_dates = sorted(
        dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar.set_trade_dates(trade_dates)

    # Trim end_signal_date
    if trade_dates and end_signal_date >= trade_dates[-1]:
        end_signal_date = trade_dates[-2]
        _log(f"  Trimmed end_signal_date to {end_signal_date}")

    n_codes = dm.df_daily["code"].n_unique()
    _log(f"\n  Total universe: {n_codes} codes")
    _log(f"  Trade dates: {len(trade_dates)} ({trade_dates[0]} .. {trade_dates[-1]})")
    _log(f"  Data load time: {load_time:.1f}s")

    # Phase 2: Run backtest
    _log(f"\nPhase 2: Backtest execution")
    raw_store = RawPriceStore(hf_repo_id=REPO, hf_token=TOKEN)
    fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
    allocator = top_n_equal_weight_allocator(top_n)

    engine = BacktestEngine(
        calendar=calendar, selection_engine=selection_engine,
        raw_price_store=raw_store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
    )

    tracemalloc.start()
    bt_start = time.time()

    result = engine.run(
        formula="CLOSE > MA(CLOSE, 20)",
        start_date=start_date,
        end_signal_date=end_signal_date,
        initial_cash=cash,
        rebalance_freq="weekly",
        fee_schedule=fee_schedule,
    )

    bt_time = time.time() - bt_start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Final memory
    rss_final = get_rss_mb()

    valuation_days = result.equity_curve.height
    n_trades = result.trades.height
    n_signals = result.equity_curve.filter(pl.col("signal_date").is_not_null()).height
    final_equity = float(result.equity_curve["equity"].tail(1)[0]) if result.equity_curve.height > 0 else 0

    # Check for accounting violations
    has_negative_cash = bool(result.equity_curve.filter(pl.col("cash") < 0).height > 0)

    # Build result
    benchmark = {
        "id": f"B{3 if start_year == 2019 else 4}",
        "period": f"{start_year}-{end_year}",
        "start_date": str(start_date),
        "end_signal_date": str(end_signal_date),
        "universe": n_codes,
        "valuation_days": valuation_days,
        "n_signals": n_signals,
        "n_trades": n_trades,
        "final_equity": round(final_equity, 2),
        "data_load_sec": round(load_time, 2),
        "backtest_sec": round(bt_time, 2),
        "total_sec": round(load_time + bt_time, 2),
        "peak_tracemalloc": _format_bytes(peak),
        "peak_tracemalloc_bytes": peak,
        "peak_rss_mb": round(rss_final, 1),
        "has_negative_cash": has_negative_cash,
        "memory_per_year": memory_per_year,
    }

    _log(f"\n{'='*60}")
    _log(f"Benchmark complete (memopt)")
    _log(f"{'='*60}")
    _log(f"  Valuation days: {valuation_days}")
    _log(f"  Trades: {n_trades}")
    _log(f"  Final equity: {final_equity:,.2f}")
    _log(f"  Backtest time: {bt_time:.1f}s")
    _log(f"  Peak tracemalloc: {_format_bytes(peak)}")
    _log(f"  Peak RSS: {rss_final:.0f} MB")
    _log(f"  Negative cash: {has_negative_cash}")

    return benchmark


def main():
    parser = argparse.ArgumentParser(description="Memory-optimized multi-year benchmark")
    parser.add_argument("--start", type=int, required=True, help="Start year")
    parser.add_argument("--end", type=int, required=True, help="End year")
    parser.add_argument("--output", type=str, required=True, help="Output JSON path")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--cash", type=float, default=10_000_000)
    args = parser.parse_args()

    _log(f"Benchmark (memopt): {args.start}-{args.end} â†?{args.output}")

    benchmark = run_benchmark_optimized(args.start, args.end, args.top_n, args.cash)

    # Save
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(benchmark, indent=2, default=str), encoding="utf-8")
    _log(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()

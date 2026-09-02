"""Long-run benchmark harness for BlinkQuant."""

import argparse
import datetime
import json
import os
import sys
import time
import tracemalloc
from pathlib import Path

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from huggingface_hub import hf_hub_download
import polars as pl


KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def _format_bytes(b):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def load_data(start_date, end_date, hf_repo, token):
    dm = DataManager()
    history_start = start_date - datetime.timedelta(days=60)
    # Load one extra year for T+1 execution on last trade day
    needed_years = list(range(history_start.year, end_date.year + 2))

    parts = []
    for year in needed_years:
        try:
            _log(f"Downloading {hf_repo}/stock_kline_{year}.parquet ...")
            p = hf_hub_download(
                repo_id=hf_repo, filename=f"stock_kline_{year}.parquet",
                repo_type="dataset", token=token,
            )
            scan = pl.scan_parquet(p)
            available = scan.collect_schema().names()
            use_cols = [c for c in KEEP_COLS if c in available]
            df = scan.select(use_cols).collect()
            df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
            parts.append(df)
            _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes")
        except Exception as e:
            _log(f"  {year}: SKIPPED ({type(e).__name__})")
            continue

    if not parts:
        raise RuntimeError(f"No data loaded for years {needed_years}")

    df = pl.concat(parts, how="diagonal")
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))
    dm.df_daily = df.sort(["code", "date"])
    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()
    return dm


def run_benchmark(start_date, end_signal_date, hf_repo, token, top_n=20, cash=10_000_000):
    _log(f"Loading data for {start_date} .. {end_signal_date}")
    load_start = time.time()
    dm = load_data(start_date, end_signal_date, hf_repo, token)
    load_time = time.time() - load_start

    data_manager.df_daily = dm.df_daily
    data_manager.df_weekly = dm.df_weekly
    data_manager.df_monthly = dm.df_monthly
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()

    calendar = TradingCalendar()
    trade_dates = sorted(
        dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar.set_trade_dates(trade_dates)

    # Trim end_signal_date so last signal has a next trade day for T+1 execution
    if trade_dates and end_signal_date >= trade_dates[-1]:
        end_signal_date = trade_dates[-2]
        _log(f"  Trimmed end_signal_date to {end_signal_date} (second-to-last trade day)")

    raw_store = RawPriceStore(hf_repo_id=hf_repo, hf_token=token)
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

    n_codes = dm.df_daily["code"].n_unique()
    n_trade_days_raw = len(trade_dates)
    lookback_start = start_date - datetime.timedelta(days=60)
    effective_data_start = max(lookback_start, trade_dates[0]) if trade_dates else lookback_start
    valuation_days = result.equity_curve.height

    return {
        "universe": n_codes,
        "requested_period": f"{start_date}..{end_signal_date}",
        "lookback_period": f"{lookback_start}..{start_date - datetime.timedelta(days=1)}",
        "effective_data_period": f"{effective_data_start}..{trade_dates[-1] if trade_dates else end_signal_date}",
        "valuation_days": valuation_days,
        "n_trade_days_raw": n_trade_days_raw,
        "n_signals": result.equity_curve.filter(pl.col("signal_date").is_not_null()).height,
        "n_trades": result.trades.height,
        "final_equity": float(result.equity_curve["equity"].tail(1)[0]) if result.equity_curve.height > 0 else 0,
        "data_load_sec": round(load_time, 2),
        "backtest_sec": round(bt_time, 2),
        "total_sec": round(load_time + bt_time, 2),
        "peak_memory": _format_bytes(peak),
        "peak_memory_bytes": peak,
    }


def main():
    parser = argparse.ArgumentParser(description="BlinkQuant Benchmark")
    parser.add_argument("--periods", type=str, default="q1")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--hf-repo", type=str, default="scanli/stocka-data")
    parser.add_argument("--output-dir", type=str, default="tests/benchmarks")
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN")
    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint

    periods = {
        "q1": (datetime.date(args.year, 1, 2), datetime.date(args.year, 3, 29)),
        "q2": (datetime.date(args.year, 4, 1), datetime.date(args.year, 6, 28)),
        "q3": (datetime.date(args.year, 7, 1), datetime.date(args.year, 9, 30)),
        "q4": (datetime.date(args.year, 10, 1), datetime.date(args.year, 12, 31)),
        "1y": (datetime.date(args.year, 1, 2), datetime.date(args.year, 12, 31)),
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = []
    for period_name in args.periods.split(","):
        if period_name not in periods:
            _log(f"Unknown period: {period_name}, skipping")
            continue

        start, end = periods[period_name]
        _log(f"\n{'='*50}")
        _log(f"Benchmark: {period_name} ({start} .. {end})")
        _log(f"{'='*50}")

        try:
            result = run_benchmark(start, end, args.hf_repo, token)
            all_results.append(result)

            out_file = output_dir / f"benchmark_{period_name}_{args.year}.json"
            out_file.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
            _log(f"Saved: {out_file}")

            _log(f"  Universe: {result['universe']}")
            _log(f"  Valuation days: {result['valuation_days']}")
            _log(f"  Trades: {result['n_trades']}")
            _log(f"  Final equity: {result['final_equity']:,.2f}")
            _log(f"  Backtest time: {result['backtest_sec']}s")
            _log(f"  Peak memory: {result['peak_memory']}")
        except Exception as e:
            _log(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
            all_results.append({"period": period_name, "error": str(e)})

    summary_file = output_dir / f"benchmark_summary_{args.year}.json"
    summary_file.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    _log(f"\nSummary saved: {summary_file}")


if __name__ == "__main__":
    main()

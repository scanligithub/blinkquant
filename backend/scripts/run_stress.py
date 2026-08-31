"""Full market long-run stress test for BlinkQuant."""

import argparse
import datetime
import gc
import json
import os
import sys
import time
import tracemalloc
from pathlib import Path

# Ensure backend root is on sys.path
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


def _load_df_daily(
    start_date: datetime.date,
    end_signal_date: datetime.date,
    hf_repo: str,
    token: str,
    dm: DataManager,
) -> None:
    """Download year-sharded kline parquets from HF, build df_daily in-memory.

    Reproduces the production transform pipeline:
    download → shard (node 0) → column prune → limit flags → forward-adjust → resample.
    """
    # Determine which years are needed (MA20 needs ~40 trading days of history before start).
    history_start = start_date - datetime.timedelta(days=60)
    needed_years = list(range(history_start.year, end_signal_date.year + 1))

    parts: list[pl.DataFrame] = []
    for year in needed_years:
        _log(f"downloading {hf_repo}/stock_kline_{year}.parquet ...")
        p = hf_hub_download(
            repo_id=hf_repo,
            filename=f"stock_kline_{year}.parquet",
            repo_type="dataset",
            token=token,
        )
        # Read with column pruning where possible.
        scan = pl.scan_parquet(p)
        available = scan.collect_schema().names()
        use_cols = [c for c in KEEP_COLS if c in available]
        df = scan.select(use_cols).collect()

        # Shard to node 0 (consistent with production single-node mode).
        df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
        parts.append(df)
        _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes")
        del df

    df = pl.concat(parts, how="diagonal")
    del parts
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))

    dm.df_daily = df.sort(["code", "date"])
    _log(f"df_daily: {dm.df_daily.height} rows, "
         f"{dm.df_daily['code'].n_unique()} codes, "
         f"range {dm.df_daily['date'].min()}..{dm.df_daily['date'].max()}")

    dm._compute_limit_flags()
    dm._apply_forward_adjustment()
    dm._append_prev_close()
    dm._optimize_memory(dm.df_daily, "df_daily")
    dm._resample_all()


def _log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def _format_bytes(b: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def run_stress_test(
    formula: str = "CLOSE > MA(CLOSE, 20)",
    start_date: datetime.date = datetime.date(2010, 1, 4),
    end_signal_date: datetime.date = datetime.date(2024, 12, 31),
    initial_cash: float = 10_000_000,
    rebalance_freq: str = "weekly",
    top_n: int = 20,
    hf_repo: str = "scanli/stocka-data",
    hf_token: str = None,
    output_dir: str = None,
    checkpoint_every: int = 500,  # trading days
) -> dict:
    """Run full market stress test with memory/performance monitoring."""
    
    _log(f"Starting stress test: {formula}")
    _log(f"Period: {start_date} .. {end_signal_date}")
    _log(f"Rebalance: {rebalance_freq} | Top-N: {top_n} | Cash: {initial_cash:,.0f}")
    
    # Start memory tracking
    tracemalloc.start()
    
    # Load data
    _log("Loading HF data...")
    load_start = time.time()
    dm = DataManager()
    token = hf_token or os.getenv("HF_TOKEN")
    _load_df_daily(start_date, end_signal_date, hf_repo, token, dm)
    load_time = time.time() - load_start
    _log(f"Data loaded in {load_time:.1f}s: {dm.df_daily.height} rows, {dm.df_daily['code'].n_unique()} codes")
    
    # Wire globals
    data_manager.df_daily = dm.df_daily
    data_manager.df_weekly = dm.df_weekly
    data_manager.df_monthly = dm.df_monthly
    data_manager.df_mapping = None
    data_manager._asof_frame_cache.clear()
    data_manager._resample_all()
    selection_engine._set_cache.clear()
    
    # Calendar
    calendar = TradingCalendar()
    trade_dates = sorted(
        dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar.set_trade_dates(trade_dates)
    
    # Build engine
    raw_store = RawPriceStore(hf_repo_id=hf_repo, hf_token=token)
    fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
    allocator = top_n_equal_weight_allocator(top_n)
    
    engine = BacktestEngine(
        calendar=calendar,
        selection_engine=selection_engine,
        raw_price_store=raw_store,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=allocator,
    )
    
    _log("Starting backtest...")
    bt_start = time.time()
    
    # Progress tracking
    signal_dates = list(engine.calendar.signal_range(start_date, end_signal_date))
    if rebalance_freq == "weekly":
        signal_dates = [d for d in signal_dates if d in engine.calendar.weekly_signal_dates(start_date, end_signal_date)]
    
    _log(f"Signal dates: {len(signal_dates)}")
    
    # Run with periodic checkpoint
    checkpoint_dir = Path(output_dir) / "checkpoints" if output_dir else None
    if checkpoint_dir:
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # We'll run in segments for checkpointing
    segment_size = checkpoint_every
    all_results = []
    
    for i in range(0, len(signal_dates), segment_size):
        seg_start = signal_dates[i]
        seg_end = signal_dates[min(i + segment_size - 1, len(signal_dates) - 1)]
        
        _log(f"Segment {i//segment_size + 1}: {seg_start} .. {seg_end}")
        
        # Memory check
        current, peak = tracemalloc.get_traced_memory()
        _log(f"  Memory: current={_format_bytes(current)}, peak={_format_bytes(peak)}")
        
        # Run segment
        seg_start_time = time.time()
        
        result = engine.run(
            formula=formula,
            start_date=seg_start if i == 0 else signal_dates[i],
            end_signal_date=seg_end,
            initial_cash=initial_cash if i == 0 else 0,
            rebalance_freq=rebalance_freq,
            fee_schedule=load_fee_schedule("config/fee_schedule.yaml"),
        )
        
        seg_time = time.time() - seg_start_time
        _log(f"  Segment time: {seg_time:.1f}s | Trades: {result.trades.height} | Equity pts: {result.equity_curve.height}")
        
        all_results.append({
            "segment": i // segment_size + 1,
            "start": seg_start.isoformat(),
            "end": seg_end.isoformat(),
            "time_sec": seg_time,
            "trades": result.trades.height,
            "equity_points": result.equity_curve.height,
            "final_equity": float(result.equity_curve["equity"].tail(1)[0]) if result.equity_curve.height > 0 else 0,
            "final_cash": float(result.equity_curve["cash"].tail(1)[0]) if result.equity_curve.height > 0 else 0,
        })
        
        # Save checkpoint
        if checkpoint_dir:
            engine.save_checkpoint(checkpoint_dir / f"cp_{signal_dates[min(i + segment_size - 1, len(signal_dates) - 1)].isoformat()}", signal_dates[min(i + segment_size - 1, len(signal_dates) - 1)])
            _log(f"  Checkpoint saved")
        
        # Force GC
        gc.collect()
    
    total_time = time.time() - bt_start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # Aggregate results
    total_trades = sum(r["trades"] for r in all_results)
    final_result = all_results[-1] if all_results else {}
    
    summary = {
        "total_time_sec": total_time,
        "signal_dates": len(signal_dates),
        "total_trades": total_trades,
        "final_equity": final_result.get("final_equity", 0),
        "final_cash": final_result.get("final_cash", 0),
        "peak_memory_bytes": peak,
        "peak_memory_human": _format_bytes(peak),
        "segments": all_results,
    }
    
    _log("=" * 50)
    _log("STRESS TEST COMPLETE")
    _log(f"Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
    _log(f"Signal dates: {len(signal_dates)}")
    _log(f"Total trades: {total_trades}")
    _log(f"Final equity: {summary['final_equity']:,.2f}")
    _log(f"Final cash: {summary['final_cash']:,.2f}")
    _log(f"Peak memory: {_format_bytes(peak)}")
    _log("=" * 50)
    
    return summary


def _log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def _format_bytes(b: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlinkQuant Stress Test")
    parser.add_argument("--start", type=str, default="2010-01-04", help="Start date")
    parser.add_argument("--end", type=str, default="2024-12-31", help="End signal date")
    parser.add_argument("--formula", type=str, default="CLOSE > MA(CLOSE, 20)")
    parser.add_argument("--rebalance", type=str, default="weekly", choices=["daily", "weekly"])
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--cash", type=float, default=10_000_000)
    parser.add_argument("--hf-repo", type=str, default="scanli/stocka-data")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--checkpoint-every", type=int, default=500)
    
    args = parser.parse_args()
    
    # Env
    token = os.getenv("HF_TOKEN")
    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint
        print(f"Using HF mirror: {hf_endpoint}")
    
    summary = run_stress_test(
        formula=args.formula,
        start_date=datetime.date.fromisoformat(args.start),
        end_signal_date=datetime.date.fromisoformat(args.end),
        initial_cash=args.cash,
        rebalance_freq=args.rebalance,
        top_n=args.top_n,
        hf_repo=args.hf_repo,
        hf_token=token,
        output_dir=args.output_dir,
        checkpoint_every=args.checkpoint_every,
    )
    
    # Save summary
    if args.output_dir:
        out_file = Path(args.output_dir) / f"stress_summary_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(summary, default=str, indent=2), encoding="utf-8")
        print(f"Summary saved: {out_file}")
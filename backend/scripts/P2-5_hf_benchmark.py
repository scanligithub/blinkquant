"""P2-5: B1-B4 benchmark with real process RSS measurement."""
import sys, os, datetime, time, json

_BACKEND_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, _BACKEND_ROOT)
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
import psutil
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from core.resource_guard import ResourceGuard
from huggingface_hub import hf_hub_download

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def get_rss_mb():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

def run_benchmark(name, start_year, end_year, guard):
    log(f"\n{'='*60}")
    log(f"Benchmark: {name} ({start_year}-{end_year})")
    log(f"{'='*60}")

    with guard.run(name) as ctx:
        # Load data
        log("Phase 1: Data loading")
        t_load_start = time.time()
        dm = DataManager()
        parts = []
        for year in range(start_year - 1, end_year + 1):
            try:
                p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                                    repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
                scan = pl.scan_parquet(p)
                available = scan.collect_schema().names()
                use_cols = [c for c in KEEP_COLS if c in available]
                df = scan.select(use_cols).collect()
                parts.append(df)
                log(f"  {year}: {df.height} rows, RSS={get_rss_mb():.0f}MB")
            except Exception as e:
                log(f"  {year}: skipped ({e})")

        df = pl.concat(parts)
        df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
        df = df.sort(['code', 'date'])
        dm.df_daily = df
        dm._compute_limit_flags()
        dm._apply_forward_adjustment()
        dm._append_prev_close()
        dm._optimize_memory(dm.df_daily, 'df_daily')
        dm._resample_all()
        data_manager.df_daily = dm.df_daily
        data_manager.df_weekly = dm.df_weekly
        data_manager.df_monthly = dm.df_monthly
        load_time = time.time() - t_load_start
        log(f"  Data load: {load_time:.1f}s, RSS={get_rss_mb():.0f}MB")

        # Setup engine
        calendar = TradingCalendar()
        trade_dates = sorted(dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list())
        calendar.set_trade_dates(trade_dates)

        selection_engine._set_cache.clear()
        selection_engine._signal_matrix_cache.clear()
        selection_engine._signal_matrix_enabled = True

        raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
        fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
        allocator = top_n_equal_weight_allocator(20)

        engine = BacktestEngine(
            calendar=calendar, selection_engine=selection_engine,
            raw_price_store=raw_store, fee_config=FeeConfig(),
            execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
        )

        # Run backtest
        log("Phase 2: Backtest execution")
        t_bt_start = time.time()
        result = engine.run(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=datetime.date(start_year, 1, 4),
            end_signal_date=datetime.date(end_year, 12, 30),
            initial_cash=10_000_000,
            rebalance_freq="weekly",
            fee_schedule=fee_schedule,
        )
        bt_time = time.time() - t_bt_start
        total_time = time.time() - t_load_start

        # Check memory
        mem = ctx.check()
        rss = get_rss_mb()

        log(f"\nResults:")
        log(f"  Trades: {result.trades.height}")
        log(f"  Final equity: {result.equity_curve['equity'].tail(1).item():,.2f}")
        log(f"  Backtest time: {bt_time:.1f}s ({bt_time/60:.1f} min)")
        log(f"  Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        log(f"  Process RSS: {rss:.1f} MB")
        log(f"  Peak RSS: {mem['memory']['peak_mb']:.1f} MB")
        log(f"  Memory status: {mem['memory']['status']}")

        return {
            "name": name,
            "period": f"{start_year}-{end_year}",
            "trades": result.trades.height,
            "equity": result.equity_curve['equity'].tail(1).item(),
            "load_time_s": load_time,
            "bt_time_s": bt_time,
            "total_time_s": total_time,
            "process_rss_mb": rss,
            "peak_rss_mb": mem['memory']['peak_mb'],
            "memory_status": mem['memory']['status'],
        }

# Run benchmarks
guard = ResourceGuard(warning_mb=4096, critical_mb=8192, hard_stop_mb=12288,
                      timeout_seconds=14400, max_concurrent=1)

results = []
for name, start, end in [
    ("B1", 2024, 2024),
    ("B2", 2024, 2024),
    ("B3", 2019, 2024),
    ("B4", 2010, 2024),
]:
    try:
        r = run_benchmark(name, start, end, guard)
        results.append(r)
    except Exception as e:
        log(f"Benchmark {name} FAILED: {e}")
        results.append({"name": name, "error": str(e)})

# Save results
summary = {
    "benchmarks": results,
    "guard_summary": guard.summary(),
}
with open("benchmarks/P2-5_hf_memory_validation.json", "w") as f:
    json.dump(summary, f, indent=2, default=str)

log(f"\n{'='*60}")
log("SUMMARY")
log(f"{'='*60}")
for r in results:
    if "error" in r:
        log(f"  {r['name']}: FAILED - {r['error']}")
    else:
        log(f"  {r['name']}: {r['total_time_s']:.0f}s, {r['trades']} trades, {r['peak_rss_mb']:.0f}MB peak RSS, {r['memory_status']}")

log(f"\nGuard summary: {guard.summary()}")
log(f"\nSaved: benchmarks/P2-5_hf_memory_validation.json")

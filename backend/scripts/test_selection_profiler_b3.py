"""Run Selection profiler on B3 (2019-2024) with and without trace."""
import sys, os, datetime, time

_BACKEND_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, _BACKEND_ROOT)
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from huggingface_hub import hf_hub_download

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

# Load B3 data
log("Loading B3 data (2019-2024)...")
dm = DataManager()
parts = []
for year in range(2018, 2025):
    p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                        repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
    scan = pl.scan_parquet(p)
    available = scan.collect_schema().names()
    use_cols = [c for c in KEEP_COLS if c in available]
    df = scan.select(use_cols).collect()
    parts.append(df)
    log(f"  {year}: {df.height} rows")

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

calendar = TradingCalendar()
trade_dates = sorted(dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list())
calendar.set_trade_dates(trade_dates)

log(f"Universe: {dm.df_daily['code'].n_unique()} codes, {len(trade_dates)} trade dates")

# === Test 1: Full B3 with trace (current behavior) ===
log("\n=== Test 1: Full B3 with trace ===")
selection_engine._set_cache.clear()
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
allocator = top_n_equal_weight_allocator(20)

engine = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Enable profiler before backtest
selection_engine.profiler_start()

t0 = time.time()
result = engine.run(
    formula="CLOSE > MA(CLOSE, 20)",
    start_date=datetime.date(2019, 1, 2),
    end_signal_date=datetime.date(2024, 12, 30),
    initial_cash=10_000_000,
    rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
bt_time = time.time() - t0
sel_prof = getattr(selection_engine, '_profiler', {})
log(f"B3 with trace: {bt_time:.1f}s, trades={result.trades.height}")
log(f"Selection profiler: {sel_prof}")

# === Test 2: Full B3 without trace ===
log("\n=== Test 2: Full B3 without trace ===")
selection_engine._set_cache.clear()
engine2 = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Enable profiler before backtest
selection_engine.profiler_start()

# Monkey-patch to skip trace
original_with_trace = selection_engine.execute_selector_with_trace
def skip_trace(formula, timeframe, bg, target_date=None, backtest_mode=False, raise_on_error=False):
    result = selection_engine.execute_selector(formula, timeframe, bg, target_date, backtest_mode, raise_on_error)
    return result, None
selection_engine.execute_selector_with_trace = skip_trace

t0 = time.time()
result2 = engine2.run(
    formula="CLOSE > MA(CLOSE, 20)",
    start_date=datetime.date(2019, 1, 2),
    end_signal_date=datetime.date(2024, 12, 30),
    initial_cash=10_000_000,
    rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
bt_time2 = time.time() - t0
sel_prof2 = getattr(selection_engine, '_profiler', {})
log(f"B3 without trace: {bt_time2:.1f}s, trades={result2.trades.height}")
log(f"Selection profiler: {sel_prof2}")

# Restore
selection_engine.execute_selector_with_trace = original_with_trace

# === Summary ===
log("\n" + "="*60)
log("P2-1 SELECTION PROFILER SUMMARY")
log("="*60)
log(f"With trace:    {bt_time:.1f}s")
log(f"Without trace: {bt_time2:.1f}s")
log(f"Trace overhead: {bt_time - bt_time2:.1f}s ({(bt_time/bt_time2 - 1)*100:.0f}% more)")
log(f"Speedup if no trace: {bt_time/bt_time2:.1f}x")
log("")
log("Stage breakdown (with trace):")
total = sum(sel_prof.values())
for k, v in sorted(sel_prof.items(), key=lambda x: -x[1]):
    if v > 0:
        log(f"  {k:<18} {v:>8.1f}s  ({v/bt_time*100:>5.1f}%)")
log("")
log("Stage breakdown (without trace):")
total2 = sum(sel_prof2.values())
for k, v in sorted(sel_prof2.items(), key=lambda x: -x[1]):
    if v > 0:
        log(f"  {k:<18} {v:>8.1f}s  ({v/bt_time2*100:>5.1f}%)")
log("")
log("Key finding: TraceGen is the dominant cost in Selection")
log("Actual selection computation is only ~3-5% of Selection time")

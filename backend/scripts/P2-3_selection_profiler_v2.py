"""P2-3: V2 Selection internal profiler.

Run B3 with trace=False and profile the Selection internals:
- HotJIT, ParseFormula, BuildAsofFrame, AtomEval, BoolFold, DateNorm
- No TraceGen (that's the whole point of P2-2A)
"""
import sys, os, datetime, time, json

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

# === V2 Selection profiler (trace=False) ===
log("\n=== V2 Selection profiler (trace=False) ===")
selection_engine._set_cache.clear()
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
allocator = top_n_equal_weight_allocator(20)

engine = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Enable profiler
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

log(f"\nV2 Selection profiler results:")
total = sum(sel_prof.values())
for k, v in sorted(sel_prof.items(), key=lambda x: -x[1]):
    if v > 0:
        log(f"  {k:<18} {v:>8.1f}s  ({v/bt_time*100:>5.1f}%)")

log(f"\nTotal Selection time: {total:.1f}s")
log(f"Total Backtest time:  {bt_time:.1f}s")
log(f"Trades: {result.trades.height}")
log(f"Equity: {result.portfolio_state.total_equity:,.2f}")

# Save results
results = {
    "selection profiler": sel_prof,
    "total_selection_time": total,
    "total_backtest_time": bt_time,
    "trades": result.trades.height,
    "equity": result.portfolio_state.total_equity,
}
with open("benchmarks/P2-3_selection_profiler_v2.json", "w") as f:
    json.dump(results, f, indent=2, default=str)
log(f"\nSaved: benchmarks/P2-3_selection_profiler_v2.json")

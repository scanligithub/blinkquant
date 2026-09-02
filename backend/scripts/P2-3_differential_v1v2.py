"""P2-3: V1/V2 exact differential at B3 scale.

Compare SelectionResult codes between trace=True (V1) and trace=False (V2).
Verifies zero semantic drift at scale.
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

# === V1: trace=True (execute_selector_with_trace) ===
log("\n=== V1: trace=True ===")
selection_engine._set_cache.clear()
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
allocator = top_n_equal_weight_allocator(20)

engine_v1 = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Force trace=True by monkey-patching
original_execute = selection_engine.execute_selector
def execute_with_trace(formula, timeframe, bg, target_date=None, backtest_mode=False, raise_on_error=False, trace=False):
    return original_execute(formula, timeframe, bg, target_date, backtest_mode, raise_on_error, trace=True)
selection_engine.execute_selector = execute_with_trace

t0 = time.time()
result_v1 = engine_v1.run(
    formula="CLOSE > MA(CLOSE, 20)",
    start_date=datetime.date(2019, 1, 2),
    end_signal_date=datetime.date(2024, 12, 30),
    initial_cash=10_000_000,
    rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
v1_time = time.time() - t0
log(f"V1 (trace=True): {v1_time:.1f}s, trades={result_v1.trades.height}, equity={result_v1.portfolio_state.total_equity:,.2f}")

# Restore
selection_engine.execute_selector = original_execute

# === V2: trace=False (lazy trace) ===
log("\n=== V2: trace=False ===")
selection_engine._set_cache.clear()
engine_v2 = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

t0 = time.time()
result_v2 = engine_v2.run(
    formula="CLOSE > MA(CLOSE, 20)",
    start_date=datetime.date(2019, 1, 2),
    end_signal_date=datetime.date(2024, 12, 30),
    initial_cash=10_000_000,
    rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
v2_time = time.time() - t0
log(f"V2 (trace=False): {v2_time:.1f}s, trades={result_v2.trades.height}, equity={result_v2.portfolio_state.total_equity:,.2f}")

# === Differential comparison ===
log("\n" + "="*60)
log("P2-3 V1/V2 EXACT DIFFERENTIAL")
log("="*60)

# 1. Trade count
v1_trades = result_v1.trades.height
v2_trades = result_v2.trades.height
trades_match = v1_trades == v2_trades
log(f"Trades:      V1={v1_trades}, V2={v2_trades}, match={trades_match}")

# 2. Equity
v1_eq = result_v1.portfolio_state.total_equity
v2_eq = result_v2.portfolio_state.total_equity
eq_match = abs(v1_eq - v2_eq) < 0.01
log(f"Equity:      V1={v1_eq:,.2f}, V2={v2_eq:,.2f}, match={eq_match}")

# 3. SelectionResult codes comparison (sample 5 dates)
log("\nSelection code comparison (sample 5 signal dates):")
formula = "CLOSE > MA(CLOSE, 20)"
sample_dates = [datetime.date(2019, 6, 3), datetime.date(2020, 3, 10),
                datetime.date(2021, 9, 15), datetime.date(2022, 12, 20),
                datetime.date(2024, 6, 3)]

all_match = True
for d in sample_dates:
    # V1: trace=True
    selection_engine._set_cache.clear()
    r1 = selection_engine.execute_selector(formula, 'D', None, target_date=d, backtest_mode=True, trace=True)
    codes1 = r1.codes if hasattr(r1, 'codes') else r1.get('codes', [])

    # V2: trace=False
    selection_engine._set_cache.clear()
    r2 = selection_engine.execute_selector(formula, 'D', None, target_date=d, backtest_mode=True, trace=False)
    codes2 = r2.codes if hasattr(r2, 'codes') else r2.get('codes', [])

    match = set(codes1) == set(codes2)
    if not match:
        all_match = False
        log(f"  {d}: MISMATCH V1={len(codes1)} V2={len(codes2)} diff={set(codes1).symmetric_difference(set(codes2))}")
    else:
        log(f"  {d}: OK ({len(codes1)} codes)")

log(f"\nAll 5 dates match: {all_match}")

# 4. Performance summary
log(f"\nPerformance: V1={v1_time:.1f}s, V2={v2_time:.1f}s, speedup={v1_time/v2_time:.1f}x")

# 5. Final verdict
if trades_match and eq_match and all_match:
    log("\n*** V1/V2 EXACT MATCH â€?zero semantic drift ***")
    verdict = "EXACT_MATCH"
else:
    log("\n*** MISMATCH DETECTED ***")
    verdict = "MISMATCH"

# Save results
results = {
    "v1_time": v1_time,
    "v2_time": v2_time,
    "speedup": v1_time/v2_time,
    "v1_trades": v1_trades,
    "v2_trades": v2_trades,
    "v1_equity": v1_eq,
    "v2_equity": v2_eq,
    "sample_dates_match": all_match,
    "verdict": verdict,
}
with open("benchmarks/P2-3_v1v2_differential.json", "w") as f:
    json.dump(results, f, indent=2, default=str)
log(f"\nSaved: benchmarks/P2-3_v1v2_differential.json")

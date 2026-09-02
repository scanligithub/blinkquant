#!/usr/bin/env python3
"""Full vs Lazy backtest comparison: find first divergence point.
Runs both paths with identical parameters, compares trades and portfolio state."""
import sys, os, datetime, json, time
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.fee_config import load_fee_schedule
from huggingface_hub import hf_hub_download

FORMULA = "CLOSE > MA(CLOSE, 20)"
START = datetime.date(2024, 1, 1)
END = datetime.date(2024, 12, 27)  # Last trading day before year-end
INITIAL_CASH = 10_000_000

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def top_n_allocator(n):
    def allocator(codes, signal_date):
        if not codes:
            return {}
        picked = codes[:n]
        return {c: 1.0 / len(picked) for c in picked}
    return allocator

# ============================================================
# PATH A: Full-load backtest
# ============================================================
log("=== PATH A: Full-load ===")
t0 = time.time()

df_all = None
for year in range(2009, 2025):
    try:
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        year_df = pl.scan_parquet(p).collect()
        use_cols = [c for c in ["date","code","open","high","low","close","volume","amount","adjustFactor","pctChg","isST"] if c in year_df.columns]
        year_df = year_df.select(use_cols)
        if df_all is None:
            df_all = year_df
        else:
            df_all = pl.concat([df_all, year_df])
    except:
        pass
df_all = df_all.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df_all = df_all.sort(['code', 'date'])

# Apply QFQ
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj_expr = adj_col.last().over("code")
qfq_expr = pl.when(latest_adj_expr > 0).then(adj_col / latest_adj_expr).otherwise(1.0)
df_all = df_all.with_columns([
    (pl.col('close') * qfq_expr).cast(pl.Float32).alias('close'),
])
data_manager.df_daily = df_all

# Build calendar from full data
trade_dates_full = sorted(df_all.select(pl.col("date")).unique().sort("date").to_series().to_list())
# Include dates after END for T+1 execution
trade_dates_full = [d for d in trade_dates_full if START <= d]
cal_full = TradingCalendar()
cal_full.set_trade_dates(trade_dates_full)

allocator = top_n_allocator(20)
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")

selection_engine._set_cache.clear()
engine_full = BacktestEngine(
    calendar=cal_full, selection_engine=selection_engine,
    raw_price_store=RawPriceStore(hf_repo_id='scanli/stocka-data'),
    fee_config=FeeConfig(), execution_config=MVP_EXECUTION_CONFIG,
    allocator=allocator,
)

result_full = engine_full.run(
    formula=FORMULA, start_date=START, end_signal_date=END,
    initial_cash=INITIAL_CASH, rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
log(f"Full-load: {time.time()-t0:.1f}s, trades={result_full.trades.height}, equity={result_full.equity_curve['equity'].tail(1).item():,.2f}")

# ============================================================
# PATH B: Lazy backtest
# ============================================================
log("\n=== PATH B: Lazy ===")
t0 = time.time()

data_manager.df_daily = None
selection_engine._set_cache.clear()

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw_store.load_latest_adjust_factors()

trade_dates_lazy = raw_store.get_trading_dates(START, datetime.date(2025, 1, 10))
cal_lazy = TradingCalendar()
cal_lazy.set_trade_dates(trade_dates_lazy)

engine_lazy = BacktestEngine(
    calendar=cal_lazy, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)
engine_lazy._latest_adj = latest_adj

result_lazy = engine_lazy.run(
    formula=FORMULA, start_date=START, end_signal_date=END,
    initial_cash=INITIAL_CASH, rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)
log(f"Lazy: {time.time()-t0:.1f}s, trades={result_lazy.trades.height}, equity={result_lazy.equity_curve['equity'].tail(1).item():,.2f}")

# ============================================================
# COMPARISON
# ============================================================
log("\n=== TRADE COMPARISON ===")
trades_full = result_full.trades.sort(['execution_date', 'code'])
trades_lazy = result_lazy.trades.sort(['execution_date', 'code'])

log(f"Trade count: full={trades_full.height}, lazy={trades_lazy.height}")

# Compare trade-by-trade
if trades_full.height != trades_lazy.height:
    log(f"COUNT MISMATCH: {trades_full.height} vs {trades_lazy.height}")

# Normalize columns for comparison
compare_cols = ['date', 'code', 'side']
for col in compare_cols:
    if col in trades_full.columns and col in trades_lazy.columns:
        pass  # OK

# Find first divergent trade
full_keys = set()
for row in trades_full.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    full_keys.add(key)

lazy_keys = set()
for row in trades_lazy.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    lazy_keys.add(key)

only_full = full_keys - lazy_keys
only_lazy = lazy_keys - full_keys

log(f"Trade keys: full={len(full_keys)}, lazy={len(lazy_keys)}")
log(f"Only in full: {len(only_full)}")
log(f"Only in lazy: {len(only_lazy)}")

if only_full:
    sorted_full = sorted(only_full)
    log(f"\nFirst 5 trades only in full:")
    for k in sorted_full[:5]:
        log(f"  {k}")

if only_lazy:
    sorted_lazy = sorted(only_lazy)
    log(f"\nFirst 5 trades only in lazy:")
    for k in sorted_lazy[:5]:
        log(f"  {k}")

# Price comparison for matching trades
log("\n=== PRICE COMPARISON (matching trades) ===")
full_trade_dict = {}
for row in trades_full.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    full_trade_dict[key] = row

lazy_trade_dict = {}
for row in trades_lazy.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    lazy_trade_dict[key] = row

common_keys = full_keys & lazy_keys
price_diffs = []
for key in sorted(common_keys)[:50]:  # Check first 50
    f = full_trade_dict[key]
    l = lazy_trade_dict[key]
    f_price = f.get('price', 0)
    l_price = l.get('price', 0)
    f_qty = f.get('quantity', 0)
    l_qty = l.get('quantity', 0)
    f_fee = f.get('fee', 0)
    l_fee = l.get('fee', 0)
    if abs(f_price - l_price) > 0.001 or abs(f_qty - l_qty) > 0.01 or abs(f_fee - l_fee) > 0.01:
        price_diffs.append({
            'date': str(key[0]), 'code': key[1], 'side': key[2],
            'full_price': f_price, 'lazy_price': l_price,
            'full_qty': f_qty, 'lazy_qty': l_qty,
            'full_fee': f_fee, 'lazy_fee': l_fee,
        })

if price_diffs:
    log(f"Price/qty/fee diffs found: {len(price_diffs)}")
    for d in price_diffs[:5]:
        log(f"  {d}")
else:
    log("No price/qty/fee diffs in first 50 matching trades")

# ============================================================
# PORTFOLIO / EQUITY COMPARISON
# ============================================================
log("\n=== EQUITY CURVE COMPARISON ===")
eq_full = result_full.equity_curve.sort('date')
eq_lazy = result_lazy.equity_curve.sort('date')

# Join on date
eq_compare = eq_full.select(['date', 'equity']).rename({'equity': 'eq_full'}).join(
    eq_lazy.select(['date', 'equity']).rename({'equity': 'eq_lazy'}),
    on='date', how='inner'
)
eq_compare = eq_compare.with_columns(
    (pl.col('eq_full') - pl.col('eq_lazy')).abs().alias('diff')
)

# Find first divergence
divergent = eq_compare.filter(pl.col('diff') > 1.0)  # > $1 difference
if divergent.height > 0:
    first_div = divergent.head(1)
    log(f"FIRST EQUITY DIVERGENCE:")
    log(f"  Date: {first_div['date'].item()}")
    log(f"  Full equity: {first_div['eq_full'].item():,.2f}")
    log(f"  Lazy equity: {first_div['eq_lazy'].item():,.2f}")
    log(f"  Diff: {first_div['diff'].item():,.2f}")
else:
    log("No equity divergence > $1 found")

log(f"\nEquity diff stats:")
log(f"  Max diff: {eq_compare['diff'].max():,.2f}")
log(f"  Mean diff: {eq_compare['diff'].mean():,.2f}")
log(f"  Divergent dates (> $1): {divergent.height}")

# Final equity
full_eq = result_full.equity_curve['equity'].tail(1).item()
lazy_eq = result_lazy.equity_curve['equity'].tail(1).item()
log(f"\nFINAL EQUITY:")
log(f"  Full: {full_eq:,.2f}")
log(f"  Lazy: {lazy_eq:,.2f}")
log(f"  Diff: {abs(full_eq - lazy_eq):,.2f} ({abs(full_eq - lazy_eq)/full_eq*100:.2f}%)")

# Save artifact
os.makedirs('artifacts/diff/full_vs_lazy', exist_ok=True)
artifact = {
    "selection_match": True,
    "trade_count_full": trades_full.height,
    "trade_count_lazy": trades_lazy.height,
    "trade_keys_only_full": len(only_full),
    "trade_keys_only_lazy": len(only_lazy),
    "equity_full": full_eq,
    "equity_lazy": lazy_eq,
    "equity_diff": abs(full_eq - lazy_eq),
    "equity_diff_pct": abs(full_eq - lazy_eq) / full_eq * 100,
    "first_divergent_date": str(first_div['date'].item()) if divergent.height > 0 else None,
    "first_divergent_full": first_div['eq_full'].item() if divergent.height > 0 else None,
    "first_divergent_lazy": first_div['eq_lazy'].item() if divergent.height > 0 else None,
    "max_eq_diff": eq_compare['diff'].max(),
}
with open('artifacts/diff/full_vs_lazy/summary.json', 'w') as f:
    json.dump(artifact, f, indent=2, default=str)
log(f"\nSaved: artifacts/diff/full_vs_lazy/summary.json")

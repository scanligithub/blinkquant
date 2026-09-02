#!/usr/bin/env python3
"""B4-V4-Golden-Replay: strict B4 replay with V4 engine, comparing against golden.

Generates full parquet artifacts (trades, equity_curve, positions_daily)
and compares against golden B4 summary metrics.
Also compares full-load vs lazy paths for same B4 config."""
import sys, os, datetime, json, time
sys.path.insert(0, 'backend')
if not os.getenv('HF_TOKEN'):
    raise RuntimeError("Set HF_TOKEN environment variable")

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from huggingface_hub import hf_hub_download

# ============================================================
# Golden B4 parameters
# ============================================================
FORMULA = "CLOSE > MA(CLOSE, 20)"
REBALANCE = "weekly"
TOP_N = 20
INITIAL_CASH = 10_000_000
START = datetime.date(2010, 1, 4)
END = datetime.date(2024, 12, 30)
KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ============================================================
# Load golden reference
# ============================================================
with open("benchmarks/B4_2010_2024_golden.json") as f:
    golden = json.load(f)

log("Golden B4 reference:")
log(f"  trades: {golden['n_trades']}")
log(f"  equity: {golden['final_equity']:,.2f}")
log(f"  formula: {golden['formula']}")
log(f"  rebalance: {golden['rebalance_freq']}")
log(f"  top_n: {golden['top_n']}")
log(f"  initial_cash: {golden['initial_cash']}")

# ============================================================
# PATH A: Full-load B4 (same as generate_golden.py)
# ============================================================
log("=" * 60)
log("PATH A: Full-load B4 (generate_golden.py path)")
log("=" * 60)
t0 = time.time()

dm = DataManager()
parts = []
for year in range(2009, 2025):
    try:
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        df = pl.scan_parquet(p).select(KEEP_COLS).collect()
        df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
        parts.append(df)
        log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes")
        del df
    except Exception as e:
        log(f"  {year}: FAILED - {e}")

df_all = pl.concat(parts, how="diagonal")
del parts
df_all = df_all.with_columns(pl.col("date").str.to_date('%Y-%m-%d'))
df_all = df_all.sort(["code", "date"])

# Apply full-load QFQ pipeline (same as generate_golden.py)
dm.df_daily = df_all
dm._compute_limit_flags()
dm._apply_forward_adjustment()
dm._append_prev_close()
dm._optimize_memory(dm.df_daily, "df_daily")
dm._resample_all()

# Install into global singletons
import core.data_manager as _mod
_mod.data_manager.df_daily = dm.df_daily
_mod.data_manager.df_weekly = dm.df_weekly
_mod.data_manager.df_monthly = dm.df_monthly
_mod.data_manager._asof_frame_cache.clear()
selection_engine._set_cache.clear()

trade_dates = sorted(dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list())
calendar = TradingCalendar()
calendar.set_trade_dates(trade_dates)
log(f"Calendar: {len(trade_dates)} dates ({trade_dates[0]} .. {trade_dates[-1]})")

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
allocator = top_n_equal_weight_allocator(TOP_N)

engine_full = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

log(f"Running full-load B4 ...")
t_bt = time.time()
result_full = engine_full.run(
    formula=FORMULA, start_date=START, end_signal_date=END,
    initial_cash=INITIAL_CASH, rebalance_freq=REBALANCE,
    top_n=TOP_N, fee_schedule=fee_schedule,
)
full_time = time.time() - t_bt
log(f"Full-load: {full_time:.1f}s, trades={result_full.trades.height}, equity={result_full.equity_curve['equity'].tail(1).item():,.2f}")

# Save full-load artifacts
os.makedirs("benchmarks/B4_V4_replay_full", exist_ok=True)
result_full.trades.write_parquet("benchmarks/B4_V4_replay_full/trades.parquet")
result_full.equity_curve.write_parquet("benchmarks/B4_V4_replay_full/equity_curve.parquet")
result_full.positions_daily.write_parquet("benchmarks/B4_V4_replay_full/positions_daily.parquet")

# Clean up
_mod.data_manager.df_daily = None
_mod.data_manager.df_weekly = None
_mod.data_manager.df_monthly = None
_mod.data_manager._asof_frame_cache.clear()
selection_engine._set_cache.clear()

# ============================================================
# PATH B: Lazy B4
# ============================================================
log("\n" + "=" * 60)
log("PATH B: Lazy B4")
log("=" * 60)
t0 = time.time()

raw_store_b = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
latest_adj = raw_store_b.load_latest_adjust_factors()

trade_dates_b = raw_store_b.get_trading_dates(START, datetime.date(2025, 1, 10))
cal_b = TradingCalendar()
cal_b.set_trade_dates(trade_dates_b)
log(f"Calendar: {len(trade_dates_b)} dates ({trade_dates_b[0]} .. {trade_dates_b[-1]})")

engine_lazy = BacktestEngine(
    calendar=cal_b, selection_engine=selection_engine,
    raw_price_store=raw_store_b, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)
engine_lazy._latest_adj = latest_adj

log(f"Running lazy B4 ...")
t_bt = time.time()
result_lazy = engine_lazy.run(
    formula=FORMULA, start_date=START, end_signal_date=END,
    initial_cash=INITIAL_CASH, rebalance_freq=REBALANCE,
    top_n=TOP_N, fee_schedule=fee_schedule,
)
lazy_time = time.time() - t_bt
log(f"Lazy: {lazy_time:.1f}s, trades={result_lazy.trades.height}, equity={result_lazy.equity_curve['equity'].tail(1).item():,.2f}")

# Save lazy artifacts
os.makedirs("benchmarks/B4_V4_replay_lazy", exist_ok=True)
result_lazy.trades.write_parquet("benchmarks/B4_V4_replay_lazy/trades.parquet")
result_lazy.equity_curve.write_parquet("benchmarks/B4_V4_replay_lazy/equity_curve.parquet")
result_lazy.positions_daily.write_parquet("benchmarks/B4_V4_replay_lazy/positions_daily.parquet")

# ============================================================
# COMPARISON
# ============================================================
log("\n" + "=" * 60)
log("COMPARISON: V4 Full vs V4 Lazy vs Golden")
log("=" * 60)

full_trades = result_full.trades
lazy_trades = result_lazy.trades
full_ec = result_full.equity_curve
lazy_ec = result_lazy.equity_curve

# --- Trade count ---
log(f"\n--- Trade Count ---")
log(f"  Golden:   {golden['n_trades']}")
log(f"  Full:     {full_trades.height}")
log(f"  Lazy:     {lazy_trades.height}")
log(f"  Full==Lazy: {full_trades.height == lazy_trades.height}")
log(f"  Full==Golden: {full_trades.height == golden['n_trades']}")

# --- Final equity ---
full_eq = full_ec['equity'].tail(1).item()
lazy_eq = lazy_ec['equity'].tail(1).item()
gold_eq = golden['final_equity']
log(f"\n--- Final Equity ---")
log(f"  Golden:   {gold_eq:>15,.2f}")
log(f"  Full:     {full_eq:>15,.2f}")
log(f"  Lazy:     {lazy_eq:>15,.2f}")
log(f"  Full diff from golden: {abs(full_eq - gold_eq):>10,.2f} ({abs(full_eq - gold_eq)/gold_eq*100:.4f}%)")
log(f"  Lazy diff from golden: {abs(lazy_eq - gold_eq):>10,.2f} ({abs(lazy_eq - gold_eq)/gold_eq*100:.4f}%)")
log(f"  Full==Lazy: {abs(full_eq - lazy_eq) < 0.01}")

# --- Trade keys comparison ---
full_keys = set()
for row in full_trades.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    full_keys.add(key)

lazy_keys = set()
for row in lazy_trades.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    lazy_keys.add(key)

only_full = full_keys - lazy_keys
only_lazy = lazy_keys - full_keys
log(f"\n--- Trade Keys ---")
log(f"  Full keys: {len(full_keys)}")
log(f"  Lazy keys: {len(lazy_keys)}")
log(f"  Only in full: {len(only_full)}")
log(f"  Only in lazy: {len(only_lazy)}")

if only_full:
    log(f"  First 5 only-in-full:")
    for k in sorted(only_full)[:5]:
        log(f"    {k}")
if only_lazy:
    log(f"  First 5 only-in-lazy:")
    for k in sorted(only_lazy)[:5]:
        log(f"    {k}")

# --- Price/qty/fee comparison ---
full_trade_dict = {}
for row in full_trades.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    full_trade_dict[key] = row

lazy_trade_dict = {}
for row in lazy_trades.iter_rows(named=True):
    key = (row['execution_date'], row['code'], row['side'])
    lazy_trade_dict[key] = row

common_keys = full_keys & lazy_keys
price_diffs = []
qty_diffs = []
fee_diffs = []
for key in sorted(common_keys):
    f = full_trade_dict[key]
    l = lazy_trade_dict[key]
    fp = f.get('price', 0)
    lp = l.get('price', 0)
    fq = f.get('qty', 0)
    lq = l.get('qty', 0)
    ff = f.get('fee', 0)
    lf = l.get('fee', 0)
    if abs(fp - lp) > 0.001:
        price_diffs.append((key, fp, lp))
    if abs(fq - lq) > 0.01:
        qty_diffs.append((key, fq, lq))
    if abs(ff - lf) > 0.01:
        fee_diffs.append((key, ff, lf))

log(f"\n--- Price/Qty/Fee Diffs ---")
log(f"  Price diffs: {len(price_diffs)}")
log(f"  Qty diffs:   {len(qty_diffs)}")
log(f"  Fee diffs:   {len(fee_diffs)}")
if price_diffs:
    log(f"  First 3 price diffs:")
    for k, fp, lp in price_diffs[:3]:
        log(f"    {k}: full={fp:.6f}, lazy={lp:.6f}, diff={abs(fp-lp):.6f}")
if qty_diffs:
    log(f"  First 3 qty diffs:")
    for k, fq, lq in qty_diffs[:3]:
        log(f"    {k}: full={fq}, lazy={lq}")

# --- Equity curve comparison ---
eq_compare = full_ec.select(['date', 'equity']).rename({'equity': 'eq_full'}).join(
    lazy_ec.select(['date', 'equity']).rename({'equity': 'eq_lazy'}),
    on='date', how='inner'
)
eq_compare = eq_compare.with_columns(
    (pl.col('eq_full') - pl.col('eq_lazy')).abs().alias('diff')
)
divergent = eq_compare.filter(pl.col('diff') > 1.0)

log(f"\n--- Equity Curve ---")
log(f"  Valuation dates: full={full_ec.height}, lazy={lazy_ec.height}")
log(f"  Max equity diff: {eq_compare['diff'].max():,.2f}")
log(f"  Mean equity diff: {eq_compare['diff'].mean():,.2f}")
log(f"  Divergent dates (> $1): {divergent.height}")
if divergent.height > 0:
    first_div = divergent.head(1)
    log(f"  First divergence: {first_div['date'].item()}, diff={first_div['diff'].item():,.2f}")

# --- Positions daily comparison ---
full_pos = result_full.positions_daily.sort(['date', 'code'])
lazy_pos = result_lazy.positions_daily.sort(['date', 'code'])
log(f"\n--- Positions Daily ---")
log(f"  Full: {full_pos.height} rows")
log(f"  Lazy: {lazy_pos.height} rows")

# --- Diagnostics ---
full_diag = result_full.execution_diagnostics or {}
lazy_diag = result_lazy.execution_diagnostics or {}
log(f"\n--- Diagnostics ---")
full_rej = sum(full_diag.get('rej_counters', {}).values())
lazy_rej = sum(lazy_diag.get('rej_counters', {}).values())
log(f"  Full: intents={full_diag.get('intents_total', 0)}, rej={full_rej}")
log(f"  Lazy: intents={lazy_diag.get('intents_total', 0)}, rej={lazy_rej}")
log(f"  Negative cash: full={full_diag.get('has_negative_cash', False)}, lazy={lazy_diag.get('has_negative_cash', False)}")
log(f"  Accounting violations: full={full_diag.get('accounting_invariant_violations', 0)}, lazy={lazy_diag.get('accounting_invariant_violations', 0)}")

# --- Timing ---
log(f"\n--- Timing ---")
log(f"  Full: {full_time:.1f}s")
log(f"  Lazy: {lazy_time:.1f}s")
log(f"  Speedup: {full_time/lazy_time:.2f}x")

# ============================================================
# VERDICT
# ============================================================
all_pass = (
    full_trades.height == lazy_trades.height and
    full_trades.height == golden['n_trades'] and
    abs(full_eq - lazy_eq) < 0.01 and
    abs(full_eq - gold_eq) < 0.01 and
    abs(lazy_eq - gold_eq) < 0.01 and
    len(only_full) == 0 and
    len(only_lazy) == 0 and
    len(price_diffs) == 0 and
    len(qty_diffs) == 0 and
    len(fee_diffs) == 0
)

log("\n" + "=" * 60)
if all_pass:
    log("RESULT: B4-V4-Golden-Replay EXACT MATCH")
    log("  V4 Full == V4 Lazy == Golden B4")
    log("  All trades, prices, quantities, fees, equity: IDENTICAL")
else:
    log("RESULT: B4-V4-Golden-Replay DIFFERENCES FOUND")
    if full_trades.height != golden['n_trades']:
        log(f"  TRADE COUNT: full={full_trades.height} vs golden={golden['n_trades']}")
    if full_trades.height != lazy_trades.height:
        log(f"  TRADE COUNT: full={full_trades.height} vs lazy={lazy_trades.height}")
    if abs(full_eq - gold_eq) > 0.01:
        log(f"  EQUITY: full={full_eq:,.2f} vs golden={gold_eq:,.2f} (diff={abs(full_eq-gold_eq):,.2f})")
    if abs(lazy_eq - gold_eq) > 0.01:
        log(f"  EQUITY: lazy={lazy_eq:,.2f} vs golden={gold_eq:,.2f} (diff={abs(lazy_eq-gold_eq):,.2f})")
    if len(only_full) > 0:
        log(f"  TRADE KEYS: {len(only_full)} only-in-full")
    if len(only_lazy) > 0:
        log(f"  TRADE KEYS: {len(only_lazy)} only-in-lazy")
    if len(price_diffs) > 0:
        log(f"  PRICE: {len(price_diffs)} diffs")
    if len(qty_diffs) > 0:
        log(f"  QTY: {len(qty_diffs)} diffs")

log("=" * 60)

# Save summary
summary = {
    "golden_trades": golden['n_trades'],
    "golden_equity": golden['final_equity'],
    "full_trades": full_trades.height,
    "full_equity": full_eq,
    "lazy_trades": lazy_trades.height,
    "lazy_equity": lazy_eq,
    "full_time_sec": full_time,
    "lazy_time_sec": lazy_time,
    "full_vs_lazy_keys_diff": len(only_full) + len(only_lazy),
    "full_vs_lazy_price_diffs": len(price_diffs),
    "full_vs_lazy_qty_diffs": len(qty_diffs),
    "full_vs_lazy_fee_diffs": len(fee_diffs),
    "max_equity_diff": float(eq_compare['diff'].max()),
    "exact_match": all_pass,
}
with open("benchmarks/B4_V4_golden_replay.json", "w") as f:
    json.dump(summary, f, indent=2, default=str)
log(f"Saved: benchmarks/B4_V4_golden_replay.json")

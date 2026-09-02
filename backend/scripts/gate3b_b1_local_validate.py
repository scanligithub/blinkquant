#!/usr/bin/env python3
"""Gate 3B: Local B1 validation before HF Space deployment.

Verifies B1 (2024 Q1) produces identical results to frozen golden.
This is the final local checkpoint before pushing to HF Space."""
import sys, os, datetime, json, time
sys.path.insert(0, 'backend')
if not os.getenv('HF_TOKEN'):
    raise RuntimeError("Set HF_TOKEN environment variable")

import polars as pl
from core.raw_price_store import RawPriceStore
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule

B1_START = datetime.date(2024, 1, 2)
B1_END = datetime.date(2024, 3, 29)
FORMULA = "CLOSE > MA(CLOSE, 20)"

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# Load frozen golden
log("Loading frozen B1 golden...")
g_trades = pl.read_parquet('tests/golden/2024q1/trades.parquet')
g_ec = pl.read_parquet('tests/golden/2024q1/equity_curve.parquet')
g_pos = pl.read_parquet('tests/golden/2024q1/positions_daily.parquet')
with open('tests/golden/2024q1/metrics.json') as f:
    g_metrics = json.load(f)
with open('tests/golden/2024q1/diagnostics.json') as f:
    g_diag = json.load(f)

log(f"Golden: trades={g_trades.height}, equity={g_ec['equity'].tail(1).item():,.2f}")

# Run V4 lazy backtest
log("\nRunning V4 lazy backtest (B1)...")
t0 = time.time()

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
latest_adj = raw_store.load_latest_adjust_factors()

trade_dates = raw_store.get_trading_dates(B1_START, datetime.date(2024, 4, 5))
cal = TradingCalendar()
cal.set_trade_dates(trade_dates)

allocator = top_n_equal_weight_allocator(20)
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
selection_engine._set_cache.clear()

engine = BacktestEngine(
    calendar=cal, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)
engine._latest_adj = latest_adj

result = engine.run(
    formula=FORMULA, start_date=B1_START, end_signal_date=B1_END,
    initial_cash=10_000_000, rebalance_freq="weekly",
    top_n=20, fee_schedule=fee_schedule,
)
bt_time = time.time() - t0
log(f"V4: trades={result.trades.height}, equity={result.equity_curve['equity'].tail(1).item():,.2f}, time={bt_time:.1f}s")

# === COMPARISON ===
log("\n" + "=" * 60)
log("B1 VALIDATION: V4 Lazy vs Frozen Golden")
log("=" * 60)

# 1. Trade count
trades_match = result.trades.height == g_trades.height
log(f"Trade count: V4={result.trades.height}, Golden={g_trades.height} -> {'PASS' if trades_match else 'FAIL'}")

# 2. Trade keys
v_keys = set((r['execution_date'], r['code'], r['side']) for r in result.trades.iter_rows(named=True))
g_keys = set((r['execution_date'], r['code'], r['side']) for r in g_trades.iter_rows(named=True))
keys_match = v_keys == g_keys
log(f"Trade keys: V4={len(v_keys)}, Golden={len(g_keys)}, match={keys_match} -> {'PASS' if keys_match else 'FAIL'}")

# 3. Price/qty/fee
v_dict = {(r['execution_date'], r['code'], r['side']): r for r in result.trades.iter_rows(named=True)}
g_dict = {(r['execution_date'], r['code'], r['side']): r for r in g_trades.iter_rows(named=True)}
common = v_keys & g_keys
qty_diffs = sum(1 for k in common if abs(v_dict[k]['qty'] - g_dict[k]['qty']) > 0.01)
price_diffs = sum(1 for k in common if abs(v_dict[k]['price'] - g_dict[k]['price']) > 0.001)
fee_diffs = sum(1 for k in common if abs(v_dict[k]['fee'] - g_dict[k]['fee']) > 0.01)
pf_match = qty_diffs == price_diffs == fee_diffs == 0
log(f"Price/qty/fee diffs: price={price_diffs}, qty={qty_diffs}, fee={fee_diffs} -> {'PASS' if pf_match else 'FAIL'}")

# 4. Equity curve
v_eq = result.equity_curve['equity'].tail(1).item()
g_eq = g_ec['equity'].tail(1).item()
eq_match = abs(v_eq - g_eq) < 0.01
log(f"Final equity: V4={v_eq:,.2f}, Golden={g_eq:,.2f}, diff={abs(v_eq-g_eq):,.2f} -> {'PASS' if eq_match else 'FAIL'}")

#逐行 equity curve comparison
ec_join = result.equity_curve.select(['date', 'equity']).rename({'equity': 'v_eq'}).join(
    g_ec.select(['date', 'equity']).rename({'equity': 'g_eq'}),
    on='date', how='inner'
)
if ec_join.height > 0:
    ec_diffs = ec_join.with_columns((pl.col('v_eq') - pl.col('g_eq')).abs().alias('diff'))
    max_diff = ec_diffs['diff'].max()
    divergent = ec_diffs.filter(pl.col('diff') > 0.01).height
    ec_row_match = divergent == 0
    log(f"EC rows: V4={result.equity_curve.height}, Golden={g_ec.height}, max_diff={max_diff:,.2f}, divergent={divergent} -> {'PASS' if ec_row_match else 'FAIL'}")
else:
    ec_row_match = False
    log(f"EC rows: no matching dates -> FAIL")

# 5. Positions
v_pos_keys = set((r['date'], r['code']) for r in result.positions_daily.iter_rows(named=True))
g_pos_keys = set((r['date'], r['code']) for r in g_pos.iter_rows(named=True))
pos_match = v_pos_keys == g_pos_keys
log(f"Position keys: V4={len(v_pos_keys)}, Golden={len(g_pos_keys)}, match={pos_match} -> {'PASS' if pos_match else 'FAIL'}")

# 6. Diagnostics
diag = result.execution_diagnostics or {}
v_rej = sum(diag.get('rej_counters', {}).values())
g_rej = sum(g_diag.get('rej_counters', {}).values())
diag_match = v_rej == g_rej and not diag.get('has_negative_cash', False)
log(f"Diagnostics: V4 intents={diag.get('intents_total',0)}, rej={v_rej}, Golden intents={g_diag.get('intents_total',0)}, rej={g_rej} -> {'PASS' if diag_match else 'FAIL'}")

# === VERDICT ===
all_pass = trades_match and keys_match and pf_match and eq_match and ec_row_match and pos_match and diag_match
log("\n" + "=" * 60)
if all_pass:
    log("B1 LOCAL VALIDATION: PASS")
    log("V4 Lazy == Frozen Golden (2024 Q1)")
    log("Ready for HF Space deployment.")
else:
    log("B1 LOCAL VALIDATION: FAIL")
    if not trades_match:
        log(f"  TRADE COUNT MISMATCH: {result.trades.height} vs {g_trades.height}")
    if not keys_match:
        log(f"  TRADE KEYS MISMATCH")
    if not pf_match:
        log(f"  PRICE/QTY/FEE MISMATCH")
    if not eq_match:
        log(f"  EQUITY MISMATCH: {v_eq:,.2f} vs {g_eq:,.2f}")
    if not ec_row_match:
        log(f"  EQUITY CURVE ROW MISMATCH")
    if not pos_match:
        log(f"  POSITION KEYS MISMATCH")
    if not diag_match:
        log(f"  DIAGNOSTICS MISMATCH")
log("=" * 60)

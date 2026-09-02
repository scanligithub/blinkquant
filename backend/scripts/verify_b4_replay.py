#!/usr/bin/env python3
"""Verify B4_V4_golden replay exact match."""
import polars as pl
import json

# Load new golden
trades_g = pl.read_parquet('benchmarks/B4_V4_golden/trades.parquet')
ec_g = pl.read_parquet('benchmarks/B4_V4_golden/equity_curve.parquet')
pos_g = pl.read_parquet('benchmarks/B4_V4_golden/positions_daily.parquet')

# Load replay (full-load)
trades_f = pl.read_parquet('benchmarks/B4_V4_replay_full/trades.parquet')
ec_f = pl.read_parquet('benchmarks/B4_V4_replay_full/equity_curve.parquet')
pos_f = pl.read_parquet('benchmarks/B4_V4_replay_full/positions_daily.parquet')

# Load replay (lazy)
trades_l = pl.read_parquet('benchmarks/B4_V4_replay_lazy/trades.parquet')
ec_l = pl.read_parquet('benchmarks/B4_V4_replay_lazy/equity_curve.parquet')
pos_l = pl.read_parquet('benchmarks/B4_V4_replay_lazy/positions_daily.parquet')

print('=== TRADES ===')
print(f'Golden: {trades_g.height}, Full: {trades_f.height}, Lazy: {trades_l.height}')
print(f'Golden==Full: {trades_g.height == trades_f.height}')
print(f'Full==Lazy: {trades_f.height == trades_l.height}')

# Compare trade keys
g_keys = set((r['execution_date'], r['code'], r['side']) for r in trades_g.iter_rows(named=True))
f_keys = set((r['execution_date'], r['code'], r['side']) for r in trades_f.iter_rows(named=True))
l_keys = set((r['execution_date'], r['code'], r['side']) for r in trades_l.iter_rows(named=True))
print(f'Keys: Golden==Full: {g_keys == f_keys}, Full==Lazy: {f_keys == l_keys}')

# Compare qty/price/fee
g_dict = {(r['execution_date'], r['code'], r['side']): r for r in trades_g.iter_rows(named=True)}
f_dict = {(r['execution_date'], r['code'], r['side']): r for r in trades_f.iter_rows(named=True)}
l_dict = {(r['execution_date'], r['code'], r['side']): r for r in trades_l.iter_rows(named=True)}

common = g_keys & f_keys
qty_diffs = sum(1 for k in common if abs(g_dict[k]['qty'] - f_dict[k]['qty']) > 0.01)
price_diffs = sum(1 for k in common if abs(g_dict[k]['price'] - f_dict[k]['price']) > 0.001)
fee_diffs = sum(1 for k in common if abs(g_dict[k]['fee'] - f_dict[k]['fee']) > 0.01)
print(f'Golden vs Full: qty={qty_diffs}, price={price_diffs}, fee={fee_diffs}')

common2 = f_keys & l_keys
qty_diffs2 = sum(1 for k in common2 if abs(f_dict[k]['qty'] - l_dict[k]['qty']) > 0.01)
price_diffs2 = sum(1 for k in common2 if abs(f_dict[k]['price'] - l_dict[k]['price']) > 0.001)
fee_diffs2 = sum(1 for k in common2 if abs(f_dict[k]['fee'] - l_dict[k]['fee']) > 0.01)
print(f'Full vs Lazy: qty={qty_diffs2}, price={price_diffs2}, fee={fee_diffs2}')

print()
print('=== EQUITY CURVE ===')
print(f'Golden: {ec_g.height}, Full: {ec_f.height}, Lazy: {ec_l.height}')
eq_g = ec_g['equity'].tail(1).item()
eq_f = ec_f['equity'].tail(1).item()
eq_l = ec_l['equity'].tail(1).item()
print(f'Final: Golden={eq_g:,.2f}, Full={eq_f:,.2f}, Lazy={eq_l:,.2f}')
print(f'Golden==Full: {abs(eq_g - eq_f) < 0.01}')
print(f'Full==Lazy: {abs(eq_f - eq_l) < 0.01}')

#逐行对比 equity curve
ec_compare = ec_g.select(['date', 'equity']).rename({'equity': 'eq_g'}).join(
    ec_f.select(['date', 'equity']).rename({'equity': 'eq_f'}),
    on='date', how='inner'
)
ec_diffs = ec_compare.with_columns((pl.col('eq_g') - pl.col('eq_f')).abs().alias('diff'))
divergent = ec_diffs.filter(pl.col('diff') > 0.01)
print(f'Equity curve dates: golden vs full: {divergent.height} divergent (> $0.01)')

print()
print('=== POSITIONS DAILY ===')
print(f'Golden: {pos_g.height}, Full: {pos_f.height}, Lazy: {pos_l.height}')
pos_gk = set((r['date'], r['code']) for r in pos_g.iter_rows(named=True))
pos_fk = set((r['date'], r['code']) for r in pos_f.iter_rows(named=True))
pos_lk = set((r['date'], r['code']) for r in pos_l.iter_rows(named=True))
print(f'Position keys: Golden==Full: {pos_gk == pos_fk}, Full==Lazy: {pos_fk == pos_lk}')

print()
print('=== DIAGNOSTICS ===')
with open('benchmarks/B4_V4_golden/diagnostics.json') as f:
    dg = json.load(f)
rej_total = sum(dg.get('rej_counters', {}).values())
print(f'intents={dg.get("intents_total", 0)}, rej={rej_total}')
print(f'negative_cash={dg.get("has_negative_cash", False)}')
print(f'accounting_violations={dg.get("accounting_invariant_violations", 0)}')

print()
print('=== METRICS ===')
with open('benchmarks/B4_V4_golden/metrics.json') as f:
    mg = json.load(f)
for k, v in mg.items():
    print(f'  {k}: {v}')

# Summary
all_pass = (
    trades_g.height == trades_f.height == trades_l.height and
    g_keys == f_keys == l_keys and
    qty_diffs == price_diffs == fee_diffs == 0 and
    qty_diffs2 == price_diffs2 == fee_diffs2 == 0 and
    abs(eq_g - eq_f) < 0.01 and abs(eq_f - eq_l) < 0.01 and
    pos_gk == pos_fk == pos_lk
)
print()
if all_pass:
    print('RESULT: B4-V4-Golden-Replay EXACT MATCH')
    print('  Golden == Full == Lazy')
else:
    print('RESULT: DIFFERENCES FOUND')

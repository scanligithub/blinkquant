#!/usr/bin/env python3
"""Clean selection comparison: both paths with QFQ applied, for sz.002415 @ 2024-06-03"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from core.engine import selection_engine
from huggingface_hub import hf_hub_download

TARGET = datetime.date(2024, 6, 3)

# --- Full-load with QFQ ---
print("=== Full-load (with QFQ) ===")
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

# Apply QFQ (same as DataManager._apply_forward_adjustment)
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj_expr = adj_col.last().over("code")
qfq_expr = pl.when(latest_adj_expr > 0).then(adj_col / latest_adj_expr).otherwise(1.0)
df_all = df_all.with_columns([
    (pl.col('close') * qfq_expr).cast(pl.Float32).alias('close'),
])
data_manager.df_daily = df_all

selection_engine._set_cache.clear()
result_full = selection_engine.execute_selector(
    'CLOSE > MA(CLOSE, 20)', 'D', None,
    target_date=TARGET, backtest_mode=True, raise_on_error=True
)
full_codes = set(result_full.codes) if hasattr(result_full, 'codes') else set()
print(f"Full-load: {len(full_codes)} codes")

# --- Lazy path ---
print("\n=== Lazy path ===")
selection_engine._set_cache.clear()
data_manager.df_daily = None

raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()
result_lazy = selection_engine.execute_selector(
    'CLOSE > MA(CLOSE, 20)', 'D', None,
    target_date=TARGET, backtest_mode=True, raise_on_error=True,
    qfq_data_provider=raw, latest_adj=latest_adj
)
lazy_codes = set(result_lazy.codes) if hasattr(result_lazy, 'codes') else set()
print(f"Lazy: {len(lazy_codes)} codes")

# Compare
common = lazy_codes & full_codes
only_lazy = lazy_codes - full_codes
only_full = full_codes - lazy_codes
print(f"\nCommon: {len(common)}, Only-lazy: {len(only_lazy)}, Only-full: {len(only_full)}")

# If differences exist, debug one
if only_lazy:
    code = list(only_lazy)[0]
    print(f"\n--- Debug {code} ---")
    
    # Full-load: evaluate expression directly
    blink_parser = selection_engine.blink_parser if hasattr(selection_engine, 'blink_parser') else None
    from core.security import blink_parser as bp
    expr = bp.parse_expression('CLOSE > MA(CLOSE, 20)', 'D')
    
    code_full = data_manager.df_daily.filter(
        (pl.col('code') == code) & (pl.col('date') <= TARGET)
    ).sort('date')
    sig_full = code_full.select(expr.alias('_signal')).tail(1).item()
    print(f"Full-load: {code_full.height} rows, signal={sig_full}")
    if code_full.height >= 20:
        last20 = code_full.tail(20)
        ma20 = last20['close'].mean()
        print(f"  Full close (last): {code_full['close'].tail(1).item():.4f}")
        print(f"  Full MA20 (manual): {ma20:.4f}")
    
    # Lazy: evaluate expression directly
    data_manager.df_daily = None
    qfq_df = raw.load_qfq_window(TARGET - datetime.timedelta(days=250), TARGET, latest_adj)
    code_lazy = qfq_df.filter(pl.col('code') == code).sort('date')
    sig_lazy = code_lazy.select(expr.alias('_signal')).tail(1).item()
    print(f"Lazy: {code_lazy.height} rows, signal={sig_lazy}")
    if code_lazy.height >= 20:
        ma20_l = code_lazy['close'].tail(20).mean()
        print(f"  Lazy close (last): {code_lazy['close'].tail(1).item():.4f}")
        print(f"  Lazy MA20 (manual): {ma20_l:.4f}")

if only_full:
    code = list(only_full)[0]
    print(f"\n--- Debug {code} (only in full) ---")

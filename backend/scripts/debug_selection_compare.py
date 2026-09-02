#!/usr/bin/env python3
"""Compare lazy vs full-load selection for a single date â€?clean version"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.engine import selection_engine
from core.data_manager import data_manager
from huggingface_hub import hf_hub_download

target = datetime.date(2024, 6, 3)

# --- Full-load path ---
print("Loading full data...")
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
data_manager.df_daily = df_all

selection_engine._set_cache.clear()
result_full = selection_engine.execute_selector(
    'CLOSE > MA(CLOSE, 20)', 'D', None,
    target_date=target, backtest_mode=True, raise_on_error=True
)
full_codes = set(result_full.codes) if hasattr(result_full, 'codes') else set()
full_df = data_manager.df_daily  # Save reference before clearing
print(f"Full-load: {len(full_codes)} codes")

# Clear everything before lazy test
selection_engine._set_cache.clear()
data_manager.df_daily = None

# --- Lazy path ---
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()
result_lazy = selection_engine.execute_selector(
    'CLOSE > MA(CLOSE, 20)', 'D', None,
    target_date=target, backtest_mode=True, raise_on_error=True,
    qfq_data_provider=raw, latest_adj=latest_adj
)
lazy_codes = set(result_lazy.codes) if hasattr(result_lazy, 'codes') else set()
print(f"Lazy: {len(lazy_codes)} codes")

# Compare
common = lazy_codes & full_codes
only_lazy = lazy_codes - full_codes
only_full = full_codes - lazy_codes

print(f"\nCommon: {len(common)}, Only-lazy: {len(only_lazy)}, Only-full: {len(only_full)}")

if only_lazy:
    # Check one of the extra codes in detail
    code = list(only_lazy)[0]
    print(f"\n--- Debug {code} ---")
    
    # Check lazy data
    qfq_df = raw.load_qfq_window(target - datetime.timedelta(days=250), target, latest_adj)
    code_lazy = qfq_df.filter(pl.col('code') == code).sort('date')
    print(f"Lazy: {code_lazy.height} rows, last_date={code_lazy['date'].tail(1).item() if code_lazy.height > 0 else 'N/A'}")
    
    # Check full-load data
    code_full = full_df.filter(
        (pl.col('code') == code) & (pl.col('date') <= target)
    ).sort('date')
    print(f"Full: {code_full.height} rows, last_date={code_full['date'].tail(1).item() if code_full.height > 0 else 'N/A'}")
    
    # Check if code exists in raw data at all
    raw_check = raw.scan_window(target - datetime.timedelta(days=365), target, for_qfq=False).filter(
        pl.col('code') == code
    ).collect()
    print(f"Raw window (no qfq): {raw_check.height} rows")

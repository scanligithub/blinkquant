#!/usr/bin/env python3
"""Compare QFQ values between lazy load and full-load for a single date"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore

raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()

# Test: load a 5-day window for one code
target = datetime.date(2024, 6, 3)
start = target - datetime.timedelta(days=10)
df = raw.load_qfq_window(start, target, latest_adj)

# Pick one code
sample_code = 'bj.920000'
code_df = df.filter(pl.col('code') == sample_code)
print(f"Code {sample_code}, {code_df.height} rows:")
print(code_df.sort('date'))

# Now load raw data and compare
print("\n--- Raw data for same code/date range ---")
raw_df = raw.scan_window(start, target, for_qfq=True).filter(
    pl.col('code') == sample_code
).collect().sort('date')
print(raw_df.select(['date', 'code', 'open', 'close', 'adjustFactor']))

# Check what adjustFactor values look like in raw
print(f"\nRaw adjustFactor: {raw_df['adjustFactor'].to_list()}")
print(f"Raw adjustFactor is_null: {raw_df['adjustFactor'].is_null().to_list()}")
print(f"Raw adjustFactor forward_fill: {raw_df['adjustFactor'].forward_fill().fill_null(1.0).to_list()}")
print(f"latest_adj for {sample_code}: {latest_adj.get(sample_code)}")

#!/usr/bin/env python3
"""Compare latest_adj between RawPriceStore and DataManager"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import DataManager

# RawPriceStore latest_adj
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj_raw = raw.load_latest_adjust_factors()

# DataManager latest_adj
dm = DataManager()
df = None
for year in range(2009, 2025):
    try:
        from huggingface_hub import hf_hub_download
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        year_df = pl.scan_parquet(p).select(["code", "adjustFactor", "date"]).collect()
        if df is None:
            df = year_df
        else:
            df = pl.concat([df, year_df])
    except Exception:
        continue

df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df = df.sort(['code', 'date'])

# Compute latest_adj using DataManager's approach
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj_dm_series = adj_col.last().over("code")
dm_latest = (
    df.group_by("code")
    .agg(latest_adj_dm_series.last().alias("latest_adj"))
)
dm_latest_dict = {row["code"]: float(row["latest_adj"]) for row in dm_latest.iter_rows(named=True)}

# Compare
print(f"RawPriceStore latest_adj: {len(latest_adj_raw)} codes")
print(f"DataManager latest_adj: {len(dm_latest_dict)} codes")

# Find mismatches
mismatches = []
for code, raw_val in latest_adj_raw.items():
    dm_val = dm_latest_dict.get(code)
    if dm_val is None:
        mismatches.append((code, raw_val, None))
    elif abs(raw_val - dm_val) > 1e-6:
        mismatches.append((code, raw_val, dm_val))

print(f"Mismatches: {len(mismatches)}")
if mismatches:
    for code, raw_val, dm_val in mismatches[:10]:
        print(f"  {code}: raw={raw_val}, dm={dm_val}")

#!/usr/bin/env python3
"""Compare QFQ values: load_qfq_window vs DataManager _apply_forward_adjustment for sh.600000"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import DataManager
from huggingface_hub import hf_hub_download

sample_code = 'sh.600000'
target = datetime.date(2024, 6, 3)
lookback_start = target - datetime.timedelta(days=250)

# --- Path 1: Lazy load_qfq_window ---
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()
qfq_df = raw.load_qfq_window(lookback_start, target, latest_adj)
lazy_code = (qfq_df.filter(pl.col('code') == sample_code).sort('date'))
print(f"Lazy: {lazy_code.height} rows, last close={lazy_code['close'].tail(1).item():.4f}")

# --- Path 2: Full-load raw + manual QFQ ---
# Load raw data for just this code
raw_window = raw.scan_window(lookback_start, target, for_qfq=True).filter(
    pl.col('code') == sample_code
).collect().sort('date')
print(f"Raw window: {raw_window.height} rows, adjustFactor={raw_window['adjustFactor'].tail(1).item():.4f}")

# Full-history adjustFactor for this code
adj_all = []
for year in range(2009, 2025):
    try:
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        ydf = pl.scan_parquet(p).select(["code", "adjustFactor", "date"]).filter(
            pl.col("code") == sample_code
        ).collect()
        adj_all.append(ydf)
    except:
        pass

adj_combined = pl.concat(adj_all, how="diagonal").sort("date")
# Convert date if needed
schema = adj_combined.schema
if schema['date'] == pl.Utf8:
    adj_combined = adj_combined.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))

# Forward-fill on full history
adj_combined = adj_combined.with_columns(
    pl.col("adjustFactor").forward_fill().fill_null(1.0).alias("adj_ff")
)
latest_full = adj_combined['adj_ff'].tail(1).item()
print(f"latest_adj (lazy): {latest_adj.get(sample_code)}")
print(f"latest_adj (full): {latest_full}")

# Compute QFQ using full-history forward-fill
full_adj_window = adj_combined.filter(
    (pl.col('date') >= lookback_start) & (pl.col('date') <= target)
).select(['date', 'adj_ff'])

full_qfq = raw_window.join(full_adj_window, on='date', how='left')
full_qfq = full_qfq.with_columns(
    (pl.col('close') * pl.col('adj_ff') / latest_full).cast(pl.Float32).alias('full_close')
)

# Compare
compare = lazy_code.select(['date', 'close']).rename({'close': 'lazy_close'}).join(
    full_qfq.select(['date', 'full_close']), on='date', how='inner'
)
compare = compare.with_columns(
    (pl.col('lazy_close') - pl.col('full_close')).abs().alias('diff')
)
print(f"\nComparison (last 5 days):")
print(compare.tail(5))
print(f"\nMax diff: {compare['diff'].max():.6f}")
print(f"Mean diff: {compare['diff'].mean():.6f}")

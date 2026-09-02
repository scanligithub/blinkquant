#!/usr/bin/env python3
"""Compare MA20 and close values for sz.002415"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from core.security import blink_parser
from huggingface_hub import hf_hub_download

target = datetime.date(2024, 6, 3)
code = 'sz.002415'

# Full-load
df_all = None
for year in range(2009, 2025):
    try:
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        year_df = pl.scan_parquet(p).collect()
        use_cols = [c for c in ["date","code","open","high","low","close","volume","amount","adjustFactor"] if c in year_df.columns]
        year_df = year_df.select(use_cols)
        if df_all is None:
            df_all = year_df
        else:
            df_all = pl.concat([df_all, year_df])
    except:
        pass
df_all = df_all.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df_all = df_all.sort(['code', 'date'])

code_full = df_all.filter(
    (pl.col('code') == code) & (pl.col('date') <= target)
).sort('date')

# Compute MA20 manually
code_full = code_full.with_columns(
    pl.col('close').rolling_mean(window_size=20).alias('ma20')
)
last = code_full.tail(3)
print("=== Full-load (last 3 rows) ===")
print(last.select(['date', 'close', 'ma20']))
full_close = last['close'].tail(1).item()
full_ma20 = last['ma20'].tail(1).item()
print(f"Full: close={full_close:.4f}, ma20={full_ma20:.4f}, signal={full_close > full_ma20}")

# Lazy
data_manager.df_daily = None
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()
qfq_df = raw.load_qfq_window(target - datetime.timedelta(days=250), target, latest_adj)
code_lazy = qfq_df.filter(pl.col('code') == code).sort('date')
code_lazy = code_lazy.with_columns(
    pl.col('close').rolling_mean(window_size=20).alias('ma20')
)
last_l = code_lazy.tail(3)
print("\n=== Lazy (last 3 rows) ===")
print(last_l.select(['date', 'close', 'ma20']))
lazy_close = last_l['close'].tail(1).item()
lazy_ma20 = last_l['ma20'].tail(1).item()
print(f"Lazy: close={lazy_close:.4f}, ma20={lazy_ma20:.4f}, signal={lazy_close > lazy_ma20}")

# Check raw data for this code
print(f"\n=== Raw close values (last 5 from full-load raw) ===")
raw_last5 = code_full.tail(5).select(['date', 'close'])
print(raw_last5)

# Check raw close values from lazy
print(f"\n=== Raw close values (last 5 from lazy, via scan_window) ===")
raw_lf = raw.scan_window(target - datetime.timedelta(days=250), target, for_qfq=True)
raw_code = raw_lf.filter(pl.col('code') == code).collect().sort('date')
print(raw_code.tail(5).select(['date', 'close']))

#!/usr/bin/env python3
"""Check data_manager.df_daily columns in full-load path"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.data_manager import data_manager
from huggingface_hub import hf_hub_download

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

print(f"df_daily columns: {data_manager.df_daily.columns}")
print(f"df_daily shape: {data_manager.df_daily.shape}")
print(f"'CLOSE' in columns: {'CLOSE' in data_manager.df_daily.columns}")
print(f"'close' in columns: {'close' in data_manager.df_daily.columns}")

# Check the blink parser's field map
from core.security import blink_parser
print(f"\nblink_parser fields keys: {list(blink_parser.fields.keys())[:10]}")

# Check what CLOSE resolves to
expr = blink_parser.parse_expression('CLOSE', 'D')
print(f"\nParsed 'CLOSE' expression: {expr}")
print(f"current_df is None: {blink_parser.current_df is None}")
print(f"current_df columns has 'CLOSE': {'CLOSE' in blink_parser.current_df.columns if blink_parser.current_df is not None else 'N/A'}")
print(f"current_df columns has 'close': {'close' in blink_parser.current_df.columns if blink_parser.current_df is not None else 'N/A'}")

# Check what MA(CLOSE, 20) resolves to
expr2 = blink_parser.parse_expression('MA(CLOSE, 20)', 'D')
print(f"\nParsed 'MA(CLOSE, 20)' expression: {expr2}")

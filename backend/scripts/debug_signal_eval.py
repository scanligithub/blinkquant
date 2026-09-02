#!/usr/bin/env python3
"""Directly evaluate CLOSE > MA(CLOSE,20) for sz.002415 in both paths"""
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

# --- Full-load ---
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

# Parse expression with full-load data
expr_full = blink_parser.parse_expression('CLOSE > MA(CLOSE, 20)', 'D')
print(f"Full-load expression: {expr_full}")

# Evaluate for target code
code_df = data_manager.df_daily.filter(
    (pl.col('code') == code) & (pl.col('date') <= target)
).sort('date')
result_full = code_df.select(expr_full.alias('_signal')).tail(1).item()
print(f"Full-load: {code}, rows={code_df.height}, signal={result_full}")

# --- Lazy ---
data_manager.df_daily = None
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()

# Parse expression without data
expr_lazy = blink_parser.parse_expression('CLOSE > MA(CLOSE, 20)', 'D')
print(f"\nLazy expression: {expr_lazy}")

qfq_df = raw.load_qfq_window(target - datetime.timedelta(days=250), target, latest_adj)
code_lazy = qfq_df.filter(pl.col('code') == code).sort('date')
result_lazy = code_lazy.select(expr_lazy.alias('_signal')).tail(1).item()
print(f"Lazy: {code}, rows={code_lazy.height}, signal={result_lazy}")

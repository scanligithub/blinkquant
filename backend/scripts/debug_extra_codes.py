#!/usr/bin/env python3
"""Check why lazy path has 82 more codes than full-load for sh.601033"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from huggingface_hub import hf_hub_download

target = datetime.date(2024, 6, 3)
sample_codes = ['sh.601033', 'sh.688668', 'sz.300373']

# --- Full-load path ---
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

# Apply QFQ
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj_expr = adj_col.last().over("code")
qfq_expr = pl.when(latest_adj_expr > 0).then(adj_col / latest_adj_expr).otherwise(1.0)
df_all = df_all.with_columns([
    (pl.col('close') * qfq_expr).cast(pl.Float32).alias('close'),
])

# Compute MA20 and check signal for sample codes
for code in sample_codes:
    code_df = df_all.filter(
        (pl.col('code') == code) & (pl.col('date') <= target)
    ).sort('date')
    
    if code_df.is_empty():
        print(f"{code}: NO DATA in full-load")
        continue
    
    n_rows = code_df.height
    last_date = code_df['date'].tail(1).item()
    last_close = code_df['close'].tail(1).item()
    
    # Compute MA20
    code_df = code_df.with_columns(
        pl.col('close').rolling_mean(window_size=20).alias('ma20')
    )
    last_ma20 = code_df['ma20'].tail(1).item()
    signal = last_close > last_ma20 if last_ma20 is not None else False
    
    ma20_str = f"{last_ma20:.4f}" if last_ma20 is not None else "None"
    print(f"{code}: rows={n_rows}, last_date={last_date}, close={last_close:.4f}, ma20={ma20_str}, signal={signal}")

# --- Lazy path ---
print("\n--- Lazy path ---")
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()

for code in sample_codes:
    qfq_df = raw.load_qfq_window(target - datetime.timedelta(days=250), target, latest_adj)
    code_df = qfq_df.filter(pl.col('code') == code).sort('date')
    
    if code_df.is_empty():
        print(f"{code}: NO DATA in lazy")
        continue
    
    n_rows = code_df.height
    last_date = code_df['date'].tail(1).item()
    last_close = code_df['close'].tail(1).item()
    
    code_df = code_df.with_columns(
        pl.col('close').rolling_mean(window_size=20).alias('ma20')
    )
    last_ma20 = code_df['ma20'].tail(1).item()
    signal = last_close > last_ma20 if last_ma20 is not None else False
    
    ma20_str = f"{last_ma20:.4f}" if last_ma20 is not None else "None"
    print(f"{code}: rows={n_rows}, last_date={last_date}, close={last_close:.4f}, ma20={ma20_str}, signal={signal}")

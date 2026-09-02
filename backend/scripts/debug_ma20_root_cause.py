#!/usr/bin/env python3
"""MA20 root cause analysis: sz.002415 @ 2024-06-03
Compare full-load vs lazy â€?last 25 dates, close values, dtypes, sort order."""
import sys, os, datetime, json
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from huggingface_hub import hf_hub_download

TARGET = datetime.date(2024, 6, 3)
CODE = 'sz.002415'

# ============================================================
# PATH A: Full-load (same as memory_probe.py / generate_golden.py)
# ============================================================
print("=== PATH A: Full-load ===")
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

# Apply QFQ exactly like DataManager
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj_expr = adj_col.last().over("code")
qfq_expr = pl.when(latest_adj_expr > 0).then(adj_col / latest_adj_expr).otherwise(1.0)
df_all = df_all.with_columns([
    (pl.col('open') * qfq_expr).cast(pl.Float32).alias('open'),
    (pl.col('high') * qfq_expr).cast(pl.Float32).alias('high'),
    (pl.col('low') * qfq_expr).cast(pl.Float32).alias('low'),
    (pl.col('close') * qfq_expr).cast(pl.Float32).alias('close'),
])

data_manager.df_daily = df_all

# Extract code data â€?sorted by date (as stored in df_daily after sort)
full_code = df_all.filter(pl.col('code') == CODE).sort('date')
print(f"Full total rows: {full_code.height}")

# Last 25 dates before and including target
full_last25 = full_code.filter(pl.col('date') <= TARGET).tail(25)

# Compute MA20 using Polars expression
full_last25_maj = full_last25.with_columns(
    pl.col('close').rolling_mean(window_size=20).alias('ma20_polars')
)
print(f"\nFull last 25 rows (date, close, ma20_polars):")
for row in full_last25_maj.iter_rows(named=True):
    print(f"  {row['date']}  close={row['close']:.4f}  ma20={row.get('ma20_polars', 'N/A')}")

# Extract the actual last 20 close values used by rolling_mean
full_last20 = full_code.filter(pl.col('date') <= TARGET).tail(20)
full_close_vals = full_last20['close'].to_list()
full_ma20_manual = sum(full_close_vals) / 20.0
print(f"\nFull last 20 close values: {[round(v, 4) for v in full_close_vals]}")
print(f"Full manual MA20: {full_ma20_manual:.6f}")
print(f"Full Polars MA20 (last row): {full_last25_maj['ma20_polars'].tail(1).item():.6f}")
print(f"Full close dtype: {full_code['close'].dtype}")
print(f"Full sort: [code, date] â€?verified")

# ============================================================
# PATH B: Lazy (load_qfq_window)
# ============================================================
print("\n\n=== PATH B: Lazy (load_qfq_window) ===")
raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()

lazy_df = raw.load_qfq_window(TARGET - datetime.timedelta(days=250), TARGET, latest_adj)
lazy_code = lazy_df.filter(pl.col('code') == CODE).sort('date')
print(f"Lazy total rows: {lazy_code.height}")

lazy_last25 = lazy_code.filter(pl.col('date') <= TARGET).tail(25)
lazy_last25_maj = lazy_last25.with_columns(
    pl.col('close').rolling_mean(window_size=20).alias('ma20_polars')
)
print(f"\nLazy last 25 rows (date, close, ma20_polars):")
for row in lazy_last25_maj.iter_rows(named=True):
    print(f"  {row['date']}  close={row['close']:.4f}  ma20={row.get('ma20_polars', 'N/A')}")

lazy_last20 = lazy_code.filter(pl.col('date') <= TARGET).tail(20)
lazy_close_vals = lazy_last20['close'].to_list()
lazy_ma20_manual = sum(lazy_close_vals) / 20.0
print(f"\nLazy last 20 close values: {[round(v, 4) for v in lazy_close_vals]}")
print(f"Lazy manual MA20: {lazy_ma20_manual:.6f}")
print(f"Lazy Polars MA20 (last row): {lazy_last25_maj['ma20_polars'].tail(1).item():.6f}")
print(f"Lazy close dtype: {lazy_code['close'].dtype}")

# ============================================================
# COMPARISON
# ============================================================
print("\n\n=== COMPARISON ===")
print(f"Full manual MA20:  {full_ma20_manual:.6f}")
print(f"Lazy manual MA20:  {lazy_ma20_manual:.6f}")
print(f"Manual diff:       {abs(full_ma20_manual - lazy_ma20_manual):.6f}")

print(f"\nFull Polars MA20:  {full_last25_maj['ma20_polars'].tail(1).item():.6f}")
print(f"Lazy Polars MA20:  {lazy_last25_maj['ma20_polars'].tail(1).item():.6f}")
print(f"Polars diff:       {abs(full_last25_maj['ma20_polars'].tail(1).item() - lazy_last25_maj['ma20_polars'].tail(1).item()):.6f}")

#é€è¡Œ close diff
full_c20 = full_last20['close'].to_list()
lazy_c20 = lazy_last20['close'].to_list()
print(f"\nClose-by-close diff (full - lazy):")
for i, (f, l) in enumerate(zip(full_c20, lazy_c20)):
    d = f - l
    print(f"  [{i}] full={f:.6f} lazy={l:.6f} diff={d:.6f}")

# Sort order check
print(f"\nFull code sort order (first 5 dates): {full_code.head(5)['date'].to_list()}")
print(f"Lazy code sort order (first 5 dates):  {lazy_code.head(5)['date'].to_list()}")
print(f"Full code sort order (last 5 dates):  {full_code.tail(5)['date'].to_list()}")
print(f"Lazy code sort order (last 5 dates):  {lazy_code.tail(5)['date'].to_list()}")

# Check for duplicate dates
full_dups = full_code.filter(pl.col('date') <= TARGET).select('date').height
full_unique = full_code.filter(pl.col('date') <= TARGET).select('date').unique().height
lazy_dups = lazy_code.filter(pl.col('date') <= TARGET).select('date').height
lazy_unique = lazy_code.filter(pl.col('date') <= TARGET).select('date').unique().height
print(f"\nFull: {full_dups} rows, {full_unique} unique dates")
print(f"Lazy: {lazy_dups} rows, {lazy_unique} unique dates")

# Save debug artifact
debug = {
    "code": CODE,
    "target_date": str(TARGET),
    "full_rows": full_code.height,
    "lazy_rows": lazy_code.height,
    "full_ma20_manual": full_ma20_manual,
    "lazy_ma20_manual": lazy_ma20_manual,
    "manual_diff": abs(full_ma20_manual - lazy_ma20_manual),
    "full_ma20_polars": full_last25_maj['ma20_polars'].tail(1).item(),
    "lazy_ma20_polars": lazy_last25_maj['ma20_polars'].tail(1).item(),
    "polars_diff": abs(full_last25_maj['ma20_polars'].tail(1).item() - lazy_last25_maj['ma20_polars'].tail(1).item()),
    "full_close_dtype": str(full_code['close'].dtype),
    "lazy_close_dtype": str(lazy_code['close'].dtype),
    "full_close_last20": full_c20,
    "lazy_close_last20": lazy_c20,
    "full_dates_first5": [str(d) for d in full_code.head(5)['date'].to_list()],
    "lazy_dates_first5": [str(d) for d in lazy_code.head(5)['date'].to_list()],
    "full_dates_last5": [str(d) for d in full_code.tail(5)['date'].to_list()],
    "lazy_dates_last5": [str(d) for d in lazy_code.tail(5)['date'].to_list()],
}
with open('backend/scripts/qfq_ma20_debug.json', 'w') as f:
    json.dump(debug, f, indent=2, default=str)
print(f"\nSaved: backend/scripts/qfq_ma20_debug.json")

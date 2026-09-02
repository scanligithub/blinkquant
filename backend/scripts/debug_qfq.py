import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

from core.raw_price_store import RawPriceStore

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
adj = raw_store.load_latest_adjust_factors()
print(f"Loaded {len(adj)} adjust factors")

# Test loading qfq window for target_date normalization
target_date = datetime.date(2024, 1, 15)
lookback = target_date - datetime.timedelta(days=365)
print(f"Loading qfq window: {lookback} to {target_date}")

df = raw_store.load_qfq_window(lookback, target_date, adj)
print(f"Loaded {df.height} rows")
if df.height > 0:
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Codes: {df['code'].n_unique()}")
    # Check if target_date exists
    target_rows = df.filter(pl.col("date") == target_date)
    print(f"Rows for target_date: {target_rows.height}")
else:
    print("No data loaded!")

import polars as pl
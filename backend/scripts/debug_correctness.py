import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.data_manager import DataManager, data_manager
from core.raw_price_store import RawPriceStore
from core.engine import selection_engine

# Load data via DataManager (old way)
print("=== Loading data via DataManager ===")
dm = DataManager()
dm._load_minimal_for_calendar = True
# Load just 2024 data for quick test
# Actually, let's load the full df_daily via the memory probe approach
# But that's slow. Let's just compare qfq for a specific stock.

# Use RawPriceStore to load qfq for a specific stock and date
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
adj = raw_store.load_latest_adjust_factors()

# Test qfq for a specific stock
code = 'sh.600000'
target_date = datetime.date(2024, 1, 15)
lookback = target_date - datetime.timedelta(days=60)

df_qfq = raw_store.load_qfq_window(lookback, target_date, adj)
stock_data = df_qfq.filter(pl.col("code") == code).sort("date")
print(f"RawPriceStore qfq for {code}:")
print(stock_data.tail(5))

# Now load via DataManager
# DataManager loads all data and applies qfq in memory
# This is slow, so let's just check the adjustFactor for this stock
print("\n=== Checking adjustFactor ===")
# Load raw data for this stock to see adjustFactor
raw_lf = pl.scan_parquet('C:/Users/scanl/.cache/huggingface/hub/datasets--scanli--stocka-data/snapshots/5e52f999b43654d0f8aed8944eaf6b4462979fb8/stock_kline_2024.parquet')
raw_stock = raw_lf.filter(pl.col("code") == code).select(["date", "close", "adjustFactor"]).collect()
print(f"Raw adjustFactor for {code} (last 10):")
print(raw_stock.tail(10))

# Check latest_adj for this code
print(f"\nlatest_adj for {code}: {adj.get(code)}")

# Compute qfq manually for the last row
last_row = raw_stock.tail(1)
raw_close = last_row["close"][0]
raw_adj = last_row["adjustFactor"][0]
latest = adj.get(code, 1.0)
if latest > 0:
    qfq_close = raw_close * raw_adj / latest
    print(f"Manual qfq: raw_close={raw_close}, raw_adj={raw_adj}, latest_adj={latest} -> qfq={qfq_close}")
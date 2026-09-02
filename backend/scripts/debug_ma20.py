import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.data_manager import DataManager, data_manager
from core.raw_price_store import RawPriceStore

# Compare MA20 for a specific stock and date between two approaches
code = 'sh.600000'
target_date = datetime.date(2024, 1, 15)
lookback = target_date - datetime.timedelta(days=60)

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
adj = raw_store.load_latest_adjust_factors()

# Lazy approach: load 60-day window and compute MA20
df_lazy = raw_store.load_qfq_window(lookback, target_date, adj)
lazy_stock = df_lazy.filter(pl.col("code") == code).sort("date")
lazy_stock = lazy_stock.with_columns(pl.col("close").rolling_mean(window_size=20).alias("ma20"))
print("Lazy approach MA20 (last 5):")
print(lazy_stock.tail(5).select(["date", "close", "ma20"]))

# DataManager approach: load full 2024 data
print("\n--- DataManager (2024 only) ---")
from huggingface_hub import hf_hub_download
p = hf_hub_download(repo_id='scanli/stocka-data', filename='stock_kline_2024.parquet',
                    repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
df = pl.read_parquet(p)
df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df = df.sort(['code', 'date'])

# Apply qfq like DataManager
adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
latest_adj = adj_col.last().over("code")
qfq_expr = pl.when(latest_adj > 0).then(adj_col / latest_adj).otherwise(1.0)

df_qfq = df.with_columns([
    (pl.col("open") * qfq_expr).cast(pl.Float32),
    (pl.col("high") * qfq_expr).cast(pl.Float32),
    (pl.col("low") * qfq_expr).cast(pl.Float32),
    (pl.col("close") * qfq_expr).cast(pl.Float32),
    (pl.col("volume") / qfq_expr).cast(pl.Float64)
])

dm_stock = df_qfq.filter(pl.col("code") == code).sort("date")
dm_stock = dm_stock.with_columns(pl.col("close").rolling_mean(window_size=20).alias("ma20"))
print("DataManager MA20 (last 5):")
print(dm_stock.tail(5).select(["date", "close", "ma20"]))

# Compare the MA20 values
print("\nComparison:")
lazy_last = lazy_stock.tail(1).select(["date", "close", "ma20"])
dm_last = dm_stock.tail(1).select(["date", "close", "ma20"])
print(f"Lazy: {lazy_last}")
print(f"DM:   {dm_last}")
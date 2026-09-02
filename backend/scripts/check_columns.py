import polars as pl
from huggingface_hub import hf_hub_download
import os
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

p = hf_hub_download(repo_id='scanli/stocka-data', filename='stock_kline_2024.parquet',
                    repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
df = pl.read_parquet(p)
print("All columns:", df.columns)
print("Shape:", df.shape)
print("Size MB:", round(df.estimated_size("mb"), 1))

keep = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjustFactor', 'pctChg', 'isST']
available = [c for c in keep if c in df.columns]
print("Needed:", available)
print("Extra:", [c for c in df.columns if c not in keep])

df_small = df.select(available)
print("Small size MB:", round(df_small.estimated_size("mb"), 1))
ratio = df.estimated_size("mb") / df_small.estimated_size("mb")
print("Reduction:", round(ratio, 1), "x")

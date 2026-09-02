#!/usr/bin/env python3
"""Quick debug: test lazy selection"""
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.raw_price_store import RawPriceStore
from core.engine import selection_engine

raw = RawPriceStore(hf_repo_id='scanli/stocka-data')
latest_adj = raw.load_latest_adjust_factors()
print(f'latest_adj: {len(latest_adj)} codes')

# Try loading a small window
target = datetime.date(2024, 6, 3)
start = target - datetime.timedelta(days=260)
df = raw.load_qfq_window(start, target, latest_adj)
print(f'QFQ window: {df.shape}, cols={df.columns}')
if not df.is_empty():
    print(f'Dates: {df.select("date").min().item()} to {df.select("date").max().item()}')
    print(f'Codes: {df.select("code").n_unique()}')
    print(df.head(3))
else:
    print('QFQ window is EMPTY')

# Try executing selector
print('\n--- Executing selector ---')
result = selection_engine.execute_selector(
    'CLOSE > MA(CLOSE, 20)', 'D', None,
    target_date=target, backtest_mode=True, raise_on_error=True,
    qfq_data_provider=raw, latest_adj=latest_adj
)
print(f'Result type: {type(result)}')
if hasattr(result, 'codes'):
    print(f'Selected {len(result.codes)} codes')
    if result.codes:
        print(f'First 5: {result.codes[:5]}')
else:
    print(f'Result: {result}')

"""Quick Selection profiler test on short period."""
import sys, os, datetime, time

_BACKEND_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, _BACKEND_ROOT)
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from huggingface_hub import hf_hub_download

# Load small dataset
dm = DataManager()
parts = []
for year in [2024]:
    p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                        repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
    df = pl.scan_parquet(p).select(['date','code','open','high','low','close','volume','amount',
                                     'adjustFactor','pctChg','isST']).collect()
    parts.append(df)
df = pl.concat(parts)
df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df = df.sort(['code','date'])
dm.df_daily = df
dm._compute_limit_flags()
dm._apply_forward_adjustment()
dm._append_prev_close()
dm._optimize_memory(dm.df_daily, 'df_daily')
dm._resample_all()
data_manager.df_daily = dm.df_daily
data_manager.df_weekly = dm.df_weekly
data_manager.df_monthly = dm.df_monthly
selection_engine._set_cache.clear()

# Enable profiler
selection_engine.profiler_start()

# Run selection on a few dates
dates = sorted(dm.df_daily.select(pl.col('date')).unique().sort('date').to_series().to_list())
test_dates = dates[-20:]
print(f"Testing on {len(test_dates)} dates...")

for d in test_dates:
    result, trace = selection_engine.execute_selector_with_trace(
        'CLOSE > MA(CLOSE, 20)', 'D', None,
        target_date=d, backtest_mode=True, raise_on_error=True
    )

# Get profiler results
prof = selection_engine.profiler_stop()
total = sum(prof.values())
print(f"\nSelection Profiler Results ({len(test_dates)} dates):")
print(f"{'Stage':<20} {'Time (s)':>10} {'%':>8}")
print("-" * 40)
for name, t in sorted(prof.items(), key=lambda x: -x[1]):
    pct = (t / total * 100) if total > 0 else 0
    bar = "#" * int(pct / 3)
    print(f"  {name:<18} {t:>10.3f} {pct:>7.1f}% {bar}")
print(f"  {'TOTAL':<18} {total:>10.3f} 100.0%")
print(f"  Call count: {len(test_dates)}")

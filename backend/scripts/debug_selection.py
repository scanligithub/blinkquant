import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

from core.raw_price_store import RawPriceStore
from core.engine import selection_engine

raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')

# Test selection engine with lazy loading
print("Testing selection engine with lazy loading...")
sel = selection_engine.execute_selector(
    formula='CLOSE > MA(CLOSE, 20)',
    timeframe='D',
    background_tasks=None,
    target_date=datetime.date(2024, 1, 15),
    backtest_mode=True,
    raise_on_error=True,
    trace=False,
    qfq_data_provider=raw_store,
    latest_adj=raw_store.load_latest_adjust_factors()
)

print(f"Selection result: {sel}")
if hasattr(sel, 'codes'):
    print(f"Codes: {sel.codes}")
elif isinstance(sel, dict):
    print(f"Dict result: {sel}")
import sys, os, datetime
sys.path.insert(0, 'backend')
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

from core.data_manager import DataManager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG
from core.fee_config import load_fee_schedule

def top_n_equal_weight_allocator(n):
    def allocator(codes, signal_date):
        if not codes:
            return {}
        picked = codes[:n]
        return {c: 1.0 / len(picked) for c in picked}
    return allocator

# Setup
dm = DataManager()
calendar = TradingCalendar()
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
fee_schedule = load_fee_schedule('config/fee_schedule.yaml')
allocator = top_n_equal_weight_allocator(5)

engine = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Test selection directly for a signal date
from core.engine import selection_engine
adj = raw_store.load_latest_adjust_factors()

# Test for a known signal date (weekly)
signal_dates = calendar.weekly_signal_dates(datetime.date(2024, 1, 1), datetime.date(2024, 3, 31))
print(f"Signal dates: {sorted(signal_dates)[:5]}")

for sig_date in sorted(signal_dates)[:3]:
    exec_d = calendar.next_trade_day(sig_date)
    print(f"\nSignal date: {sig_date}, Exec date: {exec_d}")
    
    sel = selection_engine.execute_selector(
        formula='CLOSE > MA(CLOSE, 20)',
        timeframe='D',
        background_tasks=None,
        target_date=sig_date,
        backtest_mode=True,
        raise_on_error=True,
        trace=False,
        qfq_data_provider=raw_store,
        latest_adj=adj
    )
    
    if hasattr(sel, 'codes'):
        print(f"  Selected {len(sel.codes)} codes: {sel.codes[:5]}...")
    else:
        print(f"  Error: {sel}")
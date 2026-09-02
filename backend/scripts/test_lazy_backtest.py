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

# Run small backtest
result = engine.run(
    formula='CLOSE > MA(CLOSE, 20)',
    start_date=datetime.date(2024, 1, 1),
    end_signal_date=datetime.date(2024, 3, 31),
    initial_cash=1_000_000,
    rebalance_freq='weekly',
    fee_schedule=fee_schedule,
)

print(f'Trades: {result.trades.height}')
equity_val = result.equity_curve['equity'].tail(1).item()
print(f'Equity: {equity_val:,.2f}')
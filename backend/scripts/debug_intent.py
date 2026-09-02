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
from core.portfolio import Portfolio, Position

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

# Test the full flow for one signal date
sig_date = datetime.date(2024, 1, 5)
exec_d = calendar.next_trade_day(sig_date)
print(f"Signal date: {sig_date}, Exec date: {exec_d}")

# Load execution prices
px_df = raw_store.load_execution_prices([exec_d])
new_prices = {row["code"]: {"open": row["open"], "close": row["close"]} for row in px_df.iter_rows(named=True)}
print(f"Loaded prices for {len(new_prices)} codes")

# Run selection
adj = raw_store.load_latest_adjust_factors()
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

if hasattr(sel, 'codes') and sel.codes:
    codes = sel.codes
    print(f"Selected {len(codes)} codes")
    
    # Apply allocator
    weights = allocator(codes, sig_date)
    print(f"Allocator returned {len(weights)} weights")
    
    # Test portfolio
    portfolio = Portfolio(initial_cash=1_000_000)
    print(f"Initial cash: {portfolio.cash}")
    
    # Initialize engine portfolio (needed for _generate_intents)
    engine.portfolio = portfolio
    
    # Generate intents
    intents = engine._generate_intents(weights, new_prices)
    print(f"Generated {len(intents)} intents")
    for i, intent in enumerate(intents[:5]):
        print(f"  {intent}")
else:
    print(f"Selection error: {sel}")

# Test execution engine
from core.execution import ExecutionEngine
exec_engine = ExecutionEngine(exec_config=MVP_EXECUTION_CONFIG, fee_config=FeeConfig())
limit_flags = {}  # Simplified
report = exec_engine.execute(
    execution_date=exec_d,
    intents=intents,
    positions=portfolio.positions,
    raw_prices=new_prices,
    cash=portfolio.cash,
    limit_flags=limit_flags,
    fee_config=fee_schedule.get_fee_config(exec_d),
)
print(f"Execution report: {len(report.fills)} fills, {len(report.rejections)} rejections")
for fill in report.fills[:5]:
    print(f"  Fill: {fill}")
for rej in report.rejections[:5]:
    print(f"  Rejection: {rej}")
"""P2-MEM: Full Lazy Loading Memory Probe â€?BacktestEngine with zero pre-loaded data."""
import sys, os, time, datetime, json, threading, psutil

_BACKEND_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, _BACKEND_ROOT)
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import polars as pl
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

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

class RSSSampler:
    def __init__(self, interval=3.0):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.samples = []
        self._stop = threading.Event()
        self._thread = None

    def _sample_loop(self):
        while not self._stop.is_set():
            try:
                rss = self.process.memory_info().rss / (1024 * 1024)
                sys_mem = psutil.virtual_memory()
                self.samples.append({
                    "t": round(time.time() - self._start_time, 1),
                    "rss_mb": round(rss, 1),
                    "system_used_pct": sys_mem.percent,
                    "system_available_mb": round(sys_mem.available / (1024 * 1024), 0),
                })
            except Exception:
                pass
            self._stop.wait(self.interval)

    def start(self):
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def peak_rss(self):
        if not self.samples:
            return 0
        return max(s["rss_mb"] for s in self.samples)

log("=== Full Lazy Loading Memory Probe: B4 (2010-2024) ===")
log("Phase 0: Baseline measurement")

sampler = RSSSampler(interval=3.0)
sampler.start()

proc = psutil.Process(os.getpid())
baseline_rss = proc.memory_info().rss / (1024 * 1024)
sys_mem = psutil.virtual_memory()
log(f"Baseline RSS: {baseline_rss:.1f} MB")
log(f"System: {sys_mem.total / (1024**3):.1f} GB total, {sys_mem.available / (1024**3):.1f} GB available, {sys_mem.percent}% used")

# ========== PHASE 1: Calendar setup with actual trading dates ==========
log("\nPhase 1: Calendar setup with actual trading dates")
t_cal_start = time.time()

# Get actual trading dates from raw data
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data')
trade_dates = raw_store.get_trading_dates(datetime.date(2010, 1, 4), datetime.date(2024, 12, 31))
log(f"Loaded {len(trade_dates)} actual trading dates from raw data")

calendar = TradingCalendar()
calendar.set_trade_dates(trade_dates)

cal_time = time.time() - t_cal_start
rss_after_cal = proc.memory_info().rss / (1024 * 1024)
log(f"Calendar setup: {cal_time:.1f}s, RSS={rss_after_cal:.0f} MB, delta={rss_after_cal - baseline_rss:.0f} MB")

# ========== PHASE 2: Pre-compute latest_adj only ==========
log("\nPhase 2: Pre-compute latest_adj only")
t_pre_start = time.time()

latest_adj = raw_store.load_latest_adjust_factors()
log(f"  latest_adj: {len(latest_adj)} codes")

# Skip pre-computing limit_flags â€?use on-demand loading in backtest engine

pre_time = time.time() - t_pre_start
rss_after_pre = proc.memory_info().rss / (1024 * 1024)
log(f"Pre-compute: {pre_time:.1f}s, RSS={rss_after_pre:.0f} MB, delta={rss_after_pre - baseline_rss:.0f} MB")

# ========== PHASE 3: Backtest execution (fully lazy) ==========
log("\nPhase 3: Backtest execution (fully lazy)")
t_bt_start = time.time()

allocator = top_n_equal_weight_allocator(20)
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")

engine = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

# Inject pre-computed latest_adj only
engine._latest_adj = latest_adj

result = engine.run(
    formula="CLOSE > MA(CLOSE, 20)",
    start_date=datetime.date(2010, 1, 4),
    end_signal_date=datetime.date(2024, 12, 30),
    initial_cash=10_000_000,
    rebalance_freq="weekly",
    fee_schedule=fee_schedule,
)

bt_time = time.time() - t_bt_start
rss_after_bt = proc.memory_info().rss / (1024 * 1024)
log(f"Backtest complete: {bt_time:.1f}s, RSS={rss_after_bt:.0f} MB, delta={rss_after_bt - baseline_rss:.0f} MB")

# ========== RESULTS ==========
sampler.stop()
peak_rss = sampler.peak_rss()
total_time = time.time() - t_cal_start

log(f"\n{'='*60}")
log("FULL LAZY LOADING MEMORY PROBE RESULTS")
log(f"{'='*60}")
log(f"Baseline RSS:      {baseline_rss:.1f} MB")
log(f"Peak RSS:          {peak_rss:.1f} MB")
log(f"Delta RSS:         {peak_rss - baseline_rss:.1f} MB")
log(f"Final RSS:         {rss_after_bt:.1f} MB")
log(f"Total duration:    {total_time:.1f}s ({total_time/60:.1f} min)")
log(f"Trades:            {result.trades.height}")
log(f"Equity:            {result.equity_curve['equity'].tail(1).item():,.2f}")

# HF Space assessment
log(f"\n--- HF Space Assessment ---")
if peak_rss < 4096:
    log(f"Peak {peak_rss:.0f} MB < 4 GB: GREEN")
elif peak_rss < 8192:
    log(f"Peak {peak_rss:.0f} MB < 8 GB: WARNING â€?acceptable but monitor")
elif peak_rss < 12288:
    log(f"Peak {peak_rss:.0f} MB < 12 GB: CRITICAL â€?needs optimization")
else:
    log(f"Peak {peak_rss:.0f} MB >= 12 GB: HARD STOP â€?must segment or shard")

# Save results
results = {
    "baseline_rss_mb": baseline_rss,
    "peak_rss_mb": peak_rss,
    "delta_rss_mb": peak_rss - baseline_rss,
    "final_rss_mb": rss_after_bt,
    "total_time_sec": total_time,
    "trades": result.trades.height,
    "equity": result.equity_curve['equity'].tail(1).item(),
    "samples": sampler.samples,
}
with open("benchmarks/P2-5_lazy_memory_probe.json", "w") as f:
    json.dump(results, f, indent=2, default=str)
log(f"\nSaved: benchmarks/P2-5_lazy_memory_probe.json")
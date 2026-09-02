"""P2-5: Memory Probe â€?measure real process RSS during B4 execution.

Samples RSS every 5 seconds, records per-stage peaks, outputs time-series JSON.
"""
import sys, os, datetime, time, json, threading

_BACKEND_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, _BACKEND_ROOT)
pass  # HF_TOKEN set via env
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import psutil
import polars as pl
from core.data_manager import DataManager, data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from huggingface_hub import hf_hub_download

KEEP_COLS = ["date", "code", "open", "high", "low", "close",
             "volume", "amount", "adjustFactor", "pctChg", "isST"]

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

class RSSSampler:
    """Background thread that samples RSS every N seconds."""
    def __init__(self, interval=5.0):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.samples = []  # [(timestamp, rss_mb, system_used_pct)]
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

    def baseline_rss(self):
        if not self.samples:
            return 0
        return self.samples[0]["rss_mb"]

# ========== PHASE 1: Record baseline ==========
log("=== Memory Probe: B4 (2010-2024) ===")
log("Phase 0: Baseline measurement")

sampler = RSSSampler(interval=3.0)
sampler.start()

proc = psutil.Process(os.getpid())
baseline_rss = proc.memory_info().rss / (1024 * 1024)
sys_mem = psutil.virtual_memory()
log(f"Baseline RSS: {baseline_rss:.1f} MB")
log(f"System: {sys_mem.total / (1024**3):.1f} GB total, {sys_mem.available / (1024**3):.1f} GB available, {sys_mem.percent}% used")

# ========== PHASE 2: Data loading (Incremental concatenation) ==========
log("\nPhase 1: Data loading (incremental)")
t_load_start = time.time()

dm = DataManager()
df = None  # Incremental concatenation â€?avoid holding all yearly DataFrames simultaneously
for year in range(2009, 2025):
    try:
        p = hf_hub_download(repo_id='scanli/stocka-data', filename=f'stock_kline_{year}.parquet',
                            repo_type='dataset', token=os.getenv('HF_TOKEN'), endpoint=os.getenv('HF_ENDPOINT'))
        scan = pl.scan_parquet(p)
        available = scan.collect_schema().names()
        use_cols = [c for c in KEEP_COLS if c in available]
        year_df = scan.select(use_cols).collect()
        if df is None:
            df = year_df
        else:
            df = pl.concat([df, year_df])
            del year_df  # Release immediately â€?only one year's data alive at a time
        rss_now = proc.memory_info().rss / (1024 * 1024)
        log(f"  {year}: {df.height} rows, RSS={rss_now:.0f} MB, delta={rss_now - baseline_rss:.0f} MB")
    except Exception as e:
        log(f"  {year}: skipped ({e})")
df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d'))
df = df.sort(['code', 'date'])
dm.df_daily = df
dm._compute_limit_flags()
dm._apply_forward_adjustment()
dm._append_prev_close()
dm._optimize_memory(dm.df_daily, 'df_daily')
dm._resample_all()
data_manager.df_daily = dm.df_daily
data_manager.df_weekly = dm.df_weekly
data_manager.df_monthly = dm.df_monthly

rss_after_load = proc.memory_info().rss / (1024 * 1024)
load_time = time.time() - t_load_start
log(f"Data load complete: {load_time:.1f}s, RSS={rss_after_load:.0f} MB, delta={rss_after_load - baseline_rss:.0f} MB")

# ========== PHASE 3: Signal matrix precompute ==========
log("\nPhase 2: Signal matrix precompute")
selection_engine._signal_matrix_enabled = True
t_precompute_start = time.time()

calendar = TradingCalendar()
trade_dates = sorted(dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list())
calendar.set_trade_dates(trade_dates)

selection_engine._set_cache.clear()
selection_engine._signal_matrix_cache.clear()

# Trigger precompute
precomputed = selection_engine._precompute_signal_matrix("CLOSE > MA(CLOSE, 20)", 'D')
rss_after_precompute = proc.memory_info().rss / (1024 * 1024)
precompute_time = time.time() - t_precompute_start
log(f"Precompute: {precompute_time:.1f}s, success={precomputed}, RSS={rss_after_precompute:.0f} MB, delta={rss_after_precompute - baseline_rss:.0f} MB")

# ========== PHASE 4: Backtest execution ==========
log("\nPhase 3: Backtest execution")
raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
allocator = top_n_equal_weight_allocator(20)

engine = BacktestEngine(
    calendar=calendar, selection_engine=selection_engine,
    raw_price_store=raw_store, fee_config=FeeConfig(),
    execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
)

t_bt_start = time.time()
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

# ========== PHASE 5: Stop sampler and report ==========
sampler.stop()
peak_rss = sampler.peak_rss()
total_time = time.time() - t_load_start

# Find peak stage
peak_sample = max(sampler.samples, key=lambda s: s["rss_mb"]) if sampler.samples else {}
peak_t = peak_sample.get("t", 0)

log(f"\n{'='*60}")
log("MEMORY PROBE RESULTS")
log(f"{'='*60}")
log(f"Baseline RSS:      {baseline_rss:.1f} MB")
log(f"Peak RSS:          {peak_rss:.1f} MB")
log(f"Delta RSS:         {peak_rss - baseline_rss:.1f} MB")
log(f"Peak at:           {peak_t:.0f}s")
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
    "peak_at_sec": peak_t,
    "final_rss_mb": rss_after_bt,
    "total_time_sec": total_time,
    "trades": result.trades.height,
    "equity": result.equity_curve['equity'].tail(1).item(),
    "samples": sampler.samples,
}
with open("benchmarks/P2-5_memory_probe.json", "w") as f:
    json.dump(results, f, indent=2, default=str)
log(f"\nSaved: benchmarks/P2-5_memory_probe.json")

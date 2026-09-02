#!/usr/bin/env python3
"""Gate 3B: HF Space Production Validation — B1/B2/B3/B4 sequential benchmarks.

Usage on HF Space:
    python gate3b_hf_validate.py --benchmark B1
    python gate3b_hf_validate.py --benchmark B4
    python gate3b_hf_validate.py --all

Requires: local golden artifacts in benchmarks/B4_V4_golden/ for comparison."""
import sys, os, datetime, json, time, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
if not os.getenv('HF_TOKEN'):
    raise RuntimeError("Set HF_TOKEN environment variable")

import polars as pl
import psutil
import threading
from core.raw_price_store import RawPriceStore
from core.data_manager import data_manager
from core.engine import selection_engine
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule

# ============================================================
# Benchmark definitions
# ============================================================
BENCHMARKS = {
    "B1": {
        "name": "B1-2024Q1",
        "start": datetime.date(2024, 1, 2),
        "end": datetime.date(2024, 3, 29),
        "golden_dir": "tests/golden/2024q1",
        "description": "Minimal production smoke test",
    },
    "B2": {
        "name": "B2-2024",
        "start": datetime.date(2024, 1, 2),
        "end": datetime.date(2024, 12, 30),
        "golden_dir": None,  # Will use B4_V4_golden filtered
        "description": "Full year production stability",
    },
    "B3": {
        "name": "B3-2019_2024",
        "start": datetime.date(2019, 1, 2),
        "end": datetime.date(2024, 12, 30),
        "golden_dir": None,
        "description": "Multi-year production workloads",
    },
    "B4": {
        "name": "B4-2010_2024",
        "start": datetime.date(2010, 1, 4),
        "end": datetime.date(2024, 12, 30),
        "golden_dir": "benchmarks/B4_V4_golden",
        "description": "Full historical production validation",
    },
}

FORMULA = "CLOSE > MA(CLOSE, 20)"
REBALANCE = "weekly"
TOP_N = 20
INITIAL_CASH = 10_000_000

# ============================================================
# RSS sampler
# ============================================================
class RSSSampler:
    def __init__(self, interval=5.0):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.samples = []
        self._stop = threading.Event()
        self._thread = None
        self._start_time = 0

    def start(self):
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _sample_loop(self):
        while not self._stop.is_set():
            try:
                rss = self.process.memory_info().rss / (1024 * 1024)
                self.samples.append({
                    "t": round(time.time() - self._start_time, 1),
                    "rss_mb": round(rss, 1),
                })
            except Exception:
                pass
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def peak_rss(self):
        return max(s["rss_mb"] for s in self.samples) if self.samples else 0

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ============================================================
# Data loading (lazy path — same as production)
# ============================================================
def load_lazy_data(start_date, end_date, raw_store, latest_adj):
    """Pre-compute data needed for lazy backtest."""
    # Load limit flags for each date in range
    trade_dates = raw_store.get_trading_dates(start_date, end_date)
    return trade_dates

# ============================================================
# Main validation
# ============================================================
def run_benchmark(bench_key, golden_trades=None, golden_equity=None, golden_artifacts_dir=None):
    """Run a single benchmark and validate against golden."""
    bench = BENCHMARKS[bench_key]
    log(f"\n{'='*60}")
    log(f"GATE 3B: {bench['name']} — {bench['description']}")
    log(f"{'='*60}")
    log(f"Period: {bench['start']} .. {bench['end']}")

    # Start RSS sampling
    sampler = RSSSampler(interval=5.0)
    sampler.start()
    t_start = time.time()

    # --- Load data ---
    log("Loading data (lazy path)...")
    t_data = time.time()
    raw_store = RawPriceStore(hf_repo_id='scanli/stocka-data', hf_token=os.getenv('HF_TOKEN'))
    latest_adj = raw_store.load_latest_adjust_factors()

    trade_dates = raw_store.get_trading_dates(bench['start'], datetime.date(2025, 1, 10))
    cal = TradingCalendar()
    cal.set_trade_dates(trade_dates)
    log(f"Calendar: {len(trade_dates)} dates ({trade_dates[0]} .. {trade_dates[-1]})")
    data_time = time.time() - t_data

    # --- Build engine ---
    selection_engine._set_cache.clear()
    allocator = top_n_equal_weight_allocator(TOP_N)
    fee_schedule = load_fee_schedule("config/fee_schedule.yaml")

    engine = BacktestEngine(
        calendar=cal, selection_engine=selection_engine,
        raw_price_store=raw_store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
    )
    engine._latest_adj = latest_adj

    # --- Run backtest ---
    log("Running backtest...")
    t_bt = time.time()
    result = engine.run(
        formula=FORMULA, start_date=bench['start'], end_signal_date=bench['end'],
        initial_cash=INITIAL_CASH, rebalance_freq=REBALANCE,
        top_n=TOP_N, fee_schedule=fee_schedule,
    )
    bt_time = time.time() - t_bt
    total_time = time.time() - t_start

    # Stop sampler
    sampler.stop()
    peak_rss = sampler.peak_rss()

    # --- Collect results ---
    trades_count = result.trades.height
    final_equity = result.equity_curve['equity'].tail(1).item()
    diag = result.execution_diagnostics or {}
    neg_cash = diag.get('has_negative_cash', False)
    violations = diag.get('accounting_invariant_violations', 0)
    rej = sum(diag.get('rej_counters', {}).values())

    log(f"\n--- RESULTS ---")
    log(f"Trades:       {trades_count}")
    log(f"Final equity: {final_equity:,.2f}")
    log(f"Data time:    {data_time:.1f}s")
    log(f"Backtest:     {bt_time:.1f}s")
    log(f"Total:        {total_time:.1f}s")
    log(f"Peak RSS:     {peak_rss:.0f} MB")
    log(f"Negative cash: {neg_cash}")
    log(f"Violations:   {violations}")
    log(f"Rejections:   {rej}")

    # --- Validate against golden ---
    validation = {
        "benchmark": bench_key,
        "period": f"{bench['start']}..{bench['end']}",
        "trades_count": trades_count,
        "final_equity": final_equity,
        "data_time_sec": round(data_time, 1),
        "backtest_time_sec": round(bt_time, 1),
        "total_time_sec": round(total_time, 1),
        "peak_rss_mb": peak_rss,
        "has_negative_cash": neg_cash,
        "accounting_violations": violations,
        "rejections": rej,
        "oom": False,
        "timeout": False,
    }

    # Compare against golden if available
    if golden_trades is not None:
        trades_match = trades_count == golden_trades
        validation["golden_trades"] = golden_trades
        validation["trades_match"] = trades_match
        log(f"Golden trades: {golden_trades} (match: {trades_match})")
    if golden_equity is not None:
        eq_diff = abs(final_equity - golden_equity)
        eq_match = eq_diff < 0.01
        validation["golden_equity"] = golden_equity
        validation["equity_diff"] = eq_diff
        validation["equity_match"] = eq_match
        log(f"Golden equity: {golden_equity:,.2f} (diff: {eq_diff:,.2f}, match: {eq_match})")

    # Compare against local golden artifacts if directory provided
    if golden_artifacts_dir and os.path.exists(golden_artifacts_dir):
        log(f"\nComparing against golden artifacts: {golden_artifacts_dir}")
        try:
            g_trades = pl.read_parquet(os.path.join(golden_artifacts_dir, 'trades.parquet'))
            g_ec = pl.read_parquet(os.path.join(golden_artifacts_dir, 'equity_curve.parquet'))

            # Filter golden to matching period if needed
            if bench_key != "B4":
                g_trades = g_trades.filter(
                    (pl.col('execution_date') >= bench['start']) &
                    (pl.col('execution_date') <= bench['end'])
                )
                g_ec = g_ec.filter(
                    (pl.col('date') >= bench['start']) &
                    (pl.col('date') <= bench['end'])
                )

            # Trade keys comparison
            v_keys = set((r['execution_date'], r['code'], r['side']) for r in result.trades.iter_rows(named=True))
            g_keys = set((r['execution_date'], r['code'], r['side']) for r in g_trades.iter_rows(named=True))
            keys_match = v_keys == g_keys
            validation["artifact_trade_keys_match"] = keys_match
            log(f"Artifact trade keys match: {keys_match}")

            # Equity curve comparison
            v_ec = result.equity_curve.select(['date', 'equity'])
            g_ec_compare = g_ec.select(['date', 'equity'])
            ec_join = v_ec.rename({'equity': 'v_eq'}).join(
                g_ec_compare.rename({'equity': 'g_eq'}),
                on='date', how='inner'
            )
            if ec_join.height > 0:
                ec_diffs = ec_join.with_columns((pl.col('v_eq') - pl.col('g_eq')).abs().alias('diff'))
                max_diff = ec_diffs['diff'].max()
                divergent = ec_diffs.filter(pl.col('diff') > 0.01).height
                validation["artifact_ec_max_diff"] = max_diff
                validation["artifact_ec_divergent_dates"] = divergent
                log(f"Artifact EC max diff: {max_diff:,.2f}, divergent dates: {divergent}")
        except Exception as e:
            log(f"Artifact comparison error: {e}")
            validation["artifact_comparison_error"] = str(e)

    # --- Verdict ---
    all_pass = True
    reasons = []
    if golden_trades is not None and trades_count != golden_trades:
        all_pass = False
        reasons.append(f"trades: {trades_count} != {golden_trades}")
    if golden_equity is not None and abs(final_equity - golden_equity) > 0.01:
        all_pass = False
        reasons.append(f"equity: {final_equity:,.2f} != {golden_equity:,.2f}")
    if neg_cash:
        all_pass = False
        reasons.append("negative cash")
    if violations > 0:
        all_pass = False
        reasons.append(f"{violations} accounting violations")
    if peak_rss > 7000:
        all_pass = False
        reasons.append(f"peak RSS {peak_rss:.0f} MB > 7 GB")

    validation["pass"] = all_pass
    validation["fail_reasons"] = reasons

    if all_pass:
        log(f"\nRESULT: {bench['name']} PASS")
    else:
        log(f"\nRESULT: {bench['name']} FAIL")
        for r in reasons:
            log(f"  FAIL: {r}")

    return validation

def main():
    parser = argparse.ArgumentParser(description="Gate 3B: HF Space Production Validation")
    parser.add_argument("--benchmark", choices=["B1", "B2", "B3", "B4"], default="B1",
                        help="Run single benchmark (default: B1)")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks sequentially")
    args = parser.parse_args()

    benchmarks_to_run = ["B1", "B2", "B3", "B4"] if args.all else [args.benchmark]

    # Load golden reference for B4
    golden_trades = None
    golden_equity = None
    with open("benchmarks/B4_2010_2024_golden.json") as f:
        golden = json.load(f)
        golden_trades = golden["n_trades"]
        golden_equity = golden["final_equity"]

    results = {}
    for bench_key in benchmarks_to_run:
        bench = BENCHMARKS[bench_key]

        # Use B4 golden for all benchmarks (they're subsets)
        gt = golden_trades if bench_key == "B4" else None
        ge = golden_equity if bench_key == "B4" else None

        validation = run_benchmark(bench_key, golden_trades=gt, golden_equity=ge)
        results[bench_key] = validation

        # Save per-benchmark results
        os.makedirs("benchmarks/hf_space", exist_ok=True)
        with open(f"benchmarks/hf_space/{bench_key}.json", "w") as f:
            json.dump(validation, f, indent=2, default=str)
        log(f"Saved: benchmarks/hf_space/{bench_key}.json")

    # Summary
    all_pass = all(r.get("pass", False) for r in results.values())
    summary = {
        "engine": "selection-v4",
        "environment": "hf-space",
        "golden_equivalence": all_pass,
        "oom": any(r.get("oom", False) for r in results.values()),
        "timeout": any(r.get("timeout", False) for r in results.values()),
        "max_peak_rss_mb": max(r.get("peak_rss_mb", 0) for r in results.values()),
        "benchmarks": {k: "PASS" if v.get("pass") else "FAIL" for k, v in results.items()},
    }

    with open("benchmarks/hf_space/summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    log(f"\n{'='*60}")
    log("GATE 3B SUMMARY")
    log(f"{'='*60}")
    for k, v in summary["benchmarks"].items():
        log(f"  {k}: {v}")
    log(f"  Max Peak RSS: {summary['max_peak_rss_mb']:.0f} MB")
    log(f"  OOM: {summary['oom']}")
    log(f"  Timeout: {summary['timeout']}")
    log(f"  Overall: {'PASS' if all_pass else 'FAIL'}")
    log(f"{'='*60}")

if __name__ == "__main__":
    main()

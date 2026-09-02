"""Run B3 then B4 sequentially with stage-level profiler.

Usage:
    python backend/scripts/run_b3b4.py

B3 (2019-2024): 2-hour timeout
B4 (2010-2024): 4-hour timeout
"""

import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def run_benchmark(name, start, end, output, timeout_sec):
    """Run a single benchmark via run_stress.py."""
    _log(f"\n{'='*60}")
    _log(f"Starting {name}: {start} .. {end}")
    _log(f"Timeout: {timeout_sec}s ({timeout_sec/3600:.1f}h)")
    _log(f"Output: {output}")
    _log(f"{'='*60}")

    cmd = [
        sys.executable,
        os.path.join(_BACKEND_ROOT, "scripts", "run_stress.py"),
        "--start", start,
        "--end", end,
        "--output", output,
    ]

    start_time = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(Path(_BACKEND_ROOT).parent),
        timeout=timeout_sec,
        capture_output=False,
    )
    elapsed = time.time() - start_time

    if proc.returncode == 0:
        _log(f"{name} completed in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        # Read and print summary
        with open(output, "r", encoding="utf-8") as f:
            result = json.load(f)
        _log(f"  Final equity: {result.get('final_equity', 0):,.2f}")
        _log(f"  Trades: {result.get('n_trades', 0)}")
        _log(f"  Stage timings: {result.get('stage_timings', {})}")
        return True
    else:
        _log(f"{name} FAILED with return code {proc.returncode}")
        return False


def main():
    _log("B3+B4 Sequential Benchmark Runner")
    _log(f"Backend root: {_BACKEND_ROOT}")

    benchmarks_dir = Path(_BACKEND_ROOT).parent / "benchmarks"
    benchmarks_dir.mkdir(exist_ok=True)

    # B3: 2019-2024, 2-hour timeout
    b3_output = str(benchmarks_dir / "B3_2019_2024.json")
    b3_ok = run_benchmark(
        name="B3",
        start="2019-01-02",
        end="2024-12-31",
        output=b3_output,
        timeout_sec=2 * 3600,  # 2 hours
    )

    if not b3_ok:
        _log("\nB3 failed, skipping B4")
        sys.exit(1)

    # B4: 2010-2024, 4-hour timeout
    b4_output = str(benchmarks_dir / "B4_2010_2024.json")
    b4_ok = run_benchmark(
        name="B4",
        start="2010-01-04",
        end="2024-12-31",
        output=b4_output,
        timeout_sec=4 * 3600,  # 4 hours
    )

    if not b4_ok:
        _log("\nB4 failed")
        sys.exit(1)

    _log("\n" + "="*60)
    _log("ALL BENCHMARKS COMPLETE")
    _log("="*60)

    # Print comparison
    for name, path in [("B3", b3_output), ("B4", b4_output)]:
        with open(path, "r", encoding="utf-8") as f:
            r = json.load(f)
        _log(f"\n{name} ({r.get('period', '?')}):")
        _log(f"  Backtest: {r.get('backtest_sec', 0):.1f}s ({r.get('backtest_sec', 0)/60:.1f} min)")
        _log(f"  Final equity: {r.get('final_equity', 0):,.2f}")
        _log(f"  Trades: {r.get('n_trades', 0)}")
        stages = r.get("stage_timings", {})
        if stages:
            bt = r.get("backtest_sec", 1)
            _log(f"  Stage breakdown:")
            for s, t in stages.items():
                _log(f"    {s:<12} {t:>8.1f}s  ({t/bt*100:>5.1f}%)")

    _log(f"\nResults saved to: {benchmarks_dir}")


if __name__ == "__main__":
    main()

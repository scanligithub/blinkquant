#!/usr/bin/env python3
"""Run B1-B4 benchmarks on HF Space sequentially."""
import json
import urllib.request
import time
import sys

BENCHMARKS = ["B1", "B2", "B3", "B4"]
GOLDEN = {
    "B1": {"trades": 395, "equity": 6791917.78},
    "B4": {"trades": 20678, "equity": 3371518.82},
}

url = "https://scanli-blinkquant-node1.hf.space/api/v1/benchmark"

for bench in BENCHMARKS:
    print(f"\n{'='*60}")
    print(f"Running {bench}...")
    print(f"{'='*60}")
    data = json.dumps({"benchmark": bench}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            result = json.loads(resp.read().decode())
            elapsed = time.time() - t0
            print(json.dumps(result, indent=2))
            print(f"Elapsed: {elapsed:.1f}s")

            # Validate
            if bench in GOLDEN:
                g = GOLDEN[bench]
                trades_ok = result["trades"] == g["trades"]
                equity_ok = abs(result["final_equity"] - g["equity"]) < 1
                print(f"Trades match: {trades_ok} ({result['trades']} vs {g['trades']})")
                print(f"Equity match: {equity_ok} ({result['final_equity']:.2f} vs {g['equity']:.2f})")
                status = "PASS" if trades_ok and equity_ok and not result["has_negative_cash"] and result["accounting_violations"] == 0 else "FAIL"
                print(f"{bench}: {status}")
            else:
                status = "PASS" if not result["has_negative_cash"] and result["accounting_violations"] == 0 else "FAIL"
                print(f"{bench}: {status} (no golden reference)")

            # Save result
            with open(f"benchmarks/hf_space/{bench}.json", "w") as f:
                json.dump(result, f, indent=2)
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}")
        print(f"{bench}: FAIL (HTTP error)")
    except Exception as e:
        print(f"Error: {e}")
        print(f"{bench}: FAIL")


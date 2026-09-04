"""Smoke-test: submit async backtest to HF node and poll until done."""
import requests
import time
import json

NODE = "https://scanli-blinkquant-node1.hf.space"

# 1. Health check
r = requests.get(f"{NODE}/api/v1/health", timeout=10)
print(f"Health: {r.json()}")

# 2. Submit async backtest job (small range for fast smoke test)
payload = {
    "formula": "CLOSE > MA(CLOSE, 20)",
    "start_date": "2024-01-02",
    "end_signal_date": "2024-01-10",
    "initial_cash": 10000000,
}
print(f"\nSubmitting backtest: {json.dumps(payload, indent=2)}")
r = requests.post(f"{NODE}/api/v1/backtest/async", json=payload, timeout=30)
print(f"Response status: {r.status_code}")
result = r.json()
print(f"Response: {json.dumps(result, indent=2)}")

job_id = result.get("job_id")
if not job_id:
    print("ERROR: No job_id returned")
    exit(1)

print(f"\nJob ID: {job_id}")
initial_status = result.get("status")
print(f"Initial status: {initial_status}")

# Verify initial status is queued
assert initial_status == "queued", f"Expected 'queued', got '{initial_status}'"
print("PASS: Initial status is 'queued'")

# 3. Poll for results
print("\nPolling for results...")
for i in range(120):  # max 10 minutes
    time.sleep(5)
    r = requests.get(f"{NODE}/api/v1/backtest/async/{job_id}", timeout=15)
    status_result = r.json()
    current_status = status_result.get("status")
    elapsed = i * 5
    print(f"  [{elapsed:>4d}s] Status: {current_status}", end="")

    if current_status == "running":
        # Verify we see running at some point
        print(" - in progress...")
    elif current_status == "done":
        data = status_result.get("data", {})
        print()
        trades = data.get("trades", [])
        equity_curve = data.get("equity_curve", [])
        metrics = data.get("metrics", {})
        print(f"\n  === RESULTS ===")
        print(f"  Trades: {len(trades)}")
        print(f"  Equity curve points: {len(equity_curve)}")
        if equity_curve:
            final = equity_curve[-1]
            print(f"  Final equity: {final.get('equity', 'N/A')}")
        print(f"  Metrics: {json.dumps(metrics, indent=4)}")

        # Verify results are non-empty
        assert len(equity_curve) > 0, "Equity curve is empty"
        print("\n  PASS: Results are non-empty")
        print("\n=== BACKTEST COMPLETED SUCCESSFULLY ===")
        break
    elif current_status in ("failed", "cancelled", "expired"):
        error_msg = status_result.get("error", "unknown")
        print(f" - ERROR: {error_msg}")
        print("\n=== BACKTEST FAILED ===")
        exit(1)
    else:
        print(" - waiting...")
else:
    print(f"\n=== TIMEOUT: backtest did not complete in 600s ===")
    exit(1)

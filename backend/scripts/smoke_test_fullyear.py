"""Smoke test with retry across nodes and longer timeouts."""
import requests
import time
import json

NODES = [
    "https://scanli-blinkquant-node2.hf.space",
    "https://scanli-blinkquant-node3.hf.space",
    "https://scanli-blinkquant-node1.hf.space",
]

payload = {
    "formula": "CLOSE > MA(CLOSE, 20)",
    "start_date": "2024-01-02",
    "end_signal_date": "2024-12-30",
    "initial_cash": 10000000,
}

job_id = None
node_url = None

# Try each node
for n in NODES:
    print(f"Trying {n}...")
    try:
        r = requests.post(f"{n}/api/v1/backtest/async", json=payload, timeout=120)
        result = r.json()
        job_id = result.get("job_id")
        node_url = n
        print(f"  Submitted! job_id={job_id[:8]}... status={result.get('status')}")
        break
    except Exception as e:
        print(f"  Failed: {e}")
        continue

if not job_id:
    print("ERROR: Could not submit to any node")
    exit(1)

# Poll
start_time = time.time()
for i in range(240):
    time.sleep(5)
    try:
        r = requests.get(f"{node_url}/api/v1/backtest/async/{job_id}", timeout=120)
        status = r.json()
    except Exception as e:
        print(f"  [{time.time()-start_time:>5.0f}s] poll error: {e}")
        continue

    s = status.get("status")
    elapsed = time.time() - start_time
    print(f"  [{elapsed:>5.0f}s] {s}", end="")

    if s == "done":
        data = status.get("data", {})
        trades = len(data.get("trades", []))
        eq = data.get("equity_curve", [])
        final_eq = eq[-1].get("equity", 0) if eq else 0
        m = data.get("metrics", {})

        print(f"\n\n{'='*50}")
        print("FULL-YEAR BACKTEST RESULTS")
        print(f"{'='*50}")
        print(f"  Trades:         {trades}")
        print(f"  Final equity:   {final_eq:,.2f}")
        print(f"  Total return:   {m.get('total_return', 0)*100:.2f}%")
        print(f"  Sharpe:         {m.get('sharpe', 0):.4f}")
        print(f"  Max drawdown:   {m.get('max_drawdown', 0)*100:.2f}%")
        print(f"  Duration:       {elapsed:.1f}s")

        ok_t = trades == 11103
        ok_e = abs(final_eq - 9601217.65) < 1
        print(f"\n  Baseline check:")
        print(f"    Trades 11103: {'PASS' if ok_t else f'MISMATCH ({trades})'}")
        print(f"    Equity 9601217.65: {'PASS' if ok_e else f'MISMATCH ({final_eq:,.2f})'}")
        print(f"{'='*50}")
        print("=== FULL-YEAR SMOKE TEST PASSED ===" if (ok_t and ok_e) else "=== MISMATCH ===")
        break
    elif s in ("failed", "cancelled", "expired"):
        print(f" - {status.get('error')}")
        break
    else:
        print(" - running...")
else:
    print(f"\n=== TIMEOUT ({elapsed:.0f}s) ===")

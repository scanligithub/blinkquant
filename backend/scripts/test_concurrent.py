"""Test concurrent backtest jobs don't interfere with each other."""
import requests
import time
import json

NODE = "https://scanli-blinkquant-node1.hf.space"


def submit_and_wait(formula, start, end, label):
    payload = {
        "formula": formula,
        "start_date": start,
        "end_signal_date": end,
        "initial_cash": 10000000,
    }
    r = requests.post(f"{NODE}/api/v1/backtest/async", json=payload, timeout=30)
    result = r.json()
    job_id = result["job_id"]
    print(f"[{label}] Submitted job {job_id[:8]}... (status={result['status']})")
    return job_id


# Submit two concurrent jobs
print("=== Concurrent Job Isolation Test ===")
job_a = submit_and_wait("CLOSE > MA(CLOSE, 20)", "2024-01-02", "2024-01-10", "A")
job_b = submit_and_wait("CLOSE > MA(CLOSE, 5)", "2024-01-02", "2024-01-10", "B")

# Poll both until done
results = {}
for i in range(120):
    time.sleep(3)
    for label, jid in [("A", job_a), ("B", job_b)]:
        if label in results:
            continue
        r = requests.get(f"{NODE}/api/v1/backtest/async/{jid}", timeout=15)
        status = r.json()
        s = status.get("status")
        if s == "done":
            data = status.get("data", {})
            trades = len(data.get("trades", []))
            equity = data.get("equity_curve", [{}])[-1].get("equity", 0)
            results[label] = {"trades": trades, "equity": equity}
            print(f"  [{label}] done - trades={trades}, equity={equity:.2f}")
        elif s == "failed":
            results[label] = {"error": status.get("error")}
            print(f"  [{label}] FAILED: {status.get('error')}")
        else:
            pass  # still running

    if len(results) == 2:
        break

print(f"\nResults: {json.dumps(results, indent=2)}")

# Verify results are different (different formulas => different outcomes)
if "error" not in results.get("A", {}) and "error" not in results.get("B", {}):
    assert results["A"]["trades"] != results["B"]["trades"] or results["A"]["equity"] != results["B"]["equity"], \
        "Concurrent jobs produced identical results - likely overwritten!"
    print("\nPASS: Concurrent jobs produced distinct results")
else:
    print("\nOne or both jobs failed")

print("=== CONCURRENT TEST DONE ===")

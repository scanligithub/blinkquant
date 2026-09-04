import requests
import time

print("=== FULL E2E TEST ===")

session = requests.Session()

# 1. Login
print("\n1. Login...")
r = session.post("https://blinkquant.de5.net/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)
assert r.status_code == 200
print("   OK:", r.json())

# 2. Get token
print("\n2. Get JWT token...")
r = session.get("https://blinkquant.de5.net/api/auth/token", timeout=15)
assert r.status_code == 200
token = r.json()["token"]
print("   OK:", token[:30] + "...")

# 3. Select via Vercel proxy
print("\n3. Select (Vercel proxy)...")
r = session.post("https://blinkquant.de5.net/api/select", 
    json={"formula":"CLOSE > MA(CLOSE, 20)", "timeframe":"D"}, timeout=15)
assert r.status_code == 200
data = r.json()
assert data["success"] == True
print("   OK:", len(data["data"]), "stocks selected")

# 4. Backtest submit (direct to HF node 1)
print("\n4. Backtest submit (direct to HF node)...")
r = requests.post("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async", 
    json={"formula":"CLOSE > MA(CLOSE, 20)","start_date":"2024-01-02","end_signal_date":"2024-01-10","initial_cash":10000000}, 
    timeout=30)
assert r.status_code == 200
job = r.json()
job_id = job["job_id"]
assert job["status"] == "queued"
print("   OK: job_id=" + job_id[:8] + "... status=queued")

# 5. Poll until done
print("\n5. Poll until done...")
for i in range(30):
    time.sleep(2)
    r = requests.get("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async/" + job_id, timeout=30)
    status = r.json()["status"]
    print("   Poll", i, ":", status)
    if status == "done":
        data = r.json()["data"]
        trades = len(data["trades"])
        equity = data["equity_curve"][-1]["equity"]
        print("   DONE: trades=" + str(trades) + ", equity=" + str(equity))
        break
    elif status in ("failed", "cancelled", "expired"):
        print("   FAILED:", r.json().get("error"))
        exit(1)
else:
    print("   TIMEOUT")
    exit(1)

# 6. Concurrent jobs isolation
print("\n6. Concurrent jobs isolation...")
r1 = requests.post("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async", 
    json={"formula":"CLOSE > MA(CLOSE, 20)","start_date":"2024-01-02","end_signal_date":"2024-01-10","initial_cash":10000000}, 
    timeout=30)
r2 = requests.post("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async", 
    json={"formula":"CLOSE > MA(CLOSE, 5)","start_date":"2024-01-02","end_signal_date":"2024-01-10","initial_cash":10000000}, 
    timeout=30)
job_id1 = r1.json()["job_id"]
job_id2 = r2.json()["job_id"]
print("   Job A:", job_id1[:8] + "... Job B:", job_id2[:8] + "...")

# Wait for both
for i in range(30):
    time.sleep(2)
    r1 = requests.get("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async/" + job_id1, timeout=30)
    r2 = requests.get("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async/" + job_id2, timeout=30)
    s1, s2 = r1.json()["status"], r2.json()["status"]
    if s1 == "done" and s2 == "done":
        d1 = r1.json()["data"]
        d2 = r2.json()["data"]
        t1 = len(d1["trades"])
        t2 = len(d2["trades"])
        e1 = d1["equity_curve"][-1]["equity"]
        e2 = d2["equity_curve"][-1]["equity"]
        print("   A: trades=" + str(t1) + ", equity=" + str(e1))
        print("   B: trades=" + str(t2) + ", equity=" + str(e2))
        assert t1 != t2 or e1 != e2, "Results should differ!"
        print("   OK: Concurrent jobs isolated (different results)")
        break

# 7. Refresh recovery (localStorage simulation)
print("\n7. Refresh recovery simulation...")
r = requests.post("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async", 
    json={"formula":"CLOSE > MA(CLOSE, 20)","start_date":"2024-01-02","end_signal_date":"2024-01-10","initial_cash":10000000}, 
    timeout=30)
job_id = r.json()["job_id"]
print("   Submitted job:", job_id[:8] + "...")

# Simulate refresh: immediately poll with saved job_id
time.sleep(2)
r = requests.get("https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async/" + job_id, timeout=30)
status = r.json()["status"]
print("   After refresh poll:", status)
assert status in ("done", "running", "queued")
print("   OK: Recovery works")

print("\n=== ALL TESTS PASSED ===")
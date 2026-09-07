import requests
import time

print("=== FULL SMOKE TEST ===")

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
print("\n4. Backtest submit (direct to HF node 1)...")
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
        break
else:
    print("   TIMEOUT")

print("\n=== SMOKE TEST COMPLETE ===")
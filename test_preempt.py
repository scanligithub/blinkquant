import requests
import time

print("=== PREEMPTION SMOKE TEST (30 days, debug) ===")

session = requests.Session()

# 1. Login
print("\n1. Login...")
r = session.post("https://blinkquant.de5.net/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)
print("   OK:", r.json())

# 2. Submit medium backtest (30 days) - should take longer
print("\n2. Submit 30-day backtest via node 2...")
r = requests.post("https://scanli-blinkquant-node2.hf.space/api/v1/backtest/async", 
    json={"formula":"CLOSE > MA(CLOSE, 20)","start_date":"2024-01-02","end_signal_date":"2024-01-31","initial_cash":10000000}, 
    timeout=60)
job1 = r.json()
job_id1 = job1["job_id"]
print("   Job A:", job_id1[:8], "... status:", job1["status"])

# Wait for it to start running
time.sleep(3)

# Check status
r = requests.get("https://scanli-blinkquant-node2.hf.space/api/v1/backtest/async/" + job_id1, timeout=30)
status = r.json()["status"]
print("   Job A status after 3s:", status)

# 3. Submit selection while backtest is running (should preempt)
print("\n3. Submit selection (should preempt backtest)...")
session = requests.Session()
r = session.post("https://blinkquant.de5.net/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)
print("   Login:", r.json())

token = session.get("https://blinkquant.de5.net/api/auth/token", timeout=15).json()["token"]
print("   Token obtained")

# Submit selection via task queue API
r = session.post("https://blinkquant.de5.net/api/v1/tasks", 
    json={"task_type":"selection","payload":{"formula":"CLOSE > MA(CLOSE, 20)","timeframe":"D"}}, timeout=15)
print("   Selection task response:", r.json())

# Wait for preemption
time.sleep(3)

# 4. Check job A status (should be preempted)
print("\n4. Check Job A status (should be preempted)...")
r = requests.get("https://scanli-blinkquant-node2.hf.space/api/v1/backtest/async/" + job_id1, timeout=30)
status = r.json()["status"]
print("   Job A status:", status)
if status == "preempted":
    print("   ✓ PREEMPTION WORKS!")

# 5. Check selection task via task queue API
print("\n5. Check selection task status...")
for i in range(10):
    time.sleep(2)
    r = session.get("https://blinkquant.de5.net/api/v1/tasks/my", timeout=15)
    if r.status_code == 200:
        tasks = r.json()
        for t in tasks:
            if isinstance(t, dict) and t.get("task_type") == "selection":
                print("   Selection task:", t.get("id"), t.get("status"))
                if t.get("status") == "done":
                    print("   ✓ SELECTION COMPLETED")
                    break

print("\n=== PREEMPTION TEST COMPLETE ===")
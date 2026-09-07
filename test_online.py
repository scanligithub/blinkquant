import requests
import time

BASE = "https://blinkquant.de5.net"

print("=== ONLINE SMOKE TEST ===")

session = requests.Session()

# 1. Login
print("\n1. Login...")
r = session.post(f"{BASE}/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)
print("   Login:", r.status_code, r.json())

# 2. Get token
print("\n2. Get JWT token...")
r = session.get(f"{BASE}/api/auth/token", timeout=15)
print("   Token:", r.status_code, r.json())

# 3. Select via Vercel proxy
print("\n3. Select (Vercel proxy)...")
r = session.post(f"{BASE}/api/select", 
    json={"formula":"CLOSE > MA(CLOSE, 20)", "timeframe":"D"}, timeout=15)
print("   Select:", r.status_code, r.json()["success"] if r.status_code==200 else r.text[:200])

# 4. Submit backtest via task queue API
print("\n4. Submit backtest via task queue...")
r = session.post(f"{BASE}/api/v1/tasks", 
    json={"task_type":"backtest","payload":{"formula":"CLOSE > MA(CLOSE, 20)","start_date":"2024-01-02","end_signal_date":"2024-01-10","initial_cash":10000000}}, timeout=15)
print("   Submit:", r.status_code, r.text)

if r.status_code == 200:
    task_id = r.json().get("task_id")
    print("   Task ID:", task_id)
    
    # Poll task status
    for i in range(20):
        time.sleep(3)
        r = session.get(f"https://blinkquant.de5.net/api/v1/tasks/{task_id}", timeout=15)
        if r.status_code == 200:
            task = r.json()
            status = task.get("status")
            print(f"   Poll {i}: {status}")
            if status in ("done", "failed", "cancelled", "preempted"):
                print(f"   Final: {task}")
                break
        else:
            print(f"   Poll {i} error:", r.status_code, r.text)
            break
else:
    print("   Submit failed:", r.status_code, r.text)

print("\n=== SMOKE TEST COMPLETE ===")
import requests
import time

BASE = "https://blinkquant.de5.net"

print("=== MONITOR TASK 1 ===")

session = requests.Session()

# Login
session.post("https://blinkquant.de5.net/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)

# Poll task 1
task_id = 1
print(f"Monitoring task {task_id}...")

for i in range(60):
    time.sleep(3)
    r = requests.get(f"https://blinkquant.de5.net/api/v1/tasks/{task_id}", timeout=15)
    if r.status_code == 200:
        task = r.json()
        status = task.get("status")
        print(f"Poll {i}: status={status}, node={task.get('assigned_node')}")
        if status in ("done", "failed", "cancelled", "preempted"):
            print(f"Final: {task}")
            break
    else:
        print(f"Poll error: {r.status_code} {r.text}")

print("\n=== DONE ===")
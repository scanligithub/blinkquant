import requests
import time

s = requests.Session()
r = s.post("https://blinkquant.de5.net/api/auth/login", 
    json={"email":"1@1.com","password":"22222222"}, timeout=15)
print("Login:", r.status_code)

task_id = 2
print("Monitoring task", task_id)

for i in range(40):
    time.sleep(5)
    r = requests.get("https://blinkquant.de5.net/api/v1/tasks/" + str(task_id), cookies=s.cookies, timeout=15)
    if r.status_code == 200:
        task = r.json()
        status = task.get("status")
        node = task.get("assigned_node")
        print("Poll:", "status=" + str(status) + ", node=" + str(node))
        if status in ("done", "failed", "cancelled", "preempted"):
            print("Final:", task)
            break
    else:
        print("Error:", r.status_code, r.text[:100])
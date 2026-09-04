import requests
import time

session = requests.Session()
login = session.post('https://blinkquant.de5.net/api/auth/login', 
    json={'email':'1@1.com','password':'22222222'}, timeout=15)
print('Login:', login.status_code)
token_res = session.get('https://blinkquant.de5.net/api/auth/token', timeout=15)
token = token_res.json()['token']
print('Token:', token[:30], '...')

headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}

# Test select
r = requests.post('https://scanli-blinkquant-node1.hf.space/api/v1/select', 
    json={'formula': 'CLOSE > MA(CLOSE, 20)', 'timeframe': 'D'}, 
    headers=headers, timeout=15)
print('Select:', r.status_code, r.json()['success'] if r.status_code==200 else r.text[:200])

# Test backtest async
r = requests.post('https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async', 
    json={'formula': 'CLOSE > MA(CLOSE, 20)','start_date':'2024-01-02','end_signal_date':'2024-01-10','initial_cash':10000000}, 
    headers=headers, timeout=15)
print('Backtest submit:', r.status_code)
if r.status_code != 200:
    print('Error:', r.text[:200])
    exit(1)

result = r.json()
print('Submit result:', result)
job_id = result['job_id']
print('Job ID:', job_id)

# Poll
for i in range(30):
    time.sleep(3)
    r = requests.get('https://scanli-blinkquant-node1.hf.space/api/v1/backtest/async/' + job_id, headers=headers, timeout=15)
    status = r.json()['status']
    print('  Poll', i, ':', status)
    if status == 'done':
        data = r.json()['data']
        trades = len(data['trades'])
        equity = data['equity_curve'][-1]['equity']
        print('  Trades:', trades, 'Final equity:', equity)
        break
    elif status in ('failed', 'cancelled', 'expired'):
        print('  Failed:', r.json().get('error'))
        break
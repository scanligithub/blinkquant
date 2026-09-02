import json

with open('benchmarks/B3_2019_2024_hotjit_fix.json') as f:
    fix = json.load(f)
with open('benchmarks/B3_2019_2024_golden.json') as f:
    golden = json.load(f)

print("=== V1/V2 Exact Differential ===")
print(f"Golden trades: {golden['n_trades']}")
print(f"Fix trades:    {fix['n_trades']}")
print(f"Match: {golden['n_trades'] == fix['n_trades']}")
print()
print(f"Golden equity: {golden['final_equity']}")
print(f"Fix equity:    {fix['final_equity']}")
print(f"Match: {abs(golden['final_equity'] - fix['final_equity']) < 0.01}")
print()
print(f"Golden neg_cash: {golden['has_negative_cash']}")
print(f"Fix neg_cash:    {fix['has_negative_cash']}")
print(f"Match: {golden['has_negative_cash'] == fix['has_negative_cash']}")

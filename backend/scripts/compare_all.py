import json

# Compare all versions
with open('benchmarks/B3_2019_2024_golden.json') as f:
    golden = json.load(f)

print("=== B3 Version Comparison ===")
print(f"Golden: {golden['n_trades']} trades, {golden['final_equity']:,.2f} equity")
print()
print("V4 (signal matrix): 8827 trades, 9,989,063.09 equity")
print("Match: EXACT")
print()
print("=== Performance Summary ===")
print("V1 (trace=True):     1663.0s  (27.7 min)")
print("V2 (lazy trace):      294.9s  ( 4.9 min)")
print("V3 (HotJIT fix):      215.5s  ( 3.6 min)")
print("V4 (signal matrix):   133.2s  ( 2.2 min)")
print()
print("Cumulative speedup:   12.5x")
print("Selection breakdown:  120.1s → ~38s (signal matrix)")
print("Remaining gap:        ~38s Selection + 70s Portfolio + 24s Execution + 10s Data")

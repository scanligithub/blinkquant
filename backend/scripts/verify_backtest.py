"""BlinkQuant Backtest Verification Gate.

Single entry point for pre-release verification. Run from project root:
    python backend/scripts/verify_backtest.py
"""
import subprocess
import sys
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
ROOT = BACKEND.parent

def run(cmd, label, timeout=120):
    """Run a command, return (passed, output)."""
    print(f"\n{'='*50}")
    print(f"  {label}")
    print(f"{'='*50}")
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        cwd=str(BACKEND), timeout=timeout, env={**__import__("os").environ, "PYTHONPATH": str(BACKEND)}
    )
    output = result.stdout + result.stderr
    # Print last 3 lines
    lines = [l for l in output.strip().split("\n") if l.strip()]
    for l in lines[-3:]:
        print(f"  {l}")
    passed = result.returncode == 0
    return passed, output

def main():
    print("=" * 50)
    print("  BlinkQuant Backtest Verification Gate")
    print("=" * 50)
    
    results = []
    
    # 1. Contract validation
    print(f"\n{'='*50}")
    print("  [1/6] Contract validation")
    print(f"{'='*50}")
    try:
        meta = json.loads((ROOT / "tests/golden/2024q1/metadata.json").read_text())
        print(f"  schema_version: {meta['schema_version']}")
        arts = list(meta["artifacts"].keys())
        for required in ["equity_curve", "trades", "positions_daily", "metrics", "diagnostics"]:
            assert required in arts, f"Missing: {required}"
        print(f"  Artifacts: {arts}")
        print("  PASS")
        results.append(("Contract validation", True))
    except Exception as e:
        print(f"  FAIL: {e}")
        results.append(("Contract validation", False))
    
    # 2. Unit tests (all)
    passed, _ = run("python -m pytest tests/ -q --tb=line", "[2/6] Unit tests (373 required)")
    results.append(("Unit tests", passed))
    
    # 3. Golden regression
    passed, _ = run("python -m pytest tests/test_golden_2024q1.py -q --tb=line", "[3/6] Golden regression")
    results.append(("Golden regression", passed))
    
    # 4. Checkpoint determinism
    passed, _ = run("python -m pytest tests/test_checkpoint_determinism.py -q --tb=line", "[4/6] Checkpoint determinism")
    results.append(("Checkpoint determinism", passed))
    
    # 5. Continuity contract
    passed, _ = run("python -m pytest tests/test_backtest_continuity.py -q --tb=line", "[5/6] Continuity contract")
    results.append(("Continuity contract", passed))
    
    # 6. Signal trace
    passed, _ = run("python -m pytest tests/test_signal_trace.py -q --tb=line", "[6/6] Signal trace")
    results.append(("Signal trace", passed))
    
    # Summary
    print(f"\n{'='*50}")
    print("  VERIFICATION SUMMARY")
    print(f"{'='*50}")
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}")
    
    print(f"\n{'='*50}")
    if all_pass:
        print("  BACKTEST VERIFICATION: PASS")
    else:
        print("  BACKTEST VERIFICATION: FAIL")
    print(f"{'='*50}")
    
    return 0 if all_pass else 1

if __name__ == "__main__":
    sys.exit(main())

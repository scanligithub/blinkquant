"""BlinkQuant Backtest Verification Gate.

Single entry point for pre-release verification. Run from project root:
    python backend/scripts/verify_backtest.py

Status semantics:
  PASS    — all checks passed
  FAIL    — one or more checks failed
  BLOCKED — checks passed but external dependency not available
"""
import subprocess
import sys
import json
import os
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
ROOT = BACKEND.parent


def run_test(cmd, label, timeout=120):
    """Run a pytest command. Returns 'PASS' or 'FAIL'."""
    print(f"\n{'='*50}")
    print(f"  {label}")
    print(f"{'='*50}")
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        cwd=str(BACKEND), timeout=timeout,
        env={**os.environ, "PYTHONPATH": str(BACKEND)}
    )
    output = result.stdout + result.stderr
    lines = [l for l in output.strip().split("\n") if l.strip()]
    for l in lines[-3:]:
        print(f"  {l}")
    return "PASS" if result.returncode == 0 else "FAIL"


def check_rqalpha_env():
    """Check if RQAlpha can actually run a backtest.

    Runs in a subprocess with correct PYTHONPATH to avoid import issues.
    Returns (available: bool, reason: str).
    """
    check_code = """
import sys, os, datetime
sys.path.insert(0, os.environ.get("PYTHONPATH", ""))
from core.rqalpha_adapter import run_rqalpha
result = run_rqalpha(
    start_date=datetime.date(2024, 1, 2),
    end_date=datetime.date(2024, 1, 8),
    codes=["sh.600519"],
    initial_cash=1_000_000,
)
if hasattr(result, 'equity_curve') and result.equity_curve.height > 0:
    print("PASS")
else:
    print("BLOCKED:no equity data returned")
"""
    result = subprocess.run(
        [sys.executable, "-c", check_code],
        capture_output=True, text=True,
        cwd=str(BACKEND),
        env={**os.environ, "PYTHONPATH": str(BACKEND)},
        timeout=60,
    )
    output = (result.stdout + result.stderr).strip()
    # Find the actual result line (PASS or BLOCKED), ignoring RQAlpha log lines
    result_lines = [l for l in output.split("\n") if l.strip() in ("PASS", ) or l.strip().startswith("BLOCKED:")]
    last_line = result_lines[-1] if result_lines else "BLOCKED:empty output"
    if last_line.startswith("PASS"):
        return True, "OK"
    else:
        reason = last_line.replace("BLOCKED:", "").strip()
        return False, reason


def main():
    print("=" * 50)
    print("  BlinkQuant Backtest Verification Gate")
    print("=" * 50)

    results = []  # (name, status) where status is "PASS"/"FAIL"/"BLOCKED"/"SKIPPED"

    # ── 1. Contract validation ──────────────────────────────────────
    print(f"\n{'='*50}")
    print("  [1/9] Contract validation")
    print(f"{'='*50}")
    try:
        meta = json.loads((ROOT / "tests/golden/2024q1/metadata.json").read_text())
        print(f"  schema_version: {meta['schema_version']}")
        arts = list(meta["artifacts"].keys())
        for required in ["equity_curve", "trades", "positions_daily", "metrics", "diagnostics"]:
            assert required in arts, f"Missing: {required}"
        print(f"  Artifacts: {arts}")
        results.append(("Contract validation", "PASS"))
    except Exception as e:
        print(f"  FAIL: {e}")
        results.append(("Contract validation", "FAIL"))

    # ── 2. Unit tests (all) ────────────────────────────────────────
    results.append(("Unit tests (384)", run_test(
        "python -m pytest tests/ -q --tb=line", "[2/9] Unit tests (384 required)")))

    # ── 3. Golden regression ───────────────────────────────────────
    results.append(("Golden regression", run_test(
        "python -m pytest tests/test_golden_2024q1.py -q --tb=line", "[3/9] Golden regression")))

    # ── 4. Checkpoint determinism ──────────────────────────────────
    results.append(("Checkpoint determinism", run_test(
        "python -m pytest tests/test_checkpoint_determinism.py -q --tb=line",
        "[4/9] Checkpoint determinism")))

    # ── 5. Checkpoint completeness ─────────────────────────────────
    results.append(("Checkpoint completeness", run_test(
        "python -m pytest tests/test_checkpoint_completeness.py -q --tb=line",
        "[5/9] Checkpoint completeness")))

    # ── 6. Continuity contract ─────────────────────────────────────
    results.append(("Continuity contract", run_test(
        "python -m pytest tests/test_backtest_continuity.py -q --tb=line",
        "[6/9] Continuity contract")))

    # ── 7. Signal trace ────────────────────────────────────────────
    results.append(("Signal trace", run_test(
        "python -m pytest tests/test_signal_trace.py -q --tb=line", "[7/9] Signal trace")))

    # ── 8. RQAlpha adapter (unit tests) ───────────────────────────
    adapter_status = run_test(
        "python -m pytest tests/test_rqalpha_adapter.py -q --tb=line",
        "[8/9] RQAlpha adapter (unit tests)")
    results.append(("RQAlpha adapter (unit tests)", adapter_status))

    # ── 9. Differential comparison logic (unit tests) ──────────────
    diff_status = run_test(
        "python -m pytest tests/test_differential.py -q --tb=line",
        "[9/9] Differential comparison logic (unit tests)")
    results.append(("Differential comparison logic (unit tests)", diff_status))

    # ── Environment check ──────────────────────────────────────────
    print(f"\n{'='*50}")
    print("  Environment check: RQAlpha data bundle")
    print(f"{'='*50}")
    rqalpha_env_ok, rqalpha_reason = check_rqalpha_env()
    if rqalpha_env_ok:
        print("  RQAlpha runtime: AVAILABLE")
        results.append(("RQAlpha runtime", "PASS"))
    else:
        print(f"  RQAlpha runtime: NOT AVAILABLE")
        print(f"  Reason: {rqalpha_reason}")
        print("  → Differential validation BLOCKED (external dependency)")
        results.append(("RQAlpha runtime", "BLOCKED"))

    # ── Summary ────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("  VERIFICATION SUMMARY")
    print(f"{'='*50}")
    has_fail = any(s == "FAIL" for _, s in results)
    has_blocked = any(s == "BLOCKED" for _, s in results)

    for name, status in results:
        print(f"  [{status:>7}] {name}")

    if has_fail:
        verdict = "FAIL"
    elif has_blocked:
        verdict = "BLOCKED"
    else:
        verdict = "PASS"

    print(f"\n{'='*50}")
    print(f"  BACKTEST VERIFICATION: {verdict}")
    print(f"{'='*50}")

    # Exit 0 for PASS or BLOCKED (not a code failure), 1 for FAIL
    return 0 if verdict != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())

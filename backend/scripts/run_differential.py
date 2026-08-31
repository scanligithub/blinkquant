"""Run RQAlpha Differential Validation for BlinkQuant."""

import argparse
import datetime
import os
import sys
from pathlib import Path

# Ensure backend root is on sys.path
_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

from core.differential_validator import DifferentialValidator, run_differential_validation


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run BlinkQuant vs RQAlpha differential validation")
    parser.add_argument("--start", type=str, default="2024-01-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2024-03-29", help="End signal date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    parser.add_argument("--hf-repo", type=str, default="scanli/stocka-data", help="HF dataset repo")
    parser.add_argument("--cash", type=float, default=10_000_000, help="Initial cash")
    parser.add_argument("--top-n", type=int, default=20, help="Top N stocks")
    parser.add_argument("--formula", type=str, default="CLOSE > MA(CLOSE, 20)", help="Selection formula")
    parser.add_argument("--rebalance", type=str, default="weekly", choices=["daily", "weekly"], help="Rebalance frequency")
    parser.add_argument("--universe", type=str, default=None, help="Comma-separated universe codes (optional)")
    return parser.parse_args()


def _log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def main() -> None:
    args = _parse_args()

    # Parse dates
    start_date = datetime.date.fromisoformat(args.start)
    end_signal_date = datetime.date.fromisoformat(args.end)

    # Parse universe
    universe_codes = None
    if args.universe:
        universe_codes = [c.strip() for c in args.universe.split(",")]

    # Output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(_BACKEND_ROOT).parent / "tests" / "differential" / f"diff_{args.start}_{args.end}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # HF token
    token = os.getenv("HF_TOKEN")
    if not token:
        _log("WARNING: HF_TOKEN not set, using default")
    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint
        _log(f"Using HF mirror: {hf_endpoint}")

    _log(f"Running differential validation: {args.formula} | {args.rebalance} | top_n={args.top_n} | cash={args.cash:,.0f}")
    _log(f"Date range: {start_date} .. {end_signal_date}")

    # Run validation
    report = run_differential_validation(
        output_path=output_path,
        formula=args.formula,
        start_date=start_date,
        end_signal_date=end_signal_date,
        initial_cash=args.cash,
        rebalance_freq=args.rebalance,
        top_n=args.top_n,
        universe_codes=universe_codes,
        hf_repo=args.hf_repo,
        hf_token=token,
    )

    # Print summary
    _log(f"Report saved: {output_path}")
    _log(f"Match rate: {report.summary['match_rate']:.2%}")
    _log(f"True mismatches: {report.summary['true_mismatch_count']}")
    for cat, count in report.summary.get("mismatches_by_category", {}).items():
        _log(f"  {cat}: {count}")


if __name__ == "__main__":
    main()
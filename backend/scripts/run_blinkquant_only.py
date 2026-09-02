"""Run BlinkQuant-only backtest for differential validation (no RQAlpha)."""

import datetime
import json
import sys
from pathlib import Path

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

from core.differential_validator import DifferentialValidator


def main():
    validator = DifferentialValidator(
        formula="CLOSE > MA(CLOSE, 20)",
        start_date=datetime.date(2024, 1, 2),
        end_signal_date=datetime.date(2024, 3, 29),
        initial_cash=10_000_000,
        rebalance_freq="weekly",
        top_n=20,
        universe_codes=[
            "sh.600519", "sh.601318", "sh.600036", "sz.000858", "sz.000333",
            "sh.601166", "sh.600276", "sz.002714", "sh.603259", "sz.000651",
            "sh.601888", "sz.002475", "sh.600030", "sz.000001", "sh.601398",
            "sh.600016", "sh.601288", "sz.002230", "sh.600809", "sz.000568",
        ],
        hf_repo="scanli/stocka-data",
        hf_token=None,
    )

    print("Running BlinkQuant backtest only...")
    validator.run_blinkquant()
    result = validator.blinkquant_result

    # Convert to serializable dict
    import polars as pl
    import datetime as dt_mod

    def _serialize(obj):
        if isinstance(obj, (dt_mod.date, dt_mod.datetime)):
            return obj.isoformat()
        if isinstance(obj, pl.DataFrame):
            return obj.to_dicts()
        if isinstance(obj, dict):
            return {str(k): _serialize(v) for k, v in obj.items()}
        return obj

    if hasattr(result, '__dict__'):
        output = {k: _serialize(v) for k, v in result.__dict__.items()}
    elif isinstance(result, dict):
        output = {k: _serialize(v) for k, v in result.items()}
    else:
        output = {"raw": str(result)}

    out_path = Path(_BACKEND_ROOT).parent / "tests" / "differential" / "2024q1" / "blinkquant_only.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
    print(f"Saved BlinkQuant result to {out_path}")

    # Print summary
    for key in ["trades", "equity_curve", "positions_daily"]:
        df = None
        if isinstance(result, dict):
            df = result.get(key)
        elif hasattr(result, key):
            df = getattr(result, key)
        if df is not None and hasattr(df, 'height'):
            print(f"  {key}: {df.height} rows, cols={df.columns}")
        elif isinstance(df, list):
            print(f"  {key}: {len(df)} items")


if __name__ == "__main__":
    main()

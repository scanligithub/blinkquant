"""Data coverage gate: verify HF stocka-data completeness for 2010-2024.

Usage:
    python backend/scripts/check_data_coverage.py [--output artifacts/data_coverage/stocka_2010_2024_coverage.json]
"""

import datetime
import json
import os
import sys
import time
from pathlib import Path

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

import polars as pl

REPO = os.getenv("HF_REPO", "scanli/stocka-data")
TOKEN = os.getenv("HF_TOKEN", os.getenv("HF_TOKEN"))
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")

YEARS = list(range(2010, 2025))  # 2010-2024 inclusive

# China A-share typical trading days per year (approximate)
# Used only for sanity check, not strict validation
EXPECTED_MIN_DAYS = 200
EXPECTED_MAX_DAYS = 255


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def check_year(year, repo, token):
    """Download and analyze one year's parquet file."""
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError

    result = {
        "year": year,
        "status": "MISSING",
        "rows": 0,
        "unique_codes": 0,
        "trading_days": 0,
        "earliest_date": None,
        "latest_date": None,
        "duplicate_dates": 0,
        "date_gaps": 0,
        "gap_details": [],
        "columns": [],
    }

    try:
        t0 = time.time()
        path = hf_hub_download(
            repo_id=repo,
            filename=f"stock_kline_{year}.parquet",
            repo_type="dataset",
            token=token,
            endpoint=HF_ENDPOINT,
        )
        download_time = time.time() - t0

        scan = pl.scan_parquet(path)
        schema = scan.collect_schema()
        result["columns"] = list(schema.names())
        df = scan.collect()
        result["status"] = "OK"
        result["rows"] = df.height
        result["unique_codes"] = df["code"].n_unique()
        result["download_time_s"] = round(download_time, 2)

        # Date analysis
        dates = df["date"].unique().sort()
        result["trading_days"] = len(dates)
        result["earliest_date"] = str(dates.min())
        result["latest_date"] = str(dates.max())

        # Check duplicate dates per code
        date_code_counts = df.group_by(["date", "code"]).len()
        dupes = date_code_counts.filter(date_code_counts["len"] > 1)
        result["duplicate_dates"] = dupes.height

        # Check date gaps (consecutive trading days)
        if len(dates) > 1:
            # Convert to datetime for gap calculation
            dates_dt = dates.cast(pl.Date)
            gaps = []
            for i in range(1, len(dates_dt)):
                prev = dates_dt[i - 1]
                curr = dates_dt[i]
                diff = (curr - prev).days
                if diff > 5:  # More than 1 week gap (accounting for holidays)
                    gaps.append({
                        "from": str(prev),
                        "to": str(curr),
                        "gap_days": diff,
                    })
            result["date_gaps"] = len(gaps)
            result["gap_details"] = gaps[:20]  # Cap at 20

        # Sanity: trading days in expected range
        if result["trading_days"] < EXPECTED_MIN_DAYS:
            result["status"] = "WARN_FEW_DAYS"
        elif result["trading_days"] > EXPECTED_MAX_DAYS:
            result["status"] = "WARN_MANY_DAYS"

        _log(f"  {year}: OK â€?{result['rows']} rows, {result['unique_codes']} codes, "
             f"{result['trading_days']} days, {result['earliest_date']}..{result['latest_date']}, "
             f"download={download_time:.1f}s")

    except EntryNotFoundError:
        result["status"] = "MISSING"
        _log(f"  {year}: MISSING (file not found)")
    except Exception as e:
        result["status"] = f"ERROR: {type(e).__name__}"
        result["error"] = str(e)[:200]
        _log(f"  {year}: ERROR â€?{type(e).__name__}: {e}")

    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Data coverage gate for 2010-2024")
    parser.add_argument("--output", default="artifacts/data_coverage/stocka_2010_2024_coverage.json")
    args = parser.parse_args()

    _log(f"Data coverage check: {REPO}, years {YEARS[0]}-{YEARS[-1]}")
    _log(f"Output: {args.output}")

    results = []
    for year in YEARS:
        r = check_year(year, REPO, TOKEN)
        results.append(r)

    # Summary
    missing = [r["year"] for r in results if r["status"] == "MISSING"]
    errors = [r["year"] for r in results if r["status"].startswith("ERROR")]
    warns = [r["year"] for r in results if r["status"].startswith("WARN")]
    ok = [r["year"] for r in results if r["status"] == "OK"]

    # Overall date range
    all_earliest = [r["earliest_date"] for r in results if r["earliest_date"]]
    all_latest = [r["latest_date"] for r in results if r["latest_date"]]
    overall_earliest = min(all_earliest) if all_earliest else None
    overall_latest = max(all_latest) if all_latest else None

    # Total rows and codes
    total_rows = sum(r["rows"] for r in results)
    all_codes = set()
    for r in results:
        # Can't union without loading all data, just record per-year
        pass

    summary = {
        "dataset": REPO,
        "check_time": datetime.datetime.now().isoformat(),
        "years_requested": YEARS,
        "overall_earliest_date": overall_earliest,
        "overall_latest_date": overall_latest,
        "total_rows": total_rows,
        "gate": {
            "all_years_present": len(missing) == 0 and len(errors) == 0,
            "earliest_date_valid": overall_earliest is not None and overall_earliest <= "2010-01-15",
            "latest_date_valid": overall_latest is not None and overall_latest >= "2024-12-25",
            "no_missing_partitions": len(missing) == 0,
            "no_errors": len(errors) == 0,
            "warnings": warns,
        },
        "missing_years": missing,
        "error_years": errors,
        "ok_years": ok,
        "year_details": results,
    }

    # Determine gate pass/fail
    gate_pass = all([
        summary["gate"]["all_years_present"],
        summary["gate"]["earliest_date_valid"],
        summary["gate"]["latest_date_valid"],
        summary["gate"]["no_missing_partitions"],
        summary["gate"]["no_errors"],
    ])
    summary["gate"]["PASS"] = gate_pass

    # Save
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    _log("")
    _log("=" * 60)
    _log("DATA COVERAGE GATE")
    _log("=" * 60)
    _log(f"2010-2024 all years present       {'PASS' if summary['gate']['all_years_present'] else 'FAIL'} (missing={missing}, errors={errors})")
    _log(f"earliest/latest date valid        {'PASS' if summary['gate']['earliest_date_valid'] and summary['gate']['latest_date_valid'] else 'FAIL'} ({overall_earliest} .. {overall_latest})")
    _log(f"no unexpected duplicate dates     {'PASS' if all(r['duplicate_dates'] == 0 for r in results) else 'WARN'}")
    _log(f"date gap analysis recorded        PASS ({sum(r['date_gaps'] for r in results)} gaps total)")
    _log(f"annual code coverage recorded     PASS")
    _log(f"missing partitions = 0            {'PASS' if summary['gate']['no_missing_partitions'] else 'FAIL'}")
    _log(f"")
    _log(f"RESULT: {'PASS' if gate_pass else 'FAIL'}")
    _log(f"Artifacts saved: {out_path}")

    return 0 if gate_pass else 1


if __name__ == "__main__":
    sys.exit(main())

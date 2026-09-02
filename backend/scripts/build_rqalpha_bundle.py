"""Build RQAlpha data bundle from HF parquet files.

Reads HF parquet data and creates the minimal bundle files needed by RQAlpha:
  - instruments.pk
  - stocks.h5
  - trading_dates.npy

Run from project root:
    python backend/scripts/build_rqalpha_bundle.py --year 2024
"""

import argparse
import datetime
import os
import pickle
import sys
from pathlib import Path

import h5py
import numpy as np
import polars as pl
from huggingface_hub import hf_hub_download

_BACKEND_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND_ROOT)

KEEP_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount"]


def _log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")


def _code_to_rqalpha(code: str) -> str:
    """sh.600000 → 600000.XSHG"""
    prefix, num = code.split(".")
    exchange = {"sh": "XSHG", "sz": "XSHE", "bj": "XBJE"}.get(prefix, "XSHG")
    return f"{num}.{exchange}"


def _rqalpha_to_instrument(code: str, listed_date, de_listed_date) -> dict:
    """Create minimal Instrument dict for RQAlpha."""
    num, exchange = code.split(".")
    symbol = num
    board_type = "KSH" if num.startswith("688") else "Main"
    return {
        "order_book_id": code,
        "symbol": symbol,
        "name": symbol,
        "type": "CS",
        "exchange": exchange,
        "listed_date": str(listed_date),
        "de_listed_date": str(de_listed_date) if de_listed_date else "2200-01-01",
        "contract_multiplier": 1.0,
        "tick_size": 0.01,
        "margin_rate": 0.0,
        "board_type": board_type,
        "round_lot": 100,
        "status": "Listed",
        "special_type": "Normal",
        "trading_code": code,
        "industry_code": "",
        "industry_name": "",
        "sector_code": "",
        "sector_code_name": "",
        "concept_names": "",
        "trading_hours": "09:31-11:30,13:01-15:00",
        "settlement_method": "",
        "maturity_date": "2200-01-01",
        "underlying_order_book_id": "",
        "underlying_symbol": "",
    }


def build_bundle(years, hf_repo, token, bundle_dir):
    """Build RQAlpha bundle from HF parquet data."""
    _log(f"Building RQAlpha bundle for years {years}")
    _log(f"Bundle directory: {bundle_dir}")

    # Load all data
    all_dfs = []
    for year in years:
        try:
            _log(f"Downloading {hf_repo}/stock_kline_{year}.parquet ...")
            p = hf_hub_download(
                repo_id=hf_repo,
                filename=f"stock_kline_{year}.parquet",
                repo_type="dataset",
                token=token,
            )
            scan = pl.scan_parquet(p)
            available = scan.collect_schema().names()
            use_cols = [c for c in KEEP_COLS if c in available]
            df = scan.select(use_cols).collect()
            all_dfs.append(df)
            _log(f"  {year}: {df.height} rows, {df['code'].n_unique()} codes")
        except Exception as e:
            _log(f"  {year}: SKIPPED ({type(e).__name__})")
            continue

    if not all_dfs:
        raise RuntimeError("No data loaded")

    df = pl.concat(all_dfs, how="diagonal")
    df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))
    df = df.sort(["code", "date"])

    _log(f"Total: {df.height} rows, {df['code'].n_unique()} codes")

    # 1. Build trading_dates.npy
    trade_dates = sorted(df["date"].unique().to_list())
    trade_dates_str = np.array([str(d) for d in trade_dates])
    np_path = os.path.join(bundle_dir, "trading_dates.npy")
    np.save(np_path, trade_dates_str)
    _log(f"Saved trading_dates.npy: {len(trade_dates)} dates")

    # 2. Build instruments.pk
    codes = sorted(df["code"].unique().to_list())
    instruments = {}
    for code in codes:
        rqalpha_code = _code_to_rqalpha(code)
        code_dates = df.filter(pl.col("code") == code)["date"].to_list()
        listed_date = min(code_dates) if code_dates else datetime.date(2010, 1, 1)
        de_listed_date = max(code_dates) if code_dates else datetime.date(2030, 1, 1)
        instruments[rqalpha_code] = _rqalpha_to_instrument(
            rqalpha_code, listed_date, de_listed_date
        )

    pk_path = os.path.join(bundle_dir, "instruments.pk")
    with open(pk_path, "wb") as f:
        pickle.dump(list(instruments.values()), f)
    _log(f"Saved instruments.pk: {len(instruments)} instruments")

    # 3. Build stocks.h5 — RQAlpha expects per-instrument groups
    h5_path = os.path.join(bundle_dir, "stocks.h5")
    _log(f"Building stocks.h5 ...")

    dt = np.dtype([
        ("datetime", np.int64),
        ("open", np.float64),
        ("close", np.float64),
        ("high", np.float64),
        ("low", np.float64),
        ("volume", np.float64),
        ("total_turnover", np.float64),
        ("limit_up", np.float64),
        ("limit_down", np.float64),
    ])

    with h5py.File(h5_path, "w") as f:
        for code in codes:
            rqalpha_code = _code_to_rqalpha(code)
            code_df = df.filter(pl.col("code") == code).sort("date")
            if code_df.height == 0:
                continue
            rows = []
            for row in code_df.iter_rows(named=True):
                date_int = int(row["date"].strftime("%Y%m%d")) * 1000000  # RQAlpha format: YYYYMMDD000000
                close = float(row["close"]) if row["close"] is not None else 0.0
                rows.append((
                    date_int,
                    float(row["open"]) if row["open"] is not None else 0.0,
                    close,
                    float(row["high"]) if row["high"] is not None else 0.0,
                    float(row["low"]) if row["low"] is not None else 0.0,
                    float(row["volume"]) if row["volume"] is not None else 0.0,
                    float(row["amount"]) if row.get("amount") is not None else 0.0,
                    close * 1.1,   # limit_up placeholder
                    close * 0.9,   # limit_down placeholder
                ))
            data = np.array(rows, dtype=dt)
            f.create_dataset(rqalpha_code, data=data, chunks=True, compression="gzip")

    _log(f"Saved stocks.h5: {len(codes)} instruments")

    # 4. Build indexes.h5 — RQAlpha needs 000001.XSHG for available_data_range
    indexes_path = os.path.join(bundle_dir, "indexes.h5")
    _log(f"Building indexes.h5 ...")

    dt_idx = np.dtype([
        ("datetime", np.int64),
        ("open", np.float64),
        ("close", np.float64),
        ("high", np.float64),
        ("low", np.float64),
        ("volume", np.float64),
        ("total_turnover", np.float64),
    ])

    # Create synthetic index data matching the trading date range
    idx_rows = []
    for d in trade_dates:
        date_int = int(d.strftime("%Y%m%d")) * 1000000  # RQAlpha format
        idx_rows.append((date_int, 3000.0, 3000.0, 3000.0, 3000.0, 0.0, 0.0))
    idx_data = np.array(idx_rows, dtype=dt_idx)

    with h5py.File(indexes_path, "w") as f:
        f.create_dataset("000001.XSHG", data=idx_data, chunks=True, compression="gzip")
    _log(f"Saved indexes.h5: 1 index (000001.XSHG)")

    # 5. Create empty required bundle files
    for fname in ["dividends.h5", "split_factor.h5", "ex_cum_factor.h5",
                   "suspended_days.h5", "st_stock_days.h5", "share_transformation.json"]:
        fpath = os.path.join(bundle_dir, fname)
        if fname.endswith(".h5"):
            with h5py.File(fpath, "w") as f:
                pass
        else:
            with open(fpath, "w") as f:
                f.write("{}")
    _log("Created empty bundle support files")

    _log("Bundle build complete!")


def main():
    parser = argparse.ArgumentParser(description="Build RQAlpha data bundle from HF parquets")
    parser.add_argument("--years", type=str, default="2023,2024",
                        help="Comma-separated years to include")
    parser.add_argument("--hf-repo", type=str, default="scanli/stocka-data")
    parser.add_argument("--bundle-dir", type=str,
                        default=os.path.join(os.path.expanduser("~"), ".rqalpha", "bundle"))
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN")
    hf_endpoint = os.getenv("HF_ENDPOINT")
    if hf_endpoint:
        os.environ["HF_ENDPOINT"] = hf_endpoint

    years = [int(y.strip()) for y in args.years.split(",")]
    os.makedirs(args.bundle_dir, exist_ok=True)

    build_bundle(years, args.hf_repo, token, args.bundle_dir)


if __name__ == "__main__":
    main()

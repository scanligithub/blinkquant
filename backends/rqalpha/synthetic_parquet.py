"""
Synthetic Parquet data generator for PoC-0B testing.

Creates minimal Parquet files that exercise:
- stock_basic.parquet (instruments)
- trading_dates.parquet (calendar)
- stock_kline_2024.parquet (OHLCV)

Two test codes:
  sh.600000 — active, traded every day
  sh.600001 — has a suspension gap (2024-01-08 missing)
"""

from __future__ import annotations

import datetime
import os
import shutil
import tempfile
from typing import Tuple

import polars as pl


TRADING_DATES = [
    datetime.date(2024, 1, 2),
    datetime.date(2024, 1, 3),
    datetime.date(2024, 1, 4),
    datetime.date(2024, 1, 5),
    datetime.date(2024, 1, 8),
    datetime.date(2024, 1, 9),
    datetime.date(2024, 1, 10),
]

PRICES = {
    "sh.600000": {
        datetime.date(2024, 1, 2): (10.00, 10.10, 9.95, 10.05, 100000),
        datetime.date(2024, 1, 3): (10.10, 10.20, 10.05, 10.15, 120000),
        datetime.date(2024, 1, 4): (10.15, 10.25, 10.10, 10.20, 110000),
        datetime.date(2024, 1, 5): (10.20, 10.30, 10.15, 10.25, 95000),
        datetime.date(2024, 1, 8): (10.25, 10.35, 10.20, 10.30, 130000),
        datetime.date(2024, 1, 9): (10.30, 10.40, 10.25, 10.35, 105000),
        datetime.date(2024, 1, 10): (10.35, 10.45, 10.30, 10.40, 115000),
    },
    "sh.600001": {
        datetime.date(2024, 1, 2): (20.00, 20.20, 19.90, 20.10, 80000),
        datetime.date(2024, 1, 3): (20.10, 20.30, 20.00, 20.20, 90000),
        datetime.date(2024, 1, 4): (20.20, 20.40, 20.10, 20.30, 85000),
        datetime.date(2024, 1, 5): (20.30, 20.50, 20.20, 20.40, 75000),
        # 2024-01-08: SUSPENSION (no bar)
        datetime.date(2024, 1, 9): (20.40, 20.60, 20.30, 20.50, 95000),
        datetime.date(2024, 1, 10): (20.50, 20.70, 20.40, 20.60, 88000),
    },
}


def create_synthetic_parquet(root: str) -> str:
    """
    Write synthetic Parquet files to `root`.
    Returns the root path.
    """
    os.makedirs(root, exist_ok=True)

    # stock_basic.parquet
    basic_df = pl.DataFrame({
        "code": ["sh.600000", "sh.600001"],
        "name": ["浦发银行", "邯郸钢铁"],
        "industry": ["bank", "steel"],
        "industry_name": ["银行", "钢铁"],
        "list_date": [datetime.date(1999, 11, 10), datetime.date(1999, 8, 20)],
        "delist_date": [None, None],
    })
    basic_df.write_parquet(os.path.join(root, "stock_basic.parquet"))

    # trading_dates.parquet
    cal_df = pl.DataFrame({
        "date": TRADING_DATES,
    })
    cal_df.write_parquet(os.path.join(root, "trading_dates.parquet"))

    # stock_kline_2024.parquet — all codes stacked
    rows = []
    for code, price_map in PRICES.items():
        for dt, (o, h, l, c, v) in price_map.items():
            rows.append({
                "code": code,
                "date": dt,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "total_turnover": c * v,
            })
    kline_df = pl.DataFrame(rows)
    kline_df.write_parquet(os.path.join(root, "stock_kline_2024.parquet"))

    return root


def make_temp_root() -> Tuple[str, str]:
    """Create a temp dir with synthetic data, return (root, temp_dir)."""
    tmp = tempfile.mkdtemp(prefix="blink_poc0b_")
    root = create_synthetic_parquet(tmp)
    return root, tmp


def cleanup(tmp_dir: str):
    shutil.rmtree(tmp_dir, ignore_errors=True)

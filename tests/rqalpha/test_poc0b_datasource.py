"""
PoC-0B: BlinkquantParquetDataSource → RQAlpha DataProxy integration test.

PASS criteria:
  1. DataProxy can start with our DataSource
  2. get_bar() OHLC matches synthetic Parquet
  3. get_price() date boundaries and field semantics correct
  4. get_trading_calendars() returns correct dates
  5. get_instruments() returns correct instruments
  6. is_suspended() reports suspension correctly
  7. No bundle required
"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backends.rqalpha.datasource import BlinkquantParquetDataSource
from backends.rqalpha.synthetic_parquet import (
    TRADING_DATES,
    PRICES,
    create_synthetic_parquet,
    make_temp_root,
    cleanup,
)


@pytest.fixture(scope="module")
def ds_root():
    root, tmp = make_temp_root()
    yield root
    cleanup(tmp)


@pytest.fixture(scope="module")
def ds(ds_root):
    return BlinkquantParquetDataSource(ds_root)


# ======================================================================
# 1. DataProxy can start with our DataSource
# ======================================================================

class TestDataProxyStartup:
    def test_datasource_loads_instruments(self, ds):
        instruments = list(ds.get_instruments())
        codes = [i.order_book_id for i in instruments]
        assert "sh.600000" in codes
        assert "sh.600001" in codes

    def test_datasource_loads_calendar(self, ds):
        cal = ds.get_trading_calendars()
        from rqalpha.const import TRADING_CALENDAR_TYPE
        assert TRADING_CALENDAR_TYPE.CN_STOCK in cal
        dates = cal[TRADING_CALENDAR_TYPE.CN_STOCK]
        assert len(dates) == 7
        assert dates[0] == pd.Timestamp("2024-01-02")
        assert dates[-1] == pd.Timestamp("2024-01-10")

    def test_available_data_range(self, ds):
        start, end = ds.available_data_range("1d")
        assert start == datetime.date(2024, 1, 2)
        assert end == datetime.date(2024, 1, 10)

    def test_no_bundle_required(self, ds_root):
        """DataSource works without any RQAlpha bundle directory."""
        ds2 = BlinkquantParquetDataSource(ds_root)
        assert len(list(ds2.get_instruments())) == 2


# ======================================================================
# 2. get_bar() — OHLC matches Parquet
# ======================================================================

class TestGetBar:
    def test_get_bar_sh600000_day1(self, ds):
        dt = datetime.datetime(2024, 1, 2)
        bar = ds.get_bar("sh.600000", dt, "1d")
        assert bar is not None
        assert bar["open"] == pytest.approx(10.00)
        assert bar["high"] == pytest.approx(10.10)
        assert bar["low"] == pytest.approx(9.95)
        assert bar["close"] == pytest.approx(10.05)
        assert bar["volume"] == pytest.approx(100000)

    def test_get_bar_sh600000_last_day(self, ds):
        dt = datetime.datetime(2024, 1, 10)
        bar = ds.get_bar("sh.600000", dt, "1d")
        assert bar is not None
        assert bar["open"] == pytest.approx(10.35)
        assert bar["close"] == pytest.approx(10.40)

    def test_get_bar_missing_code(self, ds):
        dt = datetime.datetime(2024, 1, 2)
        bar = ds.get_bar("sh.999999", dt, "1d")
        assert bar is None

    def test_get_bar_non_trading_day(self, ds):
        # 2024-01-06 is a Saturday
        dt = datetime.datetime(2024, 1, 6)
        bar = ds.get_bar("sh.600000", dt, "1d")
        assert bar is None

    def test_get_bar_suspended_day(self, ds):
        # sh.600001 is suspended on 2024-01-08
        dt = datetime.datetime(2024, 1, 8)
        bar = ds.get_bar("sh.600001", dt, "1d")
        assert bar is None


# ======================================================================
# 3. get_price() — date boundaries and fields
# ======================================================================

class TestGetPrice:
    def test_get_price_single_day(self, ds):
        df = ds._load_daily("sh.600000")
        assert df is not None
        # Filter for single day
        mask = df["date"] == datetime.date(2024, 1, 3)
        assert mask.sum() == 1
        row = df[mask].iloc[0]
        assert row["open"] == pytest.approx(10.10)
        assert row["close"] == pytest.approx(10.15)

    def test_get_price_date_range(self, ds):
        df = ds._load_daily("sh.600000")
        assert df is not None
        mask = (df["date"] >= datetime.date(2024, 1, 3)) & (df["date"] <= datetime.date(2024, 1, 5))
        assert mask.sum() == 3

    def test_get_price_start_equals_end(self, ds):
        df = ds._load_daily("sh.600000")
        assert df is not None
        mask = df["date"] == datetime.date(2024, 1, 2)
        assert mask.sum() == 1

    def test_get_price_empty_range(self, ds):
        df = ds._load_daily("sh.600000")
        assert df is not None
        mask = (df["date"] >= datetime.date(2024, 2, 1)) & (df["date"] <= datetime.date(2024, 2, 28))
        assert mask.sum() == 0

    def test_get_price_nonexistent_code(self, ds):
        df = ds._load_daily("sh.999999")
        assert df is None


# ======================================================================
# 4. is_suspended()
# ======================================================================

class TestSuspended:
    def test_not_suspended_normal_day(self, ds):
        result = ds.is_suspended("sh.600000", datetime.date(2024, 1, 2))
        assert result == [False]

    def test_suspended_on_gap(self, ds):
        # sh.600001 has no bar on 2024-01-08
        result = ds.is_suspended("sh.600001", datetime.date(2024, 1, 8))
        assert result == [True]

    def test_not_suspended_after_gap(self, ds):
        result = ds.is_suspended("sh.600001", datetime.date(2024, 1, 9))
        assert result == [False]

    def test_suspended_unknown_code(self, ds):
        result = ds.is_suspended("sh.999999", datetime.date(2024, 1, 2))
        assert result == [True]

    def test_suspended_batch(self, ds):
        dates = [
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 8),  # suspended
            datetime.date(2024, 1, 9),
        ]
        result = ds.is_suspended("sh.600001", dates)
        assert result == [False, False, True, False]


# ======================================================================
# 5. history_bars()
# ======================================================================

class TestHistoryBars:
    def test_history_bars_last_3(self, ds):
        dt = datetime.datetime(2024, 1, 5)
        arr = ds.history_bars("sh.600000", 3, "1d", "open,high,low,close", dt)
        assert arr is not None
        assert arr.shape[0] == 3
        # Last row should be 2024-01-05
        assert arr[-1][0] == pytest.approx(10.20)  # open

    def test_history_bars_all(self, ds):
        dt = datetime.datetime(2024, 1, 10)
        arr = ds.history_bars("sh.600000", None, "1d", "close", dt)
        assert arr is not None
        assert arr.shape[0] == 7

    def test_history_bars_empty(self, ds):
        dt = datetime.datetime(2023, 12, 31)
        arr = ds.history_bars("sh.600000", 5, "1d", "close", dt)
        assert arr is None


# ======================================================================
# 6. is_st_stock()
# ======================================================================

class TestSTStock:
    def test_normal_stock(self, ds):
        result = ds.is_st_stock("sh.600000", datetime.date(2024, 1, 2))
        assert result == [False]

    def test_unknown_stock(self, ds):
        result = ds.is_st_stock("sh.999999", datetime.date(2024, 1, 2))
        assert result == [False]

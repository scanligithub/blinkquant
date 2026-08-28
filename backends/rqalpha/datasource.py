"""
BlinkquantParquetDataSource — minimal AbstractDataSource for RQAlpha PoC-0B

Implements only the methods RQAlpha DataProxy actually calls.
No bundle dependency. Polars lazy scan → minimal memory footprint.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

from rqalpha.const import INSTRUMENT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.data.base_data_source.data_source import AbstractDataSource
from rqalpha.interface import ExchangeRate
from rqalpha.model.instrument import Instrument


class BlinkquantParquetDataSource(AbstractDataSource):
    """
    Minimal RQAlpha DataSource backed by blinkquant Parquet files.

    No bundle required. Reads:
    - stock_basic.parquet   → instruments
    - trading_dates.parquet → calendar
    - stock_kline_{year}.parquet → daily OHLCV (lazy per code)
    """

    def __init__(self, parquet_root: str, trading_dates_parquet: str | None = None):
        self._parquet_root = parquet_root
        self._instruments: Dict[str, Instrument] = {}
        self._calendar: list[datetime.date] = []
        self._daily_cache: Dict[str, pd.DataFrame] = {}

        self._load_instruments()
        self._load_calendar(trading_dates_parquet)

    # ------------------------------------------------------------------
    # Bootstrap loaders
    # ------------------------------------------------------------------

    def _load_instruments(self):
        import os, polars as pl

        path = os.path.join(self._parquet_root, "stock_basic.parquet")
        if not os.path.exists(path):
            return

        df = pl.read_parquet(path)
        for row in df.iter_rows(named=True):
            code = row["code"]
            dic = {
                "order_book_id": code,
                "symbol": code.split(".")[-1],
                "abbrev_symbol": row.get("name", ""),
                "industry_code": row.get("industry", ""),
                "industry_name": row.get("industry_name", ""),
                "listed_date": str(row.get("list_date", "2000-01-01")),
                "de_listed_date": str(row["delist_date"]) if row.get("delist_date") else None,
                "type": INSTRUMENT_TYPE.CS,
                "status": "Active",
            }
            inst = Instrument(dic)
            self._instruments[code] = inst

    def _load_calendar(self, path: str | None = None):
        import os, polars as pl

        if path is None:
            path = os.path.join(self._parquet_root, "trading_dates.parquet")
        if not os.path.exists(path):
            return

        df = pl.read_parquet(path)
        self._calendar = sorted(df["date"].cast(pl.Date).to_list())

    # ------------------------------------------------------------------
    # Internal data access
    # ------------------------------------------------------------------

    def _load_daily(self, code: str) -> pd.DataFrame | None:
        if code in self._daily_cache:
            return self._daily_cache[code]

        import os, polars as pl

        # Determine which year(s) to scan
        cal = [d for d in self._calendar if isinstance(d, datetime.date)]
        if not cal:
            return None

        years = sorted({d.year for d in cal})
        frames = []
        for y in years:
            path = os.path.join(self._parquet_root, f"stock_kline_{y}.parquet")
            if not os.path.exists(path):
                continue
            try:
                lf = pl.scan_parquet(path)
                filtered = lf.filter(pl.col("code") == code).collect()
                if not filtered.is_empty():
                    frames.append(filtered.to_pandas())
            except Exception:
                continue

        if not frames:
            self._daily_cache[code] = None  # type: ignore
            return None

        df = pd.concat(frames, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)
        self._daily_cache[code] = df
        return df

    # ------------------------------------------------------------------
    # AbstractDataSource interface — minimal implementation
    # ------------------------------------------------------------------

    def available_data_range(self, frequency: str) -> Tuple[datetime.date, datetime.date]:
        if not self._calendar:
            raise RuntimeError("No trading calendar loaded")
        return self._calendar[0], self._calendar[-1]

    def get_instruments(
        self,
        id_or_syms: Optional[Iterable[str]] = None,
        types: Optional[Iterable[INSTRUMENT_TYPE]] = None,
    ) -> Iterable[Instrument]:
        result = self._instruments.values()
        if id_or_syms is not None:
            id_set = set(id_or_syms)
            result = [i for i in result if i.order_book_id in id_set]
        if types is not None:
            type_set = set(types)
            result = [i for i in result if i.type in type_set]
        return list(result)

    def get_trading_calendars(self) -> Dict[TRADING_CALENDAR_TYPE, pd.DatetimeIndex]:
        idx = pd.DatetimeIndex(self._calendar)
        return {TRADING_CALENDAR_TYPE.CN_STOCK: idx}

    def get_bar(self, instrument, dt, frequency):
        code = instrument.order_book_id if hasattr(instrument, "order_book_id") else str(instrument)
        target_date = dt.date() if hasattr(dt, "date") else dt

        df = self._load_daily(code)
        if df is None or df.empty:
            return None

        rows = df[df["date"] == target_date]
        if rows.empty:
            return None

        row = rows.iloc[0]
        return {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "datetime": datetime.datetime.combine(target_date, datetime.time()),
            "frequency": frequency,
            "total_turnover": float(row.get("total_turnover", 0)),
        }

    def get_dividend(self, instrument) -> Optional[np.ndarray]:
        return None

    def get_split(self, instrument) -> Optional[np.ndarray]:
        return None

    def get_yield_curve(self, start_date, end_date, tenor=None) -> pd.DataFrame:
        return pd.DataFrame()

    def get_settle_price(self, instrument, date) -> float:
        return 0.0

    def get_exchange_rate(self, trading_date, local, settlement=MARKET.CN) -> ExchangeRate:
        return ExchangeRate(1.0, 1.0)

    def get_share_transformation(self, order_book_id) -> Optional[dict]:
        return None

    def get_futures_trading_parameters(self, instrument, dt):
        return None

    def get_open_auction_bar(self, instrument, dt):
        return None

    def get_open_auction_volume(self, instrument, dt):
        return None

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        return None

    def get_trading_minutes_for(self, instrument, trading_dt):
        return None

    def history_bars(
        self,
        instrument,
        bar_count: Optional[int],
        frequency: str,
        fields,
        dt: datetime.datetime,
        skip_suspended: bool = True,
        include_now: bool = False,
        adjust_type: str = "pre",
        adjust_orig=None,
    ) -> Optional[np.ndarray]:
        code = instrument.order_book_id if hasattr(instrument, "order_book_id") else str(instrument)
        target_date = dt.date() if hasattr(dt, "date") else dt

        df = self._load_daily(code)
        if df is None or df.empty:
            return None

        mask = df["date"] <= target_date
        if bar_count is not None:
            mask_df = df[mask].tail(bar_count)
        else:
            mask_df = df[mask]

        if mask_df.empty:
            return None

        field_list = (
            fields.split(",") if isinstance(fields, str) and fields else fields
        )
        if field_list:
            available = [f for f in field_list if f in mask_df.columns]
            if available:
                mask_df = mask_df[available]

        return mask_df.values.astype(np.float64)

    def history_ticks(self, instrument, count, dt):
        return None

    def current_snapshot(self, instrument, frequency, dt):
        return None

    def get_algo_bar(self, id_or_ins, start_min, end_min, dt):
        return None

    def is_suspended(self, order_book_id: str, dates) -> List[bool]:
        if isinstance(dates, (datetime.date, datetime.datetime, pd.Timestamp)):
            dates = [dates]
        df = self._load_daily(order_book_id)
        results = []
        for d in dates:
            target = d.date() if hasattr(d, "date") else d
            if df is None or df.empty:
                results.append(True)
                continue
            rows = df[df["date"] == target]
            if rows.empty:
                results.append(True)
            else:
                results.append(float(rows.iloc[0]["volume"]) == 0)
        return results

    def is_st_stock(self, order_book_id: str, dates) -> List[bool]:
        inst = self._instruments.get(order_book_id)
        if inst is None:
            if isinstance(dates, (datetime.date, datetime.datetime, pd.Timestamp)):
                return [False]
            return [False] * len(dates)
        raw_name = inst._dict.get("abbrev_symbol", "") or inst._dict.get("name", "")
        is_st = "ST" in raw_name or "*ST" in raw_name
        if isinstance(dates, (datetime.date, datetime.datetime, pd.Timestamp)):
            return [is_st]
        return [is_st] * len(dates)

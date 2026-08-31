"""HF-backed CorporateActionStore — reads real dividends/splits from Parquet."""

import datetime
import logging
import os
from pathlib import Path
from typing import List, Optional

import polars as pl

from core.corporate_actions import (
    ActionType,
    CorporateAction,
    CorporateActionStore,
)

logger = logging.getLogger(__name__)


class HFCorporateActionStore:
    """Corporate action store backed by Parquet files (local or HF).

    Reads:
    - dividends.parquet → CASH_DIVIDEND actions
    - splits.parquet    → STOCK_SPLIT actions

    Follows the same dual-backend pattern as RawPriceStore.
    """

    def __init__(
        self,
        data_root: Optional[str] = None,
        hf_repo_id: Optional[str] = None,
        hf_token: Optional[str] = None,
    ):
        if hf_repo_id:
            self._data_root = None
            self._hf_repo_id = hf_repo_id
            self._hf_token = hf_token or os.getenv("HF_TOKEN")
            self._dividends_cache: Optional[pl.DataFrame] = None
            self._splits_cache: Optional[pl.DataFrame] = None
        elif data_root:
            self._data_root = Path(data_root)
            self._hf_repo_id = None
            self._hf_token = None
            self._dividends_cache: Optional[pl.DataFrame] = None
            self._splits_cache: Optional[pl.DataFrame] = None
        else:
            raise ValueError("HFCorporateActionStore needs data_root or hf_repo_id")

    def _resolve_file(self, filename: str) -> Optional[Path]:
        """Resolve a parquet file path (local or HF download)."""
        if self._data_root:
            path = self._data_root / filename
            return path if path.exists() else None

        try:
            from huggingface_hub import hf_hub_download
            from huggingface_hub.utils import EntryNotFoundError

            path = hf_hub_download(
                repo_id=self._hf_repo_id,
                filename=filename,
                repo_type="dataset",
                token=self._hf_token,
            )
            return Path(path)
        except EntryNotFoundError:
            logger.warning(f"HF file missing: {self._hf_repo_id}/{filename}")
            return None
        except Exception as e:
            logger.warning(f"HF fetch failed for {filename}: {e}")
            return None

    def _load_dividends(self) -> pl.DataFrame:
        if self._dividends_cache is not None:
            return self._dividends_cache

        path = self._resolve_file("dividends.parquet")
        if path is None:
            self._dividends_cache = pl.DataFrame()
            return self._dividends_cache

        try:
            df = pl.read_parquet(path)
            self._dividends_cache = df
        except Exception as e:
            logger.warning(f"Failed to read dividends.parquet: {e}")
            self._dividends_cache = pl.DataFrame()

        return self._dividends_cache

    def _load_splits(self) -> pl.DataFrame:
        if self._splits_cache is not None:
            return self._splits_cache

        path = self._resolve_file("splits.parquet")
        if path is None:
            self._splits_cache = pl.DataFrame()
            return self._splits_cache

        try:
            df = pl.read_parquet(path)
            self._splits_cache = df
        except Exception as e:
            logger.warning(f"Failed to read splits.parquet: {e}")
            self._splits_cache = pl.DataFrame()

        return self._splits_cache

    def _parse_int_date(self, val) -> Optional[datetime.date]:
        """Convert int date (YYYYMMDD) or str date to datetime.date."""
        if val is None:
            return None
        if isinstance(val, (int, float)):
            ival = int(val)
            if ival == 0:
                return None
            try:
                return datetime.date(ival // 10000, (ival % 10000) // 100, ival % 100)
            except (ValueError, OverflowError):
                return None
        if isinstance(val, str):
            try:
                return datetime.date.fromisoformat(val)
            except ValueError:
                return None
        return None

    def _date_to_int(self, d: datetime.date) -> int:
        return d.year * 10000 + d.month * 100 + d.day

    def _build_actions(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        code_filter: Optional[str] = None,
    ) -> List[CorporateAction]:
        """Build CorporateAction list from loaded parquet data."""
        actions: List[CorporateAction] = []
        start_int = self._date_to_int(start_date)
        end_int = self._date_to_int(end_date)

        # Dividends → CASH_DIVIDEND
        div_df = self._load_dividends()
        if not div_df.is_empty():
            div_filtered = div_df.filter(
                (pl.col("ex_dividend_date") >= start_int)
                & (pl.col("ex_dividend_date") <= end_int)
            )
            if code_filter:
                div_filtered = div_filtered.filter(pl.col("code") == code_filter)

            for row in div_filtered.iter_rows(named=True):
                d = self._parse_int_date(row.get("ex_dividend_date"))
                if d is None:
                    continue
                actions.append(CorporateAction(
                    date=d,
                    code=row["code"],
                    action_type=ActionType.CASH_DIVIDEND,
                    cash_dividend_per_share=float(row.get("dividend_cash_before_tax", 0.0)),
                ))

        # Splits → STOCK_SPLIT
        split_df = self._load_splits()
        if not split_df.is_empty():
            split_filtered = split_df.filter(
                (pl.col("ex_date") >= start_int)
                & (pl.col("ex_date") <= end_int)
            )
            if code_filter:
                split_filtered = split_filtered.filter(pl.col("code") == code_filter)

            for row in split_filtered.iter_rows(named=True):
                d = self._parse_int_date(row.get("ex_date"))
                if d is None:
                    continue
                factor = float(row.get("split_factor", 1.0))
                actions.append(CorporateAction(
                    date=d,
                    code=row["code"],
                    action_type=ActionType.STOCK_SPLIT,
                    split_ratio=factor,
                ))

        actions.sort(key=lambda a: (a.code, a.date))
        return actions

    def to_store(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        code_filter: Optional[str] = None,
    ) -> CorporateActionStore:
        """Convert to CorporateActionStore for use with BacktestEngine."""
        actions = self._build_actions(start_date, end_date, code_filter)
        return CorporateActionStore(actions)

    def query(
        self,
        code: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> List[CorporateAction]:
        """Query corporate actions for a specific code in date range."""
        return self._build_actions(start_date, end_date, code_filter=code)

    def query_all(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> List[CorporateAction]:
        """Query all corporate actions in date range."""
        return self._build_actions(start_date, end_date)

    def clear_cache(self):
        self._dividends_cache = None
        self._splits_cache = None

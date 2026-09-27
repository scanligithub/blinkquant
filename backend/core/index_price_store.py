"""Index benchmark price access for backtests.

Production source: stockA HF Dataset index_kline_{YYYY}.parquet.
The store is deliberately separate from RawPriceStore because index data has
no adjustFactor and must never enter the stock QFQ path.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)

_INDEX_COLS = ["date", "code", "close"]


class _LocalIndexBackend:
    def __init__(self, data_root: str):
        self.data_root = Path(data_root)
        self._cache: dict[int, Optional[Path]] = {}

    def resolve_year_file(self, year: int) -> Optional[Path]:
        if year not in self._cache:
            p = self.data_root / f"index_kline_{year}.parquet"
            self._cache[year] = p if p.exists() else None
        return self._cache[year]


class _HFIndexBackend:
    def __init__(self, repo_id: str, token: Optional[str] = None):
        self.repo_id = repo_id
        self.token = token or os.getenv("HF_TOKEN")
        self._cache: dict[int, Optional[Path]] = {}

    def resolve_year_file(self, year: int) -> Optional[Path]:
        if year in self._cache:
            return self._cache[year]
        try:
            from huggingface_hub import hf_hub_download
            from huggingface_hub.utils import EntryNotFoundError
            path = hf_hub_download(
                repo_id=self.repo_id,
                filename=f"index_kline_{year}.parquet",
                repo_type="dataset",
                token=self.token,
            )
            self._cache[year] = Path(path)
        except EntryNotFoundError:
            logger.warning("HF index file missing: %s/index_kline_%s.parquet", self.repo_id, year)
            self._cache[year] = None
        except Exception as exc:
            logger.warning("HF index fetch failed for %s: %s", year, exc)
            self._cache[year] = None
        return self._cache[year]


class IndexPriceStore:
    """Read-only daily index close series from local parquet or HF Dataset."""

    INDEX_ALIASES = {
        "CSI300": "000300",
        "HS300": "000300",
        "沪深300": "000300",
    }

    def __init__(self, data_root: str | None = None, hf_repo_id: str | None = None, hf_token: str | None = None):
        if hf_repo_id:
            self.backend = _HFIndexBackend(hf_repo_id, hf_token)
        elif data_root:
            self.backend = _LocalIndexBackend(data_root)
        else:
            raise ValueError("IndexPriceStore requires data_root or hf_repo_id")

    @classmethod
    def canonical_index_id(cls, index_id: str) -> str:
        value = str(index_id).strip()
        return cls.INDEX_ALIASES.get(value.upper(), value)

    def load_close_window(
        self,
        index_id: str,
        start: dt.date,
        end: dt.date,
    ) -> pl.DataFrame:
        canonical = self.canonical_index_id(index_id)
        code_variants = self._index_code_variants(canonical)
        frames = []
        for year in range(start.year, end.year + 1):
            path = self.backend.resolve_year_file(year)
            if path is None:
                continue
            lf = pl.scan_parquet(path)
            schema = lf.collect_schema()
            if "date" not in schema.names() or "code" not in schema.names() or "close" not in schema.names():
                raise ValueError(f"index_kline_{year}.parquet missing required date/code/close columns")
            lf = lf.select(["date", "code", "close"])
            if schema.get("date") == pl.Utf8:
                lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            frames.append(
                lf.filter(
                    (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                    & (pl.col("code").is_in(code_variants))
                )
            )
        if not frames:
            raise ValueError(f"benchmark index data not found: {canonical}")
        df = pl.concat(frames, how="vertical").sort("date").unique("date", keep="last")
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if df.is_empty():
            raise ValueError(f"benchmark index {canonical} has no data in {start}..{end}")
        return df.collect()

    @staticmethod
    def _index_code_variants(index_id: str) -> list[str]:
        if index_id.isdigit():
            prefix = "sz" if index_id.startswith("39") else "sh"
            return [index_id, f"{prefix}{index_id}", f"{prefix}.{index_id}"]
        if "." in index_id:
            prefix, number = index_id.split(".", 1)
            return [index_id, f"{prefix}{number}", number]
        raise ValueError(f"unsupported benchmark index_id: {index_id}")

    def load_returns(
        self,
        index_id: str,
        dates: list[dt.date],
    ) -> tuple[pl.DataFrame, float]:
        """Return aligned daily returns and cumulative benchmark return.

        The window includes the prior trading day so the first requested date
        can have a valid close-to-close return.
        """
        if not dates:
            return pl.DataFrame(schema={"date": pl.Date, "benchmark_return": pl.Float64}), 0.0
        ordered = sorted(set(dates))
        start = ordered[0]
        end = ordered[-1]
        # Include the preceding calendar window; index files are filtered by date.
        window = self.load_close_window(index_id, start, end)
        window = window.with_columns(
            pl.col("close").pct_change().alias("benchmark_return")
        )
        aligned = (
            pl.DataFrame({"date": ordered})
            .join(window.select(["date", "benchmark_return"]), on="date", how="left")
        )
        if aligned["benchmark_return"].null_count() > 0:
            # First valuation date may legitimately lack a return. Later missing
            # observations are a data-integrity error, not a silent zero.
            if aligned["benchmark_return"][1:].null_count() > 0:
                raise ValueError("benchmark return series has missing trading-day observations")
            aligned = aligned.with_columns(pl.col("benchmark_return").fill_null(0.0))
        cumulative = float((1.0 + aligned["benchmark_return"]).product() - 1.0)
        return aligned, cumulative


__all__ = ["IndexPriceStore"]

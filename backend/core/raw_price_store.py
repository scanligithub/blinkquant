import datetime
import polars as pl
from pathlib import Path
from typing import Optional


class RawPriceStore:
    """
    Lazy parquet scanner for raw OHLCV data.
    Supports predicate pushdown via Polars LazyFrame.
    Window-cached for repeated scans within same backtest run.
    
    Key design:
    - Caches LazyFrame (query plan), NOT collected DataFrame
    - Uses Polars predicate pushdown on parquet files
    - Bounded cache (LRU, max 16 windows)
    """
    
    def __init__(self, data_root: str):
        self.data_root = Path(data_root)
        self._file_cache: dict[int, Path] = {}
        self._scan_cache: dict[tuple, pl.LazyFrame] = {}
        self._cache_order: list[tuple] = []  # for LRU
    
    def _find_year_file(self, year: int) -> Optional[Path]:
        if year in self._file_cache:
            return self._file_cache[year]
        candidates = list(self.data_root.glob(f"stock_kline_{year}.parquet"))
        if not candidates:
            return None
        self._file_cache[year] = candidates[0]
        return candidates[0]
    
    def scan_window(self, start: datetime.date, end: datetime.date) -> pl.LazyFrame:
        """
        Returns LazyFrame of raw OHLCV for [start, end] inclusive.
        Uses predicate pushdown on parquet files.
        Caches query plan (LazyFrame), not collected data.
        """
        cache_key = (start.isoformat(), end.isoformat())
        if cache_key in self._scan_cache:
            # LRU: move to end
            self._cache_order.remove(cache_key)
            self._cache_order.append(cache_key)
            return self._scan_cache[cache_key]
        
        lfs = []
        for year in range(start.year, end.year + 1):
            file = self._find_year_file(year)
            if file is None:
                continue
            lf = pl.scan_parquet(file).filter(
                (pl.col("date") >= start) & (pl.col("date") <= end)
            )
            lfs.append(lf)
        
        if not lfs:
            result = pl.LazyFrame(schema={
                "date": pl.Date, "code": pl.Utf8,
                "open": pl.Float32, "high": pl.Float32, "low": pl.Float32, "close": pl.Float32,
                "volume": pl.Float64, "amount": pl.Float64,
            })
        else:
            result = pl.concat(lfs, how="diagonal").sort(["code", "date"])
        
        # Cache (LRU: max 16 windows)
        if len(self._scan_cache) >= 16:
            oldest = self._cache_order.pop(0)
            del self._scan_cache[oldest]
        self._scan_cache[cache_key] = result
        self._cache_order.append(cache_key)
        return result
    
    def load_execution_prices(self, dates: list[datetime.date]) -> pl.DataFrame:
        """
        Returns DataFrame with raw_open/raw_close for given execution dates.
        Used by ExecutionEngine for fill prices.
        """
        if not dates:
            return pl.DataFrame(schema={
                "date": pl.Date, "code": pl.Utf8,
                "open": pl.Float32, "close": pl.Float32,
            })
        start = min(dates)
        end = max(dates)
        lf = self.scan_window(start, end).filter(pl.col("date").is_in(dates))
        return lf.select(["date", "code", "open", "close"]).collect()
    
    def clear_cache(self):
        self._scan_cache.clear()
        self._cache_order.clear()
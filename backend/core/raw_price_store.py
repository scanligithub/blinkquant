import datetime
import logging
import os
import polars as pl
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CANONICAL_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount"]


class _LocalParquetBackend:
    """本地目录后端：data_root/stock_kline_{YYYY}.parquet（开发/测试）。"""

    def __init__(self, data_root: str):
        self.data_root = Path(data_root)
        self._file_cache: dict[int, Optional[Path]] = {}

    def resolve_year_file(self, year: int) -> Optional[Path]:
        if year in self._file_cache:
            return self._file_cache[year]
        candidates = list(self.data_root.glob(f"stock_kline_{year}.parquet"))
        path = candidates[0] if candidates else None
        self._file_cache[year] = path
        return path

    @property
    def source_type(self) -> str:
        return f"local:{self.data_root}"


class _HFParquetBackend:
    """HF Dataset 后端：按需 hf_hub_download 单年文件到本地缓存，再 lazy scan。

    生产语义：
    - 不把完整 raw 数据载入常驻 RAM（文件落在 hub 磁盘缓存，polars 流式扫描）；
    - 年级粒度裁剪：只下载与回测窗口相交年份的 parquet；
    - 数据集内即未复权 raw OHLCV（前复权仅在 DataManager 内存层发生）。
    """

    def __init__(self, repo_id: str, token: Optional[str] = None):
        self.repo_id = repo_id
        self.token = token or os.getenv("HF_TOKEN")
        self._file_cache: dict[int, Optional[Path]] = {}

    def resolve_year_file(self, year: int) -> Optional[Path]:
        if year in self._file_cache:
            return self._file_cache[year]
        try:
            from huggingface_hub import hf_hub_download
            from huggingface_hub.utils import EntryNotFoundError
            path = hf_hub_download(
                repo_id=self.repo_id,
                filename=f"stock_kline_{year}.parquet",
                repo_type="dataset",
                token=self.token,
            )
            self._file_cache[year] = Path(path)
        except EntryNotFoundError:
            logger.warning(f"HF raw price year file missing: {self.repo_id}/stock_kline_{year}.parquet")
            self._file_cache[year] = None
        except Exception as e:  # 网络/鉴权等：降级为缺年，由上层严格估值兜底暴露
            logger.warning(f"HF raw price fetch failed for year {year}: {e}")
            self._file_cache[year] = None
        return self._file_cache[year]

    @property
    def source_type(self) -> str:
        return f"hf:{self.repo_id}"


class RawPriceStore:
    """
    raw OHLCV 访问门面。数据源隐藏在后端之后：

    - RawPriceStore(data_root=...)      → 本地 parquet（开发/测试）
    - RawPriceStore(hf_repo_id=...)     → HF Dataset（生产，按年懒下载 + 磁盘缓存）

    关键设计：
    - scan_window 返回 LazyFrame 查询计划（缓存计划而非 collect 结果，LRU≤16）；
    - 兼容两种 date 物理类型：Date（本地测试夹具）与 Utf8 "YYYY-MM-DD"
      （HF 数据集原貌），统一归一化为 Date 后再做范围过滤。
    """

    def __init__(
        self,
        data_root: Optional[str] = None,
        hf_repo_id: Optional[str] = None,
        hf_token: Optional[str] = None,
    ):
        if hf_repo_id:
            self.backend = _HFParquetBackend(hf_repo_id, hf_token)
        elif data_root:
            self.backend = _LocalParquetBackend(data_root)
        else:
            raise ValueError("RawPriceStore 需要 data_root 或 hf_repo_id 之一")
        self._scan_cache: dict[tuple, pl.LazyFrame] = {}
        self._cache_order: list[tuple] = []

    @property
    def source_type(self) -> str:
        return self.backend.source_type

    @staticmethod
    def _normalized(lf: pl.LazyFrame) -> pl.LazyFrame:
        """date 列归一化到 Date；投影到规范列集，屏蔽数据集额外列差异。"""
        schema = lf.collect_schema()
        if schema.get("date") == pl.Utf8:
            lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
        cols = [c for c in _CANONICAL_COLS if c in schema.names()]
        return lf.select(cols)

    def scan_window(self, start: datetime.date, end: datetime.date) -> pl.LazyFrame:
        """返回 [start, end] 内 raw OHLCV 的 LazyFrame（LRU 缓存查询计划）。"""
        cache_key = (start.isoformat(), end.isoformat())
        if cache_key in self._scan_cache:
            self._cache_order.remove(cache_key)
            self._cache_order.append(cache_key)
            return self._scan_cache[cache_key]

        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = self._normalized(pl.scan_parquet(file)).filter(
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

        if len(self._scan_cache) >= 16:
            oldest = self._cache_order.pop(0)
            del self._scan_cache[oldest]
        self._scan_cache[cache_key] = result
        self._cache_order.append(cache_key)
        return result

    def load_execution_prices(self, dates: list[datetime.date]) -> pl.DataFrame:
        """指定交易日的 date/code/open/close（执行价 open、估值 close 同源单次加载）。"""
        if not dates:
            return pl.DataFrame(schema={
                "date": pl.Date, "code": pl.Utf8,
                "open": pl.Float32, "close": pl.Float32,
            })
        start, end = min(dates), max(dates)
        lf = self.scan_window(start, end).filter(pl.col("date").is_in(dates))
        return lf.select(["date", "code", "open", "close"]).collect()

    def clear_cache(self):
        self._scan_cache.clear()
        self._cache_order.clear()
import datetime
import logging
import os
import polars as pl
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CANONICAL_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount"]
_QFQ_COLS = ["date", "code", "open", "high", "low", "close", "volume", "amount", "adjustFactor"]


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
    def _normalized(lf: pl.LazyFrame, for_qfq: bool = False) -> pl.LazyFrame:
        """date 列归一化到 Date；投影到规范列集，屏蔽数据集额外列差异。"""
        schema = lf.collect_schema()
        if schema.get("date") == pl.Utf8:
            lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
        cols = [c for c in (_QFQ_COLS if for_qfq else _CANONICAL_COLS) if c in schema.names()]
        return lf.select(cols)

    def scan_window(self, start: datetime.date, end: datetime.date, for_qfq: bool = False) -> pl.LazyFrame:
        """返回 [start, end] 内 raw OHLCV 的 LazyFrame（LRU 缓存查询计划）。
        
        Args:
            start: 开始日期
            end: 结束日期
            for_qfq: 是否包含 adjustFactor 列用于前复权计算
        """
        cache_key = (start.isoformat(), end.isoformat(), for_qfq)
        if cache_key in self._scan_cache:
            self._cache_order.remove(cache_key)
            self._cache_order.append(cache_key)
            return self._scan_cache[cache_key]

        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = self._normalized(pl.scan_parquet(file), for_qfq=for_qfq).filter(
                (pl.col("date") >= start) & (pl.col("date") <= end)
            )
            lfs.append(lf)

        if not lfs:
            cols = _QFQ_COLS if for_qfq else _CANONICAL_COLS
            result = pl.LazyFrame(schema={
                "date": pl.Date, "code": pl.Utf8,
                "open": pl.Float32, "high": pl.Float32, "low": pl.Float32, "close": pl.Float32,
                "volume": pl.Float64, "amount": pl.Float64,
                "adjustFactor": pl.Float32,
            }).select(cols)
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

    def load_latest_adjust_factors(self) -> dict[str, float]:
        """扫描全量数据一次，获取每个 code 的最新 adjustFactor（用于前复权计算）。
        
        返回: {code: latest_adjust_factor}
        """
        lfs = []
        if hasattr(self.backend, 'repo_id'):
            years = range(2009, 2025)
        else:
            import glob
            files = glob.glob(str(self.backend.data_root / "stock_kline_*.parquet"))
            years = []
            for f in files:
                try:
                    year = int(f.split("_")[-1].replace(".parquet", ""))
                    years.append(year)
                except ValueError:
                    pass
        
        for year in years:
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            try:
                lf = pl.scan_parquet(file).select(["code", "adjustFactor", "date"])
                lfs.append(lf)
            except Exception:
                continue
        
        if not lfs:
            return {}
        
        combined = pl.concat(lfs, how="diagonal").sort(["code", "date"])
        latest = (
            combined.group_by("code")
            .agg(pl.col("adjustFactor").last().alias("latest_adj"))
            .collect()
        )
        return {row["code"]: float(row["latest_adj"]) for row in latest.iter_rows(named=True)}

    def load_qfq_window(self, start: datetime.date, end: datetime.date, latest_adj: dict[str, float]) -> pl.DataFrame:
        """加载 [start, end] 范围内的前复权 OHLCV 数据。
        
        Args:
            start: 开始日期
            end: 结束日期
            latest_adj: {code: latest_adjust_factor} 来自 load_latest_adjust_factors()
            
        返回: DataFrame with columns [date, code, open, high, low, close, volume, amount] (qfq-adjusted)
        """
        # 加载 raw + adjustFactor
        lf = self.scan_window(start, end, for_qfq=True)
        
        # 对 adjustFactor 进行 forward_fill，模拟 DataManager 的行为
        lf = lf.with_columns(
            pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code").alias("adjustFactor_ff")
        )
        
        # 将 latest_adj 转为 DataFrame 并 join
        adj_df = pl.DataFrame({
            "code": list(latest_adj.keys()),
            "latest_adj": list(latest_adj.values())
        })
        
        # Join 并计算 qfq
        lf = lf.join(adj_df.lazy(), on="code", how="left")
        
        # 应用前复权: qfq_price = raw_price * (adjustFactor_ff / latest_adj)
        adj_exprs = []
        for col in ["open", "high", "low", "close"]:
            adj_exprs.append(
                pl.when(pl.col("latest_adj").is_not_null())
                .then(
                    (pl.col(col) * pl.col("adjustFactor_ff") / pl.col("latest_adj")).cast(pl.Float32)
                )
                .otherwise(pl.col(col).cast(pl.Float32))
                .alias(col)
            )
        
        # volume 反向调整
        adj_exprs.append(
            pl.when(pl.col("latest_adj").is_not_null())
            .then((pl.col("volume") / pl.col("latest_adj")).cast(pl.Float64))
            .otherwise(pl.col("volume").cast(pl.Float64))
            .alias("volume")
        )
        
        result = lf.with_columns(adj_exprs).select(
            ["date", "code", "open", "high", "low", "close", "volume", "amount"]
        ).collect()
        
        return result

    def load_limit_flags(self, dates: list[datetime.date]) -> dict[datetime.date, dict[str, dict]]:
        """计算指定日期的涨跌停/停牌标志（PIT-safe，基于 raw 数据）。
        
        Args:
            dates: 执行日期列表
            
        Returns:
            {date: {code: {"is_limit_up": bool, "is_touch_limit_up": bool,
                           "is_limit_down": bool, "is_touch_limit_down": bool,
                           "is_suspended": bool}}}
        """
        if not dates:
            return {}
        
        start = min(dates) - datetime.timedelta(days=5)
        end = max(dates)
        
        # 加载 raw 数据：close, high, low, volume, amount, isST, pctChg
        # 直接扫描 parquet 文件，选择需要的列（绕过 scan_window 的列过滤）
        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = pl.scan_parquet(file).select(
                ["date", "code", "close", "high", "low", "volume", "amount", "isST", "pctChg"]
            )
            # 归一化 date 列
            schema = lf.collect_schema()
            if schema.get("date") == pl.Utf8:
                lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            lf = lf.filter(
                (pl.col("date") >= start) & (pl.col("date") <= end)
            )
            lfs.append(lf)
        
        if not lfs:
            return {}
        
        lf = pl.concat(lfs, how="diagonal").sort(["code", "date"]).collect()
        
        if lf.is_empty():
            return {}
        
        # 计算涨跌停幅度（复制自 data_manager.limit_pct_expr，避免循环引用）
        from core.data_manager import (
            MAIN_BOARD_ST_UNIFY_DATE, CHI_NEXT_REFORM_DATE,
            NO_LIMIT_BAND_MARGIN_PCT, EX_DIV_DETECT_THRESHOLD
        )
        
        # 涨跌停幅度表达式
        base = pl.when(
            pl.col("code").str.starts_with("sh.688")
            | pl.col("code").str.starts_with("sh.689")
        ).then(pl.lit(20.0)).when(
            pl.col("code").str.starts_with("bj.")
        ).then(pl.lit(30.0)).when(
            pl.col("code").str.starts_with("sz.30")
            & (pl.col("date") < pl.lit(CHI_NEXT_REFORM_DATE))
        ).then(pl.lit(10.0)).when(
            pl.col("code").str.starts_with("sz.30")
        ).then(pl.lit(20.0)).otherwise(pl.lit(10.0))
        st_main_5 = (
            (base == pl.lit(10.0))
            & (pl.col("isST").cast(pl.Int8, strict=False) == 1)
            & (pl.col("date") < pl.lit(MAIN_BOARD_ST_UNIFY_DATE))
        ).fill_null(False)
        pct_band_expr = pl.when(st_main_5).then(pl.lit(5.0)).otherwise(base)
        
        lf = lf.with_columns(pct_band_expr.alias("pct_band"))
        lf = lf.sort(["code", "date"])
        
        prev_raw = pl.col("close").shift(1).over("code")
        pct_chg = pl.col("pctChg").cast(pl.Float64, strict=False)
        pct_valid = pct_chg.is_not_null() & (pct_chg.abs() < pl.lit(40.0))
        implied_prev = pl.col("close") / (pl.lit(1.0) + pct_chg / pl.lit(100.0))
        eff_prev = pl.when(
            pct_valid & ((implied_prev - prev_raw).abs() > pl.lit(EX_DIV_DETECT_THRESHOLD))
        ).then(implied_prev).otherwise(prev_raw)
        
        eps = 1e-4
        pct_band = pl.col("pct_band")
        
        up_price = (eff_prev * (pl.lit(1.0) + pct_band / pl.lit(100.0))).round(2)
        down_price = (eff_prev * (pl.lit(1.0) - pct_band / pl.lit(100.0))).round(2)
        
        no_limit_day = (
            (pct_chg > (pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT))) |
            (pct_chg < -(pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT)))
        ).fill_null(False)
        
        lf = lf.with_columns([
            ((pl.col("close") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_limit_up"),
            ((pl.col("high") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_up"),
            ((pl.col("close") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_limit_down"),
            ((pl.col("low") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_down"),
            ((pl.col("volume") == 0) | (pl.col("amount") == 0)).alias("is_suspended"),
        ])
        
        # 只保留请求的日期
        lf = lf.filter(pl.col("date").is_in(dates))
        
        # 转为 dict 格式
        result = {}
        for row in lf.iter_rows(named=True):
            d = row["date"]
            if d not in result:
                result[d] = {}
            result[d][row["code"]] = {
                "is_limit_up": row["is_limit_up"],
                "is_touch_limit_up": row["is_touch_limit_up"],
                "is_limit_down": row["is_limit_down"],
                "is_touch_limit_down": row["is_touch_limit_down"],
                "is_suspended": row["is_suspended"],
            }
        
        return result

    def load_limit_flags(self, dates: list[datetime.date]) -> dict[datetime.date, dict[str, dict]]:
        """计算指定日期的涨跌停/停牌标志（PIT-safe，基于 raw 数据）。
        
        批量版本：用于预加载场景。对于真正的懒加载，请使用 load_limit_flags_for_date。
        """
        if not dates:
            return {}
        
        start = min(dates) - datetime.timedelta(days=5)
        end = max(dates)
        
        # 加载 raw 数据：close, high, low, volume, amount, isST, pctChg
        # 直接扫描 parquet 文件，选择需要的列（绕过 scan_window 的列过滤）
        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = pl.scan_parquet(file).select(
                ["date", "code", "close", "high", "low", "volume", "amount", "isST", "pctChg"]
            )
            # 归一化 date 列
            schema = lf.collect_schema()
            if schema.get("date") == pl.Utf8:
                lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            lf = lf.filter(
                (pl.col("date") >= start) & (pl.col("date") <= end)
            )
            lfs.append(lf)
        
        if not lfs:
            return {}
        
        lf = pl.concat(lfs, how="diagonal").sort(["code", "date"]).collect()
        
        # 计算涨跌停幅度（复制自 data_manager.limit_pct_expr，避免循环引用）
        from core.data_manager import (
            MAIN_BOARD_ST_UNIFY_DATE, CHI_NEXT_REFORM_DATE,
            NO_LIMIT_BAND_MARGIN_PCT, EX_DIV_DETECT_THRESHOLD
        )
        
        # 涨跌停幅度表达式
        base = pl.when(
            pl.col("code").str.starts_with("sh.688")
            | pl.col("code").str.starts_with("sh.689")
        ).then(pl.lit(20.0)).when(
            pl.col("code").str.starts_with("bj.")
        ).then(pl.lit(30.0)).when(
            pl.col("code").str.starts_with("sz.30")
            & (pl.col("date") < pl.lit(CHI_NEXT_REFORM_DATE))
        ).then(pl.lit(10.0)).when(
            pl.col("code").str.starts_with("sz.30")
        ).then(pl.lit(20.0)).otherwise(pl.lit(10.0))
        st_main_5 = (
            (base == pl.lit(10.0))
            & (pl.col("isST").cast(pl.Int8, strict=False) == 1)
            & (pl.col("date") < pl.lit(MAIN_BOARD_ST_UNIFY_DATE))
        ).fill_null(False)
        pct_band_expr = pl.when(st_main_5).then(pl.lit(5.0)).otherwise(base)
        
        lf = lf.with_columns(pct_band_expr.alias("pct_band"))
        lf = lf.sort(["code", "date"])
        
        prev_raw = pl.col("close").shift(1).over("code")
        pct_chg = pl.col("pctChg").cast(pl.Float64, strict=False)
        pct_valid = pct_chg.is_not_null() & (pct_chg.abs() < pl.lit(40.0))
        implied_prev = pl.col("close") / (pl.lit(1.0) + pct_chg / pl.lit(100.0))
        eff_prev = pl.when(
            pct_valid & ((implied_prev - prev_raw).abs() > pl.lit(EX_DIV_DETECT_THRESHOLD))
        ).then(implied_prev).otherwise(prev_raw)
        
        eps = 1e-4
        pct_band = pl.col("pct_band")
        
        up_price = (eff_prev * (pl.lit(1.0) + pct_band / pl.lit(100.0))).round(2)
        down_price = (eff_prev * (pl.lit(1.0) - pct_band / pl.lit(100.0))).round(2)
        
        no_limit_day = (
            (pct_chg > (pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT))) |
            (pct_chg < -(pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT)))
        ).fill_null(False)
        
        lf = lf.with_columns([
            ((pl.col("close") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_limit_up"),
            ((pl.col("high") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_up"),
            ((pl.col("close") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_limit_down"),
            ((pl.col("low") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_down"),
            ((pl.col("volume") == 0) | (pl.col("amount") == 0)).alias("is_suspended"),
        ])
        
        # 只保留请求的日期
        lf = lf.filter(pl.col("date").is_in(dates))
        
        # 转为 dict 格式
        result = {}
        for row in lf.iter_rows(named=True):
            d = row["date"]
            if d not in result:
                result[d] = {}
            result[d][row["code"]] = {
                "is_limit_up": row["is_limit_up"],
                "is_touch_limit_up": row["is_touch_limit_up"],
                "is_limit_down": row["is_limit_down"],
                "is_touch_limit_down": row["is_touch_limit_down"],
                "is_suspended": row["is_suspended"],
            }
        
        return result

    def load_limit_flags_for_date(self, date: datetime.date, codes: list[str]) -> dict[str, dict]:
        """按需计算单个执行日期的涨跌停/停牌标志（真正的懒加载）。
        
        只加载该日期及前一天的数据，且只针对指定 codes。
        
        Args:
            date: 执行日期
            codes: 需要查询的股票代码列表
            
        Returns:
            {code: {"is_limit_up": bool, "is_touch_limit_up": bool,
                    "is_limit_down": bool, "is_touch_limit_down": bool,
                    "is_suspended": bool}}
        """
        if not codes:
            return {}
        
        # 需要前一天的 close 来计算涨跌停价
        start = date - datetime.timedelta(days=5)
        end = date
        
        # 只加载指定 codes 的数据
        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = pl.scan_parquet(file).select(
                ["date", "code", "close", "high", "low", "volume", "amount", "isST", "pctChg"]
            )
            # 归一化 date 列
            schema = lf.collect_schema()
            if schema.get("date") == pl.Utf8:
                lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            # 只过滤日期和 codes
            lf = lf.filter(
                (pl.col("date") >= start) & (pl.col("date") <= end) &
                (pl.col("code").is_in(codes))
            )
            lfs.append(lf)
        
        if not lfs:
            return {}
        
        lf = pl.concat(lfs, how="diagonal").sort(["code", "date"]).collect()
        
        if lf.is_empty():
            return {}
        
        # 计算涨跌停幅度（复制自 data_manager.limit_pct_expr，避免循环引用）
        from core.data_manager import (
            MAIN_BOARD_ST_UNIFY_DATE, CHI_NEXT_REFORM_DATE,
            NO_LIMIT_BAND_MARGIN_PCT, EX_DIV_DETECT_THRESHOLD
        )
        
        # 涨跌停幅度表达式
        base = pl.when(
            pl.col("code").str.starts_with("sh.688")
            | pl.col("code").str.starts_with("sh.689")
        ).then(pl.lit(20.0)).when(
            pl.col("code").str.starts_with("bj.")
        ).then(pl.lit(30.0)).when(
            pl.col("code").str.starts_with("sz.30")
            & (pl.col("date") < pl.lit(CHI_NEXT_REFORM_DATE))
        ).then(pl.lit(10.0)).when(
            pl.col("code").str.starts_with("sz.30")
        ).then(pl.lit(20.0)).otherwise(pl.lit(10.0))
        st_main_5 = (
            (base == pl.lit(10.0))
            & (pl.col("isST").cast(pl.Int8, strict=False) == 1)
            & (pl.col("date") < pl.lit(MAIN_BOARD_ST_UNIFY_DATE))
        ).fill_null(False)
        pct_band_expr = pl.when(st_main_5).then(pl.lit(5.0)).otherwise(base)
        
        lf = lf.with_columns(pct_band_expr.alias("pct_band"))
        lf = lf.sort(["code", "date"])
        
        prev_raw = pl.col("close").shift(1).over("code")
        pct_chg = pl.col("pctChg").cast(pl.Float64, strict=False)
        pct_valid = pct_chg.is_not_null() & (pct_chg.abs() < pl.lit(40.0))
        implied_prev = pl.col("close") / (pl.lit(1.0) + pct_chg / pl.lit(100.0))
        eff_prev = pl.when(
            pct_valid & ((implied_prev - prev_raw).abs() > pl.lit(EX_DIV_DETECT_THRESHOLD))
        ).then(implied_prev).otherwise(prev_raw)
        
        eps = 1e-4
        pct_band = pl.col("pct_band")
        
        up_price = (eff_prev * (pl.lit(1.0) + pct_band / pl.lit(100.0))).round(2)
        down_price = (eff_prev * (pl.lit(1.0) - pct_band / pl.lit(100.0))).round(2)
        
        no_limit_day = (
            (pct_chg > (pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT))) |
            (pct_chg < -(pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT)))
        ).fill_null(False)
        
        lf = lf.with_columns([
            ((pl.col("close") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_limit_up"),
            ((pl.col("high") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_up"),
            ((pl.col("close") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_limit_down"),
            ((pl.col("low") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_down"),
            ((pl.col("volume") == 0) | (pl.col("amount") == 0)).alias("is_suspended"),
        ])
        
        # 只保留请求的日期
        lf = lf.filter(pl.col("date") == date)
        
        # 转为 dict 格式
        result = {}
        for row in lf.iter_rows(named=True):
            if row["code"] in codes:
                result[row["code"]] = {
                    "is_limit_up": row["is_limit_up"],
                    "is_touch_limit_up": row["is_touch_limit_up"],
                    "is_limit_down": row["is_limit_down"],
                    "is_touch_limit_down": row["is_touch_limit_down"],
                    "is_suspended": row["is_suspended"],
                }
        
        return result

    def clear_cache(self):
        self._scan_cache.clear()
        self._cache_order.clear()

    def get_trading_dates(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
        """获取 [start, end] 范围内的所有交易日（去重排序）。"""
        lfs = []
        for year in range(start.year, end.year + 1):
            file = self.backend.resolve_year_file(year)
            if file is None:
                continue
            lf = pl.scan_parquet(file).select(["date"])
            # 归一化 date 列
            schema = lf.collect_schema()
            if schema.get("date") == pl.Utf8:
                lf = lf.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            lf = lf.filter(
                (pl.col("date") >= start) & (pl.col("date") <= end)
            )
            lfs.append(lf)
        
        if not lfs:
            return []
        
        result = pl.concat(lfs, how="diagonal").unique().sort("date").collect()
        return result["date"].to_list()

    def clear_cache(self):
        self._scan_cache.clear()
        self._cache_order.clear()
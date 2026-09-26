"""PIT 历史指数成分股解析器。

UniverseResolver 只负责回答：
    给定 index_id + as_of_date，当时哪些股票属于该指数？

它不负责：
- 行情加载
- ST / IPO 过滤
- 信号计算
- 排名
- 组合构建

成员关系语义严格为：
    start_date <= as_of < end_date
end_date = NULL 表示当前仍有效。
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Optional

import polars as pl


REQUIRED_COLUMNS = ["index_id", "stock_id", "start_date", "end_date"]


class UniverseDataError(ValueError):
    """PIT Universe 数据结构或区间关系不合法。"""


class UniverseResolver:
    """基于 PIT index_membership_history 的历史 Universe Resolver。"""

    def __init__(self, membership: pl.DataFrame):
        self._df = self._prepare(membership)

    @classmethod
    def from_parquet(cls, path: str | Path) -> "UniverseResolver":
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(f"index membership file not found: {path}")
        return cls(pl.read_parquet(path))

    @classmethod
    def from_huggingface(
        cls,
        repo_id: str = "scanli/stocka-data",
        filename: str = "index_membership/index_membership_history.parquet",
        token: Optional[str] = None,
        cache_dir: Optional[str | Path] = None,
    ) -> "UniverseResolver":
        """从 stockA HF Dataset 加载 PIT 成分股文件。"""
        from huggingface_hub import hf_hub_download

        local_path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            token=token,
            cache_dir=str(cache_dir) if cache_dir is not None else None,
        )
        return cls.from_parquet(local_path)

    @staticmethod
    def _prepare(df: pl.DataFrame) -> pl.DataFrame:
        if not isinstance(df, pl.DataFrame):
            raise TypeError("membership must be a polars DataFrame")

        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise UniverseDataError(
                f"membership missing required columns: {missing}"
            )

        if df.is_empty():
            # 空文件仍允许构造；members() 会返回空集合。
            return pl.DataFrame(
                schema={
                    "index_id": pl.Utf8,
                    "stock_id": pl.Utf8,
                    "start_date": pl.Date,
                    "end_date": pl.Date,
                }
            )

        result = (
            df.select(REQUIRED_COLUMNS)
            .with_columns(
                pl.col("index_id").cast(pl.Utf8).str.strip_chars(),
                pl.col("stock_id").cast(pl.Utf8).str.strip_chars(),
                pl.col("start_date").cast(pl.Date, strict=False),
                pl.col("end_date").cast(pl.Date, strict=False),
            )
        )

        if result.filter(
            pl.col("index_id").is_null()
            | (pl.col("index_id") == "")
            | pl.col("stock_id").is_null()
            | (pl.col("stock_id") == "")
            | pl.col("start_date").is_null()
        ).height:
            raise UniverseDataError(
                "membership contains null/empty index_id or stock_id, "
                "or invalid start_date"
            )

        invalid_end = result.filter(
            pl.col("end_date").is_not_null()
            & (pl.col("end_date") <= pl.col("start_date"))
        )
        if not invalid_end.is_empty():
            raise UniverseDataError(
                "membership contains end_date <= start_date"
            )

        duplicate = (
            result.group_by(REQUIRED_COLUMNS)
            .len()
            .filter(pl.col("len") > 1)
        )
        if not duplicate.is_empty():
            raise UniverseDataError(
                f"membership contains {duplicate.height} duplicate intervals"
            )

        # 检查同一 index/stock 的有效区间不能重叠。
        # NULL end_date 视为 +infinity，因此后续任何区间都非法。
        ordered = result.sort(["index_id", "stock_id", "start_date", "end_date"])
        # NULL end_date = +infinity。使用 date.max 作为校验哨兵，
        # 同时保留原始 end_date 供最终 PIT 查询使用。
        max_date = dt.date.max
        overlap = (
            ordered
            .with_columns(
                pl.col("end_date")
                .fill_null(max_date)
                .shift(1)
                .over(["index_id", "stock_id"])
                .alias("_prev_end")
            )
            .filter(
                pl.col("_prev_end").is_not_null()
                & (pl.col("start_date") < pl.col("_prev_end"))
            )
        )
        if not overlap.is_empty():
            raise UniverseDataError(
                f"membership contains overlapping intervals: {overlap.height} rows"
            )

        return ordered

    def members(
        self,
        index_id: str,
        as_of_date: dt.date,
    ) -> list[str]:
        """返回 as_of_date 当日有效的历史成分股，按 stock_id 排序。

        严格采用：
            start_date <= as_of_date < end_date
        NULL end_date 表示无结束日期。
        """
        if not isinstance(as_of_date, dt.date):
            raise TypeError("as_of_date must be datetime.date")

        index_id = str(index_id).strip()
        if not index_id:
            raise ValueError("index_id must not be empty")

        day = dt.date(as_of_date.year, as_of_date.month, as_of_date.day)

        result = (
            self._df
            .filter(
                (pl.col("index_id") == index_id)
                & (pl.col("start_date") <= day)
                & (
                    pl.col("end_date").is_null()
                    | (day < pl.col("end_date"))
                )
            )
            .select("stock_id")
            .unique()
            .sort("stock_id")
        )
        return result["stock_id"].to_list()

    def contains(
        self,
        index_id: str,
        stock_id: str,
        as_of_date: dt.date,
    ) -> bool:
        """判断单只股票在 as_of_date 是否属于指数。"""
        if not isinstance(as_of_date, dt.date):
            raise TypeError("as_of_date must be datetime.date")

        index_id = str(index_id).strip()
        stock_id = str(stock_id).strip()
        day = dt.date(as_of_date.year, as_of_date.month, as_of_date.day)

        return (
            self._df
            .filter(
                (pl.col("index_id") == index_id)
                & (pl.col("stock_id") == stock_id)
                & (pl.col("start_date") <= day)
                & (
                    pl.col("end_date").is_null()
                    | (day < pl.col("end_date"))
                )
            )
            .height
            > 0
        )

    def available_indexes(self) -> list[str]:
        """返回数据中存在的指数代码。"""
        return (
            self._df.select("index_id")
            .unique()
            .sort("index_id")["index_id"]
            .to_list()
        )

    @property
    def membership(self) -> pl.DataFrame:
        """只读语义下返回规范化后的成员关系表。"""
        return self._df


__all__ = ["UniverseResolver", "UniverseDataError"]

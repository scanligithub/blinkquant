"""Universe 构造：选股前的预过滤。"""
import datetime
from dataclasses import dataclass
from typing import Optional
import polars as pl


@dataclass
class UniverseFilter:
    """Universe 过滤器。

    Attributes:
        min_listing_days: 最小上市天数（排除 IPO 新股）。0 = 不过滤。
        exclude_st: 是否排除 ST/*ST 股票。
    """
    min_listing_days: int = 60
    exclude_st: bool = True

    def filter(
        self,
        codes_or_df,
        target_date: datetime.date,
        *,
        df: Optional[pl.DataFrame] = None,
    ) -> list[str]:
        """过滤 target_date 的 eligible codes。

        兼容旧的 DataFrame 调用，同时支持 BacktestEngine 的
        filter(codes, target_date, df=...) 契约。
        """
        if isinstance(codes_or_df, pl.DataFrame):
            day_df = codes_or_df.filter(pl.col("date") == target_date)
        else:
            if df is None or not isinstance(df, pl.DataFrame):
                raise TypeError("list[str] input requires df=Polars DataFrame")
            codes = list(codes_or_df)
            if not codes:
                return []
            day_df = df.filter(
                (pl.col("date") == target_date)
                & pl.col("code").is_in(codes)
            )

        # IPO 过滤
        if self.min_listing_days > 0 and "listing_date" in day_df.columns:
            cutoff = target_date - datetime.timedelta(days=self.min_listing_days)
            day_df = day_df.filter(
                pl.col("listing_date") <= cutoff
            )

        # ST 过滤
        if self.exclude_st and "is_st" in day_df.columns:
            day_df = day_df.filter(~pl.col("is_st"))

        return day_df["code"].to_list()

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

    def filter(self, df: pl.DataFrame, target_date: datetime.date) -> list[str]:
        """从 df 中筛选 target_date 的 eligible codes。

        Args:
            df: 包含 'date', 'code' 列，可选 'listing_date', 'is_st' 列
            target_date: 选股目标日期

        Returns:
            过滤后的 code 列表
        """
        day_df = df.filter(pl.col("date") == target_date)
        if day_df.is_empty():
            return []

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

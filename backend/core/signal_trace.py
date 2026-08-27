"""SignalTrace：完整决策链审计。"""
import datetime
from dataclasses import dataclass, field
from typing import Optional, List
import polars as pl


@dataclass
class TraceRecord:
    """单条交易决策审计记录。"""
    signal_date: datetime.date
    execution_date: datetime.date
    code: str
    formula: str
    eligible_count: int = 0
    ranking_score: float = 0.0
    ranking_position: int = 0
    target_weight: float = 0.0
    side: str = ""           # BUY / SELL
    target_qty: int = 0
    fill_qty: int = 0
    fill_price: float = 0.0
    fee: float = 0.0
    rejection_reason: str = ""
    post_qty: int = 0        # 成交后持仓数量
    post_cost: float = 0.0   # 成交后成本
    post_cash: float = 0.0   # 成交后现金


class SignalTrace:
    """决策链审计存储。"""

    def __init__(self):
        self._records: List[TraceRecord] = []

    def record(self, rec: TraceRecord):
        self._records.append(rec)

    def query(self, code: str = None, signal_date: datetime.date = None,
              execution_date: datetime.date = None) -> List[TraceRecord]:
        """按条件过滤记录。"""
        result = self._records
        if code is not None:
            result = [r for r in result if r.code == code]
        if signal_date is not None:
            result = [r for r in result if r.signal_date == signal_date]
        if execution_date is not None:
            result = [r for r in result if r.execution_date == execution_date]
        return result

    def to_dataframe(self) -> pl.DataFrame:
        """转换为 Polars DataFrame。"""
        if not self._records:
            return pl.DataFrame()
        return pl.DataFrame([vars(r) for r in self._records])

    def __len__(self):
        return len(self._records)

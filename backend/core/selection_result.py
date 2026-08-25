from dataclasses import dataclass
from typing import Optional
import datetime


@dataclass
class SelectionResult:
    """选股结果契约。
    
    Attributes:
        requested_date: 用户原始输入日期（可为 None/非交易日），用于审计
        signal_date: 归一化后的实际 as-of 交易日（≤ requested_date 的最近交易日）
        codes: 选股结果代码列表
        metadata: 扩展元数据
    """
    requested_date: Optional[datetime.date]
    signal_date: datetime.date
    codes: list[str]
    metadata: dict
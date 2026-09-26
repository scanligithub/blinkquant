"""公司行为数据模型与 Store。"""
import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List
from pathlib import Path
import json
import polars as pl


class ActionType(Enum):
    CASH_DIVIDEND = "cash_dividend"
    STOCK_SPLIT = "stock_split"
    BONUS_SHARES = "bonus_shares"
    RIGHTS_ISSUE = "rights_issue"


@dataclass
class CorporateAction:
    """单条公司行为记录。"""
    date: datetime.date           # 公告日 / 除权除息日
    code: str
    action_type: ActionType
    # 现金分红
    cash_dividend_per_share: float = 0.0
    # 送股/转增/拆股：每 10 股送 N 股 → split_ratio = (10+N)/10
    split_ratio: float = 1.0
    # 配股
    rights_price: float = 0.0     # 配股价格
    rights_ratio: float = 0.0     # 每 10 股配 N 股 → ratio = (10+N)/10


def adjust_qty_for_split(total_qty: int, avg_cost: float,
                         split_ratio: float,
                         frozen_qty: int = 0) -> tuple[int, float, int, int]:
    """送股/转增/拆股后调整持股数量和成本。

    Returns:
        (new_total, new_cost, new_available, new_frozen)
    """
    if split_ratio <= 0 or split_ratio == 1.0:
        return total_qty, avg_cost, total_qty - frozen_qty, frozen_qty
    new_total = int(total_qty * split_ratio)
    new_frozen = int(frozen_qty * split_ratio)
    new_available = new_total - new_frozen
    new_cost = avg_cost / split_ratio if split_ratio > 0 else 0.0
    return new_total, new_cost, new_available, new_frozen


def adjust_avg_cost_for_dividend(avg_cost: float,
                                 dividend_per_share: float) -> float:
    """现金分红后调整每股成本。"""
    return max(0.0, avg_cost - dividend_per_share)


class CorporateActionStore:
    """公司行为存储：按 code + 日期范围查询。"""

    def __init__(self, actions: Optional[List[CorporateAction]] = None):
        self._actions = sorted(actions or [], key=lambda a: (a.code, a.date))

    @classmethod
    def from_records(cls, records: List[dict]) -> "CorporateActionStore":
        """从规范化字典记录构造 Store。

        生产数据必须已经完成 PIT/来源校验；这里仅负责类型归一化，
        不从 adjustFactor 或价格跳变推断公司行为。
        """
        actions = []
        for row in records:
            action_type = row.get("action_type")
            if isinstance(action_type, ActionType):
                kind = action_type
            else:
                kind = ActionType(str(action_type))
            date = row.get("date")
            if isinstance(date, datetime.datetime):
                date = date.date()
            elif isinstance(date, str):
                date = datetime.date.fromisoformat(date[:10])
            actions.append(CorporateAction(
                date=date,
                code=str(row["code"]),
                action_type=kind,
                cash_dividend_per_share=float(row.get("cash_dividend_per_share", 0.0) or 0.0),
                split_ratio=float(row.get("split_ratio", 1.0) or 1.0),
                rights_price=float(row.get("rights_price", 0.0) or 0.0),
                rights_ratio=float(row.get("rights_ratio", 0.0) or 0.0),
            ))
        return cls(actions)

    @classmethod
    def from_file(cls, path: str) -> "CorporateActionStore":
        """从 JSON/CSV/Parquet 规范化事件文件加载公司行为。"""
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"Corporate action file not found: {p}")

        suffix = p.suffix.lower()
        if suffix == ".json":
            with p.open("r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                raise ValueError("Corporate action JSON must be a list of records")
        elif suffix == ".csv":
            records = pl.read_csv(p).to_dicts()
        elif suffix in (".parquet", ".pq"):
            records = pl.read_parquet(p).to_dicts()
        else:
            raise ValueError(
                f"Unsupported corporate action file type: {suffix}; "
                "use .json, .csv or .parquet"
            )
        return cls.from_records(records)

    def query(self, code: str, start_date: datetime.date,
              end_date: datetime.date) -> List[CorporateAction]:
        """查询指定 code 在 [start_date, end_date] 内的所有公司行为。"""
        result = []
        for a in self._actions:
            if a.code > code:
                break
            if a.code == code and start_date <= a.date <= end_date:
                result.append(a)
        return result

    def query_all(self, start_date: datetime.date,
                  end_date: datetime.date) -> List[CorporateAction]:
        """查询所有 code 在 [start_date, end_date] 内的公司行为。"""
        return [a for a in self._actions
                if start_date <= a.date <= end_date]
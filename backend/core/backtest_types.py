from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol
import datetime
import polars as pl


@dataclass
class FeeConfig:
    """Fee configuration for backtest execution.
    
    Default values are RESEARCH DEFAULTS, not historical actual rates.
    For production backtests, use historical_fee_schedule(date) extension.
    """
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001
    date_start: Optional[datetime.date] = None


@dataclass
class FeeSchedule:
    entries: list

    def __post_init__(self):
        if not self.entries:
            raise ValueError("FeeSchedule requires at least one entry")
        self.entries = sorted(self.entries, key=lambda e: e.date_start or datetime.date.min)

    def get_fee_config(self, date: datetime.date) -> FeeConfig:
        result = self.entries[0]
        for entry in self.entries:
            if entry.date_start is None or entry.date_start <= date:
                result = entry
            else:
                break
        return result


@dataclass
class ExecutionConfig:
    """Execution configuration.
    
    MVP FROZEN VALUES (不可修改):
    - price_mode = "open"
    - order_sequence = "sell_first"
    - cash_reinvestment = "same_cycle"
    - partial_fill_policy = "keep_cash"
    
    Future extension (暂不实现):
    - price_mode: "open" / "vwap" / "close"
    - order_sequence: "sell_first" / "simultaneous"
    - cash_reinvestment: "same_cycle" / "T+2"
    - partial_fill_policy: "keep_cash" / "redistribute"
    """
    price_mode: str = "open"
    order_sequence: str = "sell_first"
    cash_reinvestment: str = "same_cycle"
    partial_fill_policy: str = "keep_cash"


MVP_EXECUTION_CONFIG = ExecutionConfig(
    price_mode="open",
    order_sequence="sell_first",
    cash_reinvestment="same_cycle",
    partial_fill_policy="keep_cash",
)

Allocator = Callable[[list[str], datetime.date], dict[str, float]]


def equal_weight_allocator(codes: list[str], signal_date: datetime.date) -> dict[str, float]:
    """MVP 默认等权分配器。"""
    if not codes:
        return {}
    weight = 1.0 / len(codes)
    return {code: weight for code in codes}


def top_n_equal_weight_allocator(n: int) -> Allocator:
    """Top-N 等权分配器（实验契约：N = 最终持仓数量上限）。

    确定性排序：code 升序——公式无 score 时以 code asc 为唯一序，
    杜绝 PYTHONHASHSEED 类跨进程不确定性混入组合构建。
    """
    def _alloc(codes: list[str], signal_date: datetime.date) -> dict[str, float]:
        picked = sorted(codes)[:n] if n and n > 0 else []
        if not picked:
            return {}
        w = 1.0 / len(picked)
        return {c: w for c in picked}
    return _alloc


class RankingFn(Protocol):
    """Ranking 函数签名：对候选集评分并排序。

    输入：frame 包含 ≤ signal_date 的 daily 数据
    输出：pl.DataFrame[code, score]，按 score desc 排序，相同 score 按 code asc tie-break
    """
    def __call__(self, frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
        ...


def top_n_ranked_allocator(ranking_fn: 'RankingFn', n: int) -> Allocator:
    """基于 ranking 评分的 Top-N 等权分配器。

    确定性保证：ranking_fn 内部已做 tie-break（相同 score → code asc），
    allocator 仅负责按 score 降序取前 N 并等权分配。

    注意：此 allocator 接收的 codes 参数为 eligibility pre-filter 结果，
    ranking_fn 自行从 frame 中计算 score，不依赖 codes 参数的顺序。
    """
    def _alloc(codes: list[str], signal_date: datetime.date,
               _frame=None, _ranking_fn=ranking_fn, _n=n) -> dict[str, float]:
        if _frame is None or _frame.is_empty():
            return {}
        ranked = _ranking_fn(_frame, signal_date)
        if ranked.is_empty():
            return {}
        picked = ranked["code"].to_list()[:_n]
        if not picked:
            return {}
        w = 1.0 / len(picked)
        return {c: w for c in picked}
    return _alloc


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


@dataclass
class Position:
    """持仓结构（支持 T+1 可卖/冻结语义）。
    
    Attributes:
        code: 股票代码
        total_qty: 总持仓数量
        available_qty: 当日可卖数量（T 日旧仓 + 历史已解冻）
        frozen_qty: 冻结数量（T+1 新买入，T+2 解冻）
        avg_cost: 平均成本
        market_value: 市值
    """
    code: str
    total_qty: int
    available_qty: int
    frozen_qty: int
    avg_cost: float
    market_value: float


class BacktestDataIntegrityError(RuntimeError):
    """回测数据完整性错误：持仓股在估值日缺失 raw close 等不可恢复缺口。

    与"停牌"严格区分——数据缺失不允许静默跳过或沿用陈旧市值。
    """
    def __init__(self, code: str, date):
        self.code = code
        self.date = date
        super().__init__(
            f"Data integrity error: 持仓 {code} 在 {date} 缺失有效 raw close，"
            f"拒绝估值（数据缺失 ≠ 停牌）"
        )


class BacktestLedgerError(RuntimeError):
    """回测账本恒等式违反：cash 为负 / equity != cash + positions_value 等。"""
    def __init__(self, message: str):
        super().__init__(f"Ledger invariant violated: {message}")
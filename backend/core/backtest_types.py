from dataclasses import dataclass
from typing import Callable, Optional
import datetime


@dataclass
class FeeConfig:
    """Fee configuration for backtest execution.
    
    Default values are RESEARCH DEFAULTS, not historical actual rates.
    For production backtests, use historical_fee_schedule(date) extension.
    """
    commission_rate: float = 0.00025      # 佣金费率
    commission_min: float = 5.0           # 最低佣金
    stamp_tax_rate: float = 0.0005        # 印花税费率（单边卖出）
    transfer_fee_rate: float = 0.00001    # 过户费费率（双向）


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
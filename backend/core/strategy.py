"""通用回测策略定义（P0）。

StrategyDefinition 只描述策略，不执行策略。
执行仍由现有 SelectionEngine / BacktestEngine / ExecutionEngine 负责。

第一版支持的策略层能力：
- Universe: ALL_A / INDEX
- Entry / Exit: condition / cross_above / cross_below
- Position sizing: equal_weight / top_n_equal_weight
- Rebalance: daily / weekly
- Mode: target_portfolio / event_driven
- Execution: 复用现有 ExecutionConfig
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal, Optional

from .backtest_types import ExecutionConfig, MVP_EXECUTION_CONFIG


UniverseType = Literal["all_a", "index"]
SignalTrigger = Literal["condition", "cross_above", "cross_below"]
StrategyMode = Literal["target_portfolio", "event_driven"]
SizingMethod = Literal["equal_weight", "top_n_equal_weight"]
RebalanceFrequency = Literal["daily", "weekly"]


@dataclass(frozen=True)
class UniverseDefinition:
    """策略允许交易的股票集合。

    type='all_a' 时 index_id 必须为空；
    type='index' 时 index_id 必须提供。
    """

    type: UniverseType = "all_a"
    index_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.type not in ("all_a", "index"):
            raise ValueError(f"unsupported universe type: {self.type!r}")

        if self.type == "index":
            if not self.index_id or not self.index_id.strip():
                raise ValueError("index universe requires index_id")
        elif self.index_id is not None:
            raise ValueError("all_a universe must not specify index_id")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SignalDefinition:
    """一个 Entry/Exit 信号。

    condition:
        每个 as-of 日直接判断公式真假。
    cross_above:
        A[t] > B[t] 且 A[t-1] <= B[t-1]。
    cross_below:
        A[t] < B[t] 且 A[t-1] >= B[t-1]。
    """

    condition: str
    trigger: SignalTrigger = "condition"
    timeframe: str = "D"

    def __post_init__(self) -> None:
        if not self.condition or not self.condition.strip():
            raise ValueError("signal condition must not be empty")
        if self.trigger not in ("condition", "cross_above", "cross_below"):
            raise ValueError(f"unsupported signal trigger: {self.trigger!r}")
        if not self.timeframe or self.timeframe.upper() not in ("D", "W", "M"):
            raise ValueError(f"unsupported signal timeframe: {self.timeframe!r}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PositionSizingDefinition:
    """目标组合的持仓规模定义。"""

    method: SizingMethod = "equal_weight"
    max_positions: Optional[int] = None

    def __post_init__(self) -> None:
        if self.method not in ("equal_weight", "top_n_equal_weight"):
            raise ValueError(f"unsupported sizing method: {self.method!r}")

        if self.method == "top_n_equal_weight":
            if self.max_positions is None or self.max_positions <= 0:
                raise ValueError(
                    "top_n_equal_weight requires max_positions > 0"
                )
        elif self.max_positions is not None and self.max_positions <= 0:
            raise ValueError("max_positions must be > 0 when provided")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RebalanceDefinition:
    frequency: RebalanceFrequency = "daily"

    def __post_init__(self) -> None:
        if self.frequency not in ("daily", "weekly"):
            raise ValueError(f"unsupported rebalance frequency: {self.frequency!r}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyDefinition:
    """统一通用回测策略契约。

    target_portfolio:
        每次调仓重新生成完整目标组合；没有进入目标组合的持仓会被退出。

    event_driven:
        Entry/Exit 是事件信号；无信号时保持当前持仓，不因未出现在 Entry
        集合中而自动清仓。
    """

    universe: UniverseDefinition
    entry: SignalDefinition
    exit: Optional[SignalDefinition] = None
    sizing: PositionSizingDefinition = PositionSizingDefinition()
    rebalance: RebalanceDefinition = RebalanceDefinition()
    mode: StrategyMode = "target_portfolio"
    execution: ExecutionConfig = MVP_EXECUTION_CONFIG
    name: Optional[str] = None

    def __post_init__(self) -> None:
        if self.mode not in ("target_portfolio", "event_driven"):
            raise ValueError(f"unsupported strategy mode: {self.mode!r}")

        if self.mode == "event_driven" and self.exit is None:
            raise ValueError("event_driven strategy requires an exit signal")

        if not isinstance(self.execution, ExecutionConfig):
            raise TypeError("execution must be an ExecutionConfig")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyDefinition":
        if not isinstance(data, dict):
            raise TypeError("strategy definition must be a dict")

        universe_data = data.get("universe")
        entry_data = data.get("entry")
        if not isinstance(universe_data, dict):
            raise ValueError("strategy.universe is required")
        if not isinstance(entry_data, dict):
            raise ValueError("strategy.entry is required")

        exit_data = data.get("exit")
        sizing_data = data.get("sizing") or {}
        rebalance_data = data.get("rebalance") or {}
        execution_data = data.get("execution")

        execution = (
            MVP_EXECUTION_CONFIG
            if execution_data is None
            else ExecutionConfig(**execution_data)
        )

        return cls(
            universe=UniverseDefinition(**universe_data),
            entry=SignalDefinition(**entry_data),
            exit=SignalDefinition(**exit_data) if exit_data is not None else None,
            sizing=PositionSizingDefinition(**sizing_data),
            rebalance=RebalanceDefinition(**rebalance_data),
            mode=data.get("mode", "target_portfolio"),
            execution=execution,
            name=data.get("name"),
        )


__all__ = [
    "UniverseDefinition",
    "SignalDefinition",
    "PositionSizingDefinition",
    "RebalanceDefinition",
    "StrategyDefinition",
]

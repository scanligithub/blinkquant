"""Unified, reproducible backtest configuration contract."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime
from typing import Any

from .strategy import StrategyDefinition


@dataclass(frozen=True)
class BacktestConfig:
    """Complete configuration required to reproduce one backtest run."""

    start_date: datetime.date
    end_signal_date: datetime.date
    initial_cash: float
    strategy: StrategyDefinition
    min_listing_days: int = 0
    exclude_st: bool = False
    historical_fees: bool = True

    def __post_init__(self) -> None:
        if self.start_date > self.end_signal_date:
            raise ValueError("start_date must be <= end_signal_date")
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be > 0")
        if self.min_listing_days < 0:
            raise ValueError("min_listing_days must be >= 0")
        if not isinstance(self.strategy, StrategyDefinition):
            raise TypeError("strategy must be a StrategyDefinition")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["start_date"] = self.start_date.isoformat()
        data["end_signal_date"] = self.end_signal_date.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BacktestConfig":
        if not isinstance(data, dict):
            raise TypeError("backtest config must be a dict")
        strategy_data = data.get("strategy")
        if not isinstance(strategy_data, dict):
            raise ValueError("backtest_config.strategy is required")
        return cls(
            start_date=datetime.date.fromisoformat(str(data["start_date"])),
            end_signal_date=datetime.date.fromisoformat(str(data["end_signal_date"])),
            initial_cash=float(data["initial_cash"]),
            strategy=StrategyDefinition.from_dict(strategy_data),
            min_listing_days=int(data.get("min_listing_days", 0)),
            exclude_st=bool(data.get("exclude_st", False)),
            historical_fees=bool(data.get("historical_fees", True)),
        )


__all__ = ["BacktestConfig"]

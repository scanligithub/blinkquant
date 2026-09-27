"""Unified, reproducible backtest configuration contract."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime
from typing import Any, Literal

from .backtest_types import FeeConfig
from .strategy import StrategyDefinition


@dataclass(frozen=True)
class FeePolicy:
    """Public fee-selection contract."""
    mode: Literal["historical", "fixed"] = "historical"
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001

    def __post_init__(self) -> None:
        if self.mode not in ("historical", "fixed"):
            raise ValueError("fee_policy.mode must be historical or fixed")
        for name in ("commission_rate", "commission_min", "stamp_tax_rate", "transfer_fee_rate"):
            if getattr(self, name) < 0:
                raise ValueError(f"fee_policy.{name} must be >= 0")

    @classmethod
    def from_dict(cls, data: Any) -> "FeePolicy":
        if data is None:
            return cls()
        if not isinstance(data, dict):
            raise TypeError("fee_policy must be a dict")
        return cls(
            mode=str(data.get("mode", "historical")),
            commission_rate=float(data.get("commission_rate", 0.00025)),
            commission_min=float(data.get("commission_min", 5.0)),
            stamp_tax_rate=float(data.get("stamp_tax_rate", 0.0005)),
            transfer_fee_rate=float(data.get("transfer_fee_rate", 0.00001)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "commission_rate": self.commission_rate,
            "commission_min": self.commission_min,
            "stamp_tax_rate": self.stamp_tax_rate,
            "transfer_fee_rate": self.transfer_fee_rate,
        }

    def fixed_fee_config(self) -> FeeConfig:
        return FeeConfig(
            commission_rate=self.commission_rate,
            commission_min=self.commission_min,
            stamp_tax_rate=self.stamp_tax_rate,
            transfer_fee_rate=self.transfer_fee_rate,
        )


@dataclass(frozen=True)
class BacktestConfig:
    """Complete configuration required to reproduce one backtest run."""
    start_date: datetime.date
    end_signal_date: datetime.date
    initial_cash: float
    strategy: StrategyDefinition
    min_listing_days: int = 0
    exclude_st: bool = False
    historical_fees: bool | None = None
    fee_policy: FeePolicy | None = None

    def __post_init__(self) -> None:
        if self.start_date > self.end_signal_date:
            raise ValueError("start_date must be <= end_signal_date")
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be > 0")
        if self.min_listing_days < 0:
            raise ValueError("min_listing_days must be >= 0")
        if not isinstance(self.strategy, StrategyDefinition):
            raise TypeError("strategy must be a StrategyDefinition")
        policy = self.fee_policy
        if policy is None:
            policy = FeePolicy(mode="historical" if self.historical_fees is not False else "fixed")
        elif not isinstance(policy, FeePolicy):
            raise TypeError("fee_policy must be a FeePolicy")
        if self.historical_fees is not None:
            expected = "historical" if self.historical_fees else "fixed"
            if policy.mode != expected:
                raise ValueError("historical_fees conflicts with fee_policy.mode; use fee_policy")
        object.__setattr__(self, "fee_policy", policy)
        object.__setattr__(self, "historical_fees", policy.mode == "historical")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["start_date"] = self.start_date.isoformat()
        data["end_signal_date"] = self.end_signal_date.isoformat()
        data["fee_policy"] = self.fee_policy.to_dict()
        data["historical_fees"] = self.historical_fees
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BacktestConfig":
        if not isinstance(data, dict):
            raise TypeError("backtest config must be a dict")
        strategy_data = data.get("strategy")
        if not isinstance(strategy_data, dict):
            raise ValueError("backtest_config.strategy is required")
        raw_policy = data.get("fee_policy")
        policy = FeePolicy.from_dict(raw_policy) if raw_policy is not None else FeePolicy(mode="historical" if bool(data.get("historical_fees", True)) else "fixed")
        return cls(
            start_date=datetime.date.fromisoformat(str(data["start_date"])),
            end_signal_date=datetime.date.fromisoformat(str(data["end_signal_date"])),
            initial_cash=float(data["initial_cash"]),
            strategy=StrategyDefinition.from_dict(strategy_data),
            min_listing_days=int(data.get("min_listing_days", 0)),
            exclude_st=bool(data.get("exclude_st", False)),
            fee_policy=policy,
        )


__all__ = ["BacktestConfig", "FeePolicy"]
"""RQAlpha Differential Validator for BlinkQuant."""

import datetime
import json
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from enum import Enum

import polars as pl

from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from core.data_manager import data_manager, DataManager
from core.engine import selection_engine
from core.signal_trace import SignalTraceData, CodeTrace, AtomTrace


class DiffCategory(Enum):
    """Differential classification categories."""
    PRICE_SEMANTIC = "PRICE_SEMANTIC"
    FEE_MODEL = "FEE_MODEL"
    CA_ORDERING = "CA_ORDERING"
    LOT_SIZE = "LOT_SIZE"
    T_PLUS_ONE = "T_PLUS_ONE"
    PARTIAL_FILL = "PARTIAL_FILL"
    DATA_MISSING = "DATA_MISSING"
    ROUNDING = "ROUNDING"
    TRUE_MISMATCH = "TRUE_MISMATCH"


@dataclass
class DiffDetail:
    """Single differential detail."""
    level: str  # SIGNAL, ORDER, TRADE, ACCOUNT
    category: str
    date: str  # ISO date
    code: str
    side: Optional[str] = None
    blinkquant: Dict[str, Any] = field(default_factory=dict)
    rqalpha: Dict[str, Any] = field(default_factory=dict)
    diff: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DifferentialReport:
    """Complete differential validation report."""
    schema_version: str = "1.0.0"
    generated_at: str = ""
    engine_versions: Dict[str, str] = field(default_factory=dict)
    scope: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)
    details: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(',', ':'), default=str)

    def save_json(self, path: Union[str, Path]) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")


class DifferentialValidator:
    """
    Compares BlinkQuant results against RQAlpha.

    Known semantic differences are classified, not counted as mismatches.
    """

    # Categories that are known semantic differences (excluded from mismatch count)
    SEMANTIC_CATEGORIES = {
        DiffCategory.PRICE_SEMANTIC,
        DiffCategory.T_PLUS_ONE,
    }

    def __init__(
        self,
        formula: str = "CLOSE > MA(CLOSE, 20)",
        start_date: datetime.date = datetime.date(2024, 1, 2),
        end_signal_date: datetime.date = datetime.date(2024, 3, 29),
        initial_cash: float = 10_000_000,
        rebalance_freq: str = "weekly",
        top_n: int = 20,
        universe_codes: Optional[List[str]] = None,
        hf_repo: str = "scanli/stocka-data",
        hf_token: Optional[str] = None,
    ):
        self.formula = formula
        self.start_date = start_date
        self.end_signal_date = end_signal_date
        self.initial_cash = initial_cash
        self.rebalance_freq = rebalance_freq
        self.top_n = top_n
        self.universe_codes = universe_codes
        self.hf_repo = hf_repo
        self.hf_token = hf_token

        # Results storage
        self.blinkquant_result = None
        self.rqalpha_result = None
        self.details: List[DiffDetail] = []

    def run_blinkquant(self) -> None:
        """Run BlinkQuant backtest."""
        # Load data
        dm = DataManager()
        if self.hf_token:
            dm.load_hf_data(self.hf_repo, token=self.hf_token)
        else:
            dm.load_hf_data(self.hf_repo)

        data_manager.df_daily = dm.df_daily
        data_manager.df_weekly = dm.df_weekly
        data_manager.df_monthly = dm.df_monthly
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()
        data_manager._resample_all()
        selection_engine._set_cache.clear()

        # Build engine
        calendar = TradingCalendar()
        calendar.set_trade_dates(dm.get_trade_dates())
        raw_store = RawPriceStore(hf_repo_id=self.hf_repo, hf_token=self.hf_token)
        fee_schedule = load_fee_schedule("config/fee_schedule.yaml")
        allocator = top_n_equal_weight_allocator(self.top_n)

        engine = BacktestEngine(
            calendar=calendar,
            selection_engine=selection_engine,
            raw_price_store=raw_store,
            fee_config=FeeConfig(),
            execution_config=MVP_EXECUTION_CONFIG,
            allocator=allocator,
        )

        self.blinkquant_result = engine.run(
            formula=self.formula,
            start_date=self.start_date,
            end_signal_date=self.end_signal_date,
            initial_cash=self.initial_cash,
            rebalance_freq=self.rebalance_freq,
            fee_schedule=fee_schedule,
        )

    def run_rqalpha(self) -> None:
        """Run RQAlpha backtest (placeholder - needs RQAlpha environment)."""
        # This is a placeholder. Actual implementation requires RQAlpha installation.
        # For now, we generate a mock structure to show the comparison framework.
        self.rqalpha_result = {
            "trades": [],
            "equity_curve": [],
            "positions_daily": [],
            "account": [],
        }

    def compare(self) -> DifferentialReport:
        """Run comparison and generate report."""
        if self.blinkquant_result is None:
            self.run_blinkquant()
        if self.rqalpha_result is None:
            self.run_rqalpha()

        self.details = []

        # Compare signals
        self._compare_signals()

        # Compare orders
        self._compare_orders()

        # Compare trades
        self._compare_trades()

        # Compare account state
        self._compare_account_state()

        # Build summary
        summary = self._build_summary()

        report = DifferentialReport(
            engine_versions={
                "blinkquant": "v1.0.3-9-g43547cc",
                "rqalpha": "0.x.x (placeholder)",
            },
            scope={
                "universe": self.universe_codes or "CSI300 subset",
                "start_date": self.start_date.isoformat(),
                "end_signal_date": self.end_signal_date.isoformat(),
                "strategy": self.formula,
                "rebalance": self.rebalance_freq,
                "top_n": self.top_n,
                "initial_cash": self.initial_cash,
            },
            summary=summary,
            details=[d.to_dict() for d in self.details],
        )

        return report

    def _compare_signals(self) -> None:
        """Compare signal generation."""
        # Placeholder for signal comparison
        pass

    def _compare_orders(self) -> None:
        """Compare order intents."""
        pass

    def _compare_trades(self) -> None:
        """Compare trade execution."""
        pass

    def _compare_account_state(self) -> None:
        """Compare daily account state."""
        pass

    def _build_summary(self) -> Dict[str, Any]:
        """Build comparison summary."""
        total_details = len(self.details)
        by_category = {}
        for d in self.details:
            cat = d.category
            by_category[cat] = by_category.get(cat, 0) + 1

        # Exclude semantic categories from mismatch count
        true_mismatches = sum(
            v for k, v in by_category.items()
            if k not in [c.value for c in self.SEMANTIC_CATEGORIES]
        )

        return {
            "total_signals_compared": 0,
            "total_orders_compared": 0,
            "total_trades_compared": 0,
            "total_account_days_compared": 0,
            "mismatches_by_category": {k: v for k, v in by_category.items()},
            "true_mismatch_count": true_mismatches,
            "match_rate": 1.0 - (true_mismatches / max(total_details, 1)),
        }


def run_differential_validation(
    output_path: Union[str, Path],
    **kwargs,
) -> DifferentialReport:
    """Convenience function to run full differential validation."""
    validator = DifferentialValidator(**kwargs)
    report = validator.compare()
    report.save_json(output_path)
    return report
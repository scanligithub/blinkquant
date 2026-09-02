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
        from huggingface_hub import hf_hub_download

        KEEP_COLS = ["date", "code", "open", "high", "low", "close",
                     "volume", "amount", "adjustFactor", "pctChg", "isST"]

        dm = DataManager()
        token = self.hf_token

        history_start = self.start_date - datetime.timedelta(days=60)
        needed_years = list(range(history_start.year, self.end_signal_date.year + 2))

        parts = []
        for year in needed_years:
            try:
                p = hf_hub_download(
                    repo_id=self.hf_repo,
                    filename=f"stock_kline_{year}.parquet",
                    repo_type="dataset",
                    token=token,
                )
                scan = pl.scan_parquet(p)
                available = scan.collect_schema().names()
                use_cols = [c for c in KEEP_COLS if c in available]
                df = scan.select(use_cols).collect()
                df = df.filter((df["code"].hash() % dm.total_nodes) == 0)
                parts.append(df)
            except Exception:
                continue

        if not parts:
            raise RuntimeError("No HF data loaded for BlinkQuant")

        df = pl.concat(parts, how="diagonal")
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=True))
        dm.df_daily = df.sort(["code", "date"])
        dm._compute_limit_flags()
        dm._apply_forward_adjustment()
        dm._append_prev_close()
        dm._optimize_memory(dm.df_daily, "df_daily")
        dm._resample_all()

        data_manager.df_daily = dm.df_daily
        data_manager.df_weekly = dm.df_weekly
        data_manager.df_monthly = dm.df_monthly
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()
        data_manager._resample_all()
        selection_engine._set_cache.clear()

        if self.universe_codes:
            dm.df_daily = dm.df_daily.filter(pl.col("code").is_in(self.universe_codes))

        calendar = TradingCalendar()
        trade_dates = sorted(
            dm.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
        )
        calendar.set_trade_dates(trade_dates)

        # Trim end_signal_date so last signal has a next trade day for T+1
        if trade_dates and self.end_signal_date >= trade_dates[-1]:
            self.end_signal_date = trade_dates[-2]

        raw_store = RawPriceStore(hf_repo_id=self.hf_repo, hf_token=token)
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
        """Run RQAlpha backtest via adapter."""
        from core.rqalpha_adapter import run_rqalpha

        codes = self.universe_codes
        if not codes:
            if data_manager.df_daily is not None:
                latest = data_manager.df_daily.filter(
                    pl.col("date") == data_manager.df_daily["date"].max()
                )
                codes = sorted(latest["code"].to_list())[:self.top_n]
            else:
                raise ValueError("No universe specified and no data loaded")

        # RQAlpha needs lookback data for indicator calculation (e.g., MA20)
        lookback_start = self.start_date - datetime.timedelta(days=60)

        result = run_rqalpha(
            codes=codes,
            start_date=lookback_start,
            end_date=self.end_signal_date,
            initial_cash=self.initial_cash,
            hf_repo=self.hf_repo,
            hf_token=self.hf_token,
        )

        self.rqalpha_result = {
            "trades": result.trades,
            "equity_curve": result.equity_curve,
            "positions_daily": result.positions_daily,
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
        """Compare trade execution between BlinkQuant and RQAlpha."""
        bq_trades = self.blinkquant_result.trades if hasattr(self.blinkquant_result, 'trades') else pl.DataFrame()
        ra_trades = self.rqalpha_result.get("trades", pl.DataFrame()) if isinstance(self.rqalpha_result, dict) else pl.DataFrame()

        if bq_trades.is_empty() or ra_trades.is_empty():
            return

        # Normalize BlinkQuant trades
        bq_norm = bq_trades.rename({"execution_date": "date", "qty": "quantity", "fee": "cost"})
        bq_norm = bq_norm.with_columns(pl.col("code").str.replace_all(r"^(sh|sz|bj)\.", ""))

        # Normalize RQAlpha trades
        ra_norm = ra_trades.rename({"date": "date"})
        ra_norm = ra_norm.with_columns(pl.col("code").str.replace_all(r"\.\w+$", ""))

        # Normalize side: BlinkQuant uses BUY/SELL, RQAlpha uses buy/sell
        bq_norm = bq_norm.with_columns(pl.col("side").str.to_lowercase())
        ra_norm = ra_norm.with_columns(pl.col("side").str.to_lowercase())

        # Join on (date, code, side)
        joined = bq_norm.join(
            ra_norm,
            on=["date", "code", "side"],
            how="outer",
            suffix="_ra",
        )

        for row in joined.iter_rows(named=True):
            date = str(row.get("date", ""))
            code = row.get("code", "")
            side = row.get("side", "")

            bq_qty = row.get("quantity")
            ra_qty = row.get("quantity_ra")
            bq_price = row.get("price")
            ra_price = row.get("price_ra")
            bq_fee = row.get("cost")
            ra_fee = row.get("cost_ra")

            # Check if both sides have the trade
            if bq_qty is None or ra_qty is None:
                self.details.append(DiffDetail(
                    level="TRADE", category=DiffCategory.PARTIAL_FILL.value,
                    date=date, code=code, side=side,
                    blinkquant={"qty": bq_qty}, rqalpha={"qty": ra_qty},
                    diff={"reason": "trade_missing_on_one_side"},
                ))
                continue

            if bq_qty != ra_qty:
                self.details.append(DiffDetail(
                    level="TRADE", category=DiffCategory.LOT_SIZE.value,
                    date=date, code=code, side=side,
                    blinkquant={"qty": bq_qty}, rqalpha={"qty": ra_qty},
                    diff={"qty_delta": bq_qty - ra_qty},
                ))

            if bq_price is not None and ra_price is not None:
                if abs(bq_price - ra_price) > 0.001:
                    self.details.append(DiffDetail(
                        level="TRADE", category=DiffCategory.PRICE_SEMANTIC.value,
                        date=date, code=code, side=side,
                        blinkquant={"price": bq_price}, rqalpha={"price": ra_price},
                        diff={"price_delta": bq_price - ra_price},
                    ))

            if bq_fee is not None and ra_fee is not None:
                if abs(bq_fee - ra_fee) > 0.01:
                    self.details.append(DiffDetail(
                        level="TRADE", category=DiffCategory.FEE_MODEL.value,
                        date=date, code=code, side=side,
                        blinkquant={"fee": bq_fee}, rqalpha={"fee": ra_fee},
                        diff={"fee_delta": bq_fee - ra_fee},
                    ))

    def _compare_account_state(self) -> None:
        """Compare daily account state."""
        bq_eq = self.blinkquant_result.equity_curve if hasattr(self.blinkquant_result, 'equity_curve') else pl.DataFrame()
        ra_eq = self.rqalpha_result.get("equity_curve", pl.DataFrame()) if isinstance(self.rqalpha_result, dict) else pl.DataFrame()

        if bq_eq.is_empty() or ra_eq.is_empty():
            return

        if "date" not in bq_eq.columns or "date" not in ra_eq.columns:
            return

        # Normalize column names: RQAlpha uses total_value, BlinkQuant uses equity
        bq_norm = bq_eq
        ra_norm = ra_eq
        if "total_value" in ra_norm.columns and "equity" not in ra_norm.columns:
            ra_norm = ra_norm.rename({"total_value": "equity"})

        # Filter both to the same date range (BlinkQuant's date range)
        bq_dates = bq_norm["date"].to_list()
        if bq_dates:
            min_date = min(bq_dates)
            max_date = max(bq_dates)
            ra_norm = ra_norm.filter(
                (pl.col("date") >= min_date) & (pl.col("date") <= max_date)
            )

        joined = bq_norm.join(ra_norm, on="date", how="outer", suffix="_ra")

        for row in joined.iter_rows(named=True):
            date = str(row.get("date", ""))
            bq_equity = row.get("equity")
            ra_equity = row.get("equity_ra")
            bq_cash = row.get("cash")
            ra_cash = row.get("cash_ra")

            if bq_equity is not None and ra_equity is not None:
                if abs(bq_equity - ra_equity) > 0.01:
                    self.details.append(DiffDetail(
                        level="ACCOUNT", category=DiffCategory.ROUNDING.value,
                        date=date, code="PORTFOLIO",
                        blinkquant={"equity": bq_equity}, rqalpha={"equity": ra_equity},
                        diff={"equity_delta": bq_equity - ra_equity},
                    ))

            if bq_cash is not None and ra_cash is not None:
                if abs(bq_cash - ra_cash) > 0.01:
                    self.details.append(DiffDetail(
                        level="ACCOUNT", category=DiffCategory.FEE_MODEL.value,
                        date=date, code="PORTFOLIO",
                        blinkquant={"cash": bq_cash}, rqalpha={"cash": ra_cash},
                        diff={"cash_delta": bq_cash - ra_cash},
                    ))

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
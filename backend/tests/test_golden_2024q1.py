from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

GOLDEN_DIR = Path(__file__).parent.parent.parent / "tests" / "golden" / "2024q1"


def _load_golden(name: str):
    path = GOLDEN_DIR / name
    if not path.exists():
        pytest.skip(f"Golden artifact not found: {path}")
    if name.endswith(".parquet"):
        return pl.read_parquet(path)
    elif name.endswith(".json"):
        with open(path) as f:
            return json.load(f)
    raise ValueError(f"Unsupported golden file extension: {name}")


class TestGoldenEquityCurve:
    def test_equity_curve_row_count(self):
        golden = _load_golden("equity_curve.parquet")
        assert golden.height >= 50

    def test_equity_curve_dates_monotonic(self):
        golden = _load_golden("equity_curve.parquet")
        dates = golden["date"].to_list()
        assert dates == sorted(dates)

    def test_equity_curve_first_row(self):
        golden = _load_golden("equity_curve.parquet")
        first = golden.head(1).to_dicts()[0]
        assert first["cash"] == pytest.approx(10_000_000, abs=1e-6)
        assert first["positions_value"] == 0

    def test_equity_curve_cash_never_negative(self):
        golden = _load_golden("equity_curve.parquet")
        assert golden.select(pl.col("cash").min()).item() >= -1e-6

    def test_equity_curve_equity_invariant(self):
        golden = _load_golden("equity_curve.parquet")
        max_diff = golden.select(
            (pl.col("equity") - pl.col("cash") - pl.col("positions_value")).abs().max()
        ).item()
        assert max_diff < 1e-6


class TestGoldenTrades:
    def test_trades_have_all_columns(self):
        golden = _load_golden("trades.parquet")
        expected = {"signal_date", "execution_date", "code", "side", "qty", "price", "fee"}
        assert expected.issubset(set(golden.columns))

    def test_trades_t1_ordering(self):
        golden = _load_golden("trades.parquet")
        if golden.height == 0:
            pytest.skip("No trades")
        for row in golden.iter_rows(named=True):
            assert row["execution_date"] > row["signal_date"]

    def test_trades_lot_size_compliance(self):
        golden = _load_golden("trades.parquet")
        # Only BUY must comply with 100-lot rule; SELL in A-shares allows odd lots
        buys = golden.filter(pl.col("side") == "BUY")
        violations = buys.filter(pl.col("qty") % 100 != 0)
        assert violations.height == 0, f"BUY lot size violations: {violations}"

    def test_trades_positive_fee(self):
        golden = _load_golden("trades.parquet")
        assert golden.select(pl.col("fee").min()).item() >= 0

    def test_trades_no_same_day_buy_sell(self):
        golden = _load_golden("trades.parquet")
        if golden.height == 0:
            pytest.skip("No trades")
        grouped = golden.group_by(["execution_date", "code"]).agg(pl.col("side").n_unique().alias("sides"))
        violations = grouped.filter(pl.col("sides") > 1)
        assert violations.height == 0, f"Same-day buy+sell: {violations}"


class TestGoldenPositions:
    def test_positions_have_all_columns(self):
        golden = _load_golden("positions_daily.parquet")
        expected = {"date", "code", "qty", "cost", "market_value"}
        assert expected.issubset(set(golden.columns))

    def test_positions_qty_positive(self):
        golden = _load_golden("positions_daily.parquet")
        assert golden.select(pl.col("qty").min()).item() > 0


class TestGoldenMetrics:
    def test_metrics_have_required_keys(self):
        golden = _load_golden("metrics.json")
        required = {"total_return", "cagr", "sharpe", "max_drawdown", "total_days"}
        assert required.issubset(set(golden.keys()))

    def test_metrics_total_days(self):
        golden = _load_golden("metrics.json")
        assert golden["total_days"] >= 50


class TestGoldenDiagnostics:
    def test_diagnostics_have_required_keys(self):
        golden = _load_golden("diagnostics.json")
        required = {"rej_counters", "intents_total", "partial_fill_count", "carried_events"}
        assert required.issubset(set(golden.keys()))

    def test_no_invariant_violations(self):
        golden = _load_golden("diagnostics.json")
        assert golden.get("negative_cash_count", 0) == 0
        assert golden.get("accounting_invariant_violations", 0) == 0

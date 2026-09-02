"""
PoC-2: Fee Mapping — RQAlpha transaction_cost vs blinkquant FeeSchedule.

Verifies that RQAlpha's native Trade.transaction_cost lifecycle
can be losslessly mapped to blinkquant's FeeSchedule semantics.

Key finding: RQAlpha 1d frequency fills at CLOSE price (not open).

PASS criteria:
  1. BUY commission == max(fill_price * qty * rate, min_commission)
  2. SELL commission == max(fill_price * qty * rate, min_commission)
  3. BUY stamp_tax == 0
  4. SELL stamp_tax == fill_price * qty * tax_rate
  5. minimum commission is order-level (single fill)
  6. cash_after fully consistent
  7. repeated run deterministic
"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile

import polars as pl
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backends.rqalpha.datasource import BlinkquantParquetDataSource


# ======================================================================
# Synthetic data — flat prices for deterministic fee calculation
# ======================================================================

FEE_DATES = [
    datetime.date(2024, 1, 2),
    datetime.date(2024, 1, 3),
    datetime.date(2024, 1, 4),
    datetime.date(2024, 1, 5),
    datetime.date(2024, 1, 8),
    datetime.date(2024, 1, 9),
    datetime.date(2024, 1, 10),
]

FEE_PRICES = {
    "sh.600000": {
        d: (10.00, 10.10, 9.90, 10.05, 200000)
        for d in FEE_DATES
    },
}

# RQAlpha default rates (from StockTransactionCostDecider)
RQALPHA_COMMISSION_RATE = 0.0008
RQALPHA_MIN_COMMISSION = 5.0
RQALPHA_TAX_RATE = 0.0005


def create_fee_parquet(root: str) -> str:
    os.makedirs(root, exist_ok=True)
    basic_df = pl.DataFrame({
        "code": ["sh.600000"],
        "name": ["浦发银行"],
        "industry": ["bank"],
        "industry_name": ["银行"],
        "list_date": [datetime.date(1999, 11, 10)],
        "delist_date": [None],
    })
    basic_df.write_parquet(os.path.join(root, "stock_basic.parquet"))
    cal_df = pl.DataFrame({"date": FEE_DATES})
    cal_df.write_parquet(os.path.join(root, "trading_dates.parquet"))
    rows = []
    for code, price_map in FEE_PRICES.items():
        for dt, (o, h, l, c, v) in price_map.items():
            rows.append({
                "code": code, "date": dt,
                "open": o, "high": h, "low": l, "close": c,
                "volume": v, "total_turnover": c * v,
            })
    pl.DataFrame(rows).write_parquet(os.path.join(root, "stock_kline_2024.parquet"))
    return root


@pytest.fixture(scope="module")
def fee_root():
    tmp = tempfile.mkdtemp(prefix="blink_poc2_")
    root = create_fee_parquet(tmp)
    yield root
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)


def run_fee_test(fee_root, handle_bar_fn,
                 start_date=datetime.date(2024, 1, 2),
                 end_date=datetime.date(2024, 1, 10),
                 init_cash=1000000.0,
                 min_commission=5.0):
    """Run RQAlpha with fee config and return result dict."""
    from rqalpha import main as rqalpha_main
    from rqalpha.utils.config import parse_config
    from rqalpha.environment import Environment

    config = {
        "base": {
            "start_date": start_date,
            "end_date": end_date,
            "accounts": {"STOCK": init_cash},
            "frequency": "1d",
            "benchmark": None,
            "data_bundle_path": fee_root,
            "matching_type": "next_bar",
            "persist_mode": "on_normal_exit",
        },
        "mod": {
            "sys_accounts": {
                "enabled": True,
                "stock_starting_cash": init_cash,
                "capital_ga_tax_rate": 0,
            },
            "sys_simulation": {
                "enabled": True,
                "matching_type": "next_bar",
                "slippage_model": "PriceRatioSlippage",
                "slippage": 0,
                "volume_percent": 1,
                "price_limit": False,
                "inactive_limit": False,
            },
            "sys_transaction_cost": {
                "enabled": True,
                "stock_commission_multiplier": 1.0,
                "tax_multiplier": 1.0,
                "stock_min_commission": min_commission,
                "pit_tax": False,
            },
        },
        "extra": {
            "log_level": "error",
            "force_match_if_no_rules": True,
        },
    }
    config = parse_config(config)

    ds = BlinkquantParquetDataSource(fee_root)
    original_init = Environment.__init__

    def patched_init(self, config, *args, **kwargs):
        original_init(self, config, *args, **kwargs)
        self.data_source = ds

    Environment.__init__ = patched_init
    try:
        result = rqalpha_main.run(
            config,
            user_funcs={"handle_bar": handle_bar_fn} if handle_bar_fn else None,
        )
    finally:
        Environment.__init__ = original_init

    return result


# ======================================================================
# blinkquant FeeSchedule equivalent
# ======================================================================

def blinkquant_fee(price, quantity, side,
                   commission_rate=RQALPHA_COMMISSION_RATE,
                   min_commission=RQALPHA_MIN_COMMISSION,
                   tax_rate=RQALPHA_TAX_RATE):
    """Calculate blinkquant-equivalent fee for comparison."""
    notional = price * quantity
    commission = max(notional * commission_rate, min_commission)
    stamp_tax = notional * tax_rate if side == "SELL" else 0
    return commission, stamp_tax, commission + stamp_tax


# ======================================================================
# 1. Commission below minimum
# ======================================================================

class TestCommissionMinimum:
    def test_commission_below_minimum(self, fee_root):
        """
        BUY 100 @ close(10.05) = 1005 notional
        commission = max(1005 * 0.0008, 5) = max(0.804, 5) = 5.00
        """
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 100)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]
        buy = trades[trades["side"] == "BUY"]
        assert len(buy) == 1

        row = buy.iloc[0]
        fill_price = row["last_price"]

        # RQAlpha commission must equal blinkquant formula
        bq_comm, _, _ = blinkquant_fee(fill_price, 100, "BUY")
        assert row["commission"] == pytest.approx(bq_comm), \
            f"RQAlpha {row['commission']} != blinkquant {bq_comm}"

        # Must be at minimum
        assert row["commission"] == pytest.approx(5.0), \
            f"commission: expected 5.0 (min), got {row['commission']}"

    def test_commission_above_minimum(self, fee_root):
        """
        BUY 10000 @ close(10.05) = 100500 notional
        commission = max(100500 * 0.0008, 5) = max(80.4, 5) = 80.40
        """
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 10000)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]
        buy = trades[trades["side"] == "BUY"]
        assert len(buy) == 1

        row = buy.iloc[0]
        fill_price = row["last_price"]
        expected_commission = fill_price * 10000 * RQALPHA_COMMISSION_RATE

        assert row["commission"] == pytest.approx(expected_commission), \
            f"commission: expected {expected_commission}, got {row['commission']}"

        bq_comm, _, _ = blinkquant_fee(fill_price, 10000, "BUY")
        assert row["commission"] == pytest.approx(bq_comm)

    def test_commission_formula_matches(self, fee_root):
        """Verify commission formula for both BUY and SELL."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 5000)
            elif dt == datetime.date(2024, 1, 4):
                order_shares("sh.600000", -5000)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]

        for _, row in trades.iterrows():
            side = row["side"]
            fill_price = row["last_price"]
            qty = row["last_quantity"]
            bq_comm, _, _ = blinkquant_fee(fill_price, qty, side)
            assert row["commission"] == pytest.approx(bq_comm), \
                f"{side} commission: RQAlpha {row['commission']} != blinkquant {bq_comm}"


# ======================================================================
# 2. Tax: BUY has no stamp_tax, SELL has stamp_tax
# ======================================================================

class TestStampTax:
    def test_buy_no_stamp_tax(self, fee_root):
        """BUY should have commission > 0 but stamp_tax == 0."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 1000)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]
        buy = trades[trades["side"] == "BUY"]
        assert len(buy) == 1
        assert buy.iloc[0]["commission"] > 0
        assert buy.iloc[0]["tax"] == 0

    def test_sell_has_stamp_tax(self, fee_root):
        """SELL should have commission > 0 and stamp_tax > 0."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 1000)
            elif dt == datetime.date(2024, 1, 4):
                order_shares("sh.600000", -1000)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]
        sell = trades[trades["side"] == "SELL"]
        assert len(sell) == 1

        row = sell.iloc[0]
        assert row["commission"] > 0, f"SELL commission: {row['commission']}"
        assert row["tax"] > 0, f"SELL stamp_tax: {row['tax']}"

        bq_comm, bq_tax, _ = blinkquant_fee(
            row["last_price"], row["last_quantity"], "SELL")
        assert row["tax"] == pytest.approx(bq_tax), \
            f"stamp_tax: RQAlpha {row['tax']} != blinkquant {bq_tax}"

    def test_buy_sell_total_cost(self, fee_root):
        """BUY + SELL total transaction cost should match blinkquant."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 1000)
            elif dt == datetime.date(2024, 1, 4):
                order_shares("sh.600000", -1000)

        result = run_fee_test(fee_root, handle_bar)
        trades = result["sys_analyser"]["trades"]

        for _, row in trades.iterrows():
            bq_comm, bq_tax, bq_total = blinkquant_fee(
                row["last_price"], row["last_quantity"], row["side"])
            assert row["transaction_cost"] == pytest.approx(bq_total, abs=0.01), \
                f"{row['side']} total: RQAlpha {row['transaction_cost']} != blinkquant {bq_total}"


# ======================================================================
# 3. Cash consistency
# ======================================================================

class TestCashConsistency:
    def test_cash_after_buy(self, fee_root):
        """Cash after BUY should reflect: init_cash - notional - commission."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 1000)

        result = run_fee_test(fee_root, handle_bar)
        portfolio = result["sys_analyser"]["portfolio"]
        # Portfolio index is DatetimeIndex, need to use .iloc or string
        cash_after = portfolio.iloc[1]["cash"]  # row 1 = 2024-01-03

        trades = result["sys_analyser"]["trades"]
        buy = trades[trades["side"] == "BUY"]
        fill_price = buy.iloc[0]["last_price"]
        bq_comm, _, _ = blinkquant_fee(fill_price, 1000, "BUY")
        expected = 1000000 - (fill_price * 1000) - bq_comm

        assert cash_after == pytest.approx(expected, abs=1), \
            f"cash: expected {expected}, got {cash_after}"

    def test_cash_after_buy_sell_cycle(self, fee_root):
        """Cash after BUY+SELL cycle should be init_cash minus total fees."""
        def handle_bar(context, bar_dict):
            dt = context.now.date()
            if dt == datetime.date(2024, 1, 3):
                order_shares("sh.600000", 1000)
            elif dt == datetime.date(2024, 1, 4):
                order_shares("sh.600000", -1000)

        result = run_fee_test(fee_root, handle_bar)
        portfolio = result["sys_analyser"]["portfolio"]
        # Row after SELL (row 2 = 2024-01-04)
        final_cash = portfolio.iloc[2]["cash"]

        trades = result["sys_analyser"]["trades"]
        total_fee = sum(trades["transaction_cost"])
        expected = 1000000 - total_fee

        assert final_cash == pytest.approx(expected, abs=1), \
            f"final cash: expected {expected}, got {final_cash}"


# ======================================================================
# 4. Deterministic
# ======================================================================

class TestDeterministic:
    def test_repeated_runs_identical(self, fee_root):
        """Two identical runs produce identical fee results."""
        def run_once():
            def handle_bar(context, bar_dict):
                dt = context.now.date()
                if dt == datetime.date(2024, 1, 3):
                    order_shares("sh.600000", 1000)
                elif dt == datetime.date(2024, 1, 4):
                    order_shares("sh.600000", -1000)
            result = run_fee_test(fee_root, handle_bar)
            return result["sys_analyser"]["trades"]

        r1 = run_once()
        r2 = run_once()
        assert len(r1) == len(r2), "Trade count should be identical"
        for i in range(len(r1)):
            assert r1.iloc[i]["commission"] == r2.iloc[i]["commission"]
            assert r1.iloc[i]["tax"] == r2.iloc[i]["tax"]
            assert r1.iloc[i]["transaction_cost"] == r2.iloc[i]["transaction_cost"]

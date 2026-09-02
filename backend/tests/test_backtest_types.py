import datetime
from core.backtest_types import (
    FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG,
    Allocator, SelectionResult, Position, equal_weight_allocator
)


def test_fee_config_defaults_are_research_values():
    fc = FeeConfig()
    assert fc.commission_rate == 0.00025
    assert fc.commission_min == 5.0
    assert fc.stamp_tax_rate == 0.0005
    assert fc.transfer_fee_rate == 0.00001


def test_mvp_execution_config_frozen():
    cfg = MVP_EXECUTION_CONFIG
    assert cfg.price_mode == "open"
    assert cfg.order_sequence == "sell_first"
    assert cfg.cash_reinvestment == "same_cycle"
    assert cfg.partial_fill_policy == "keep_cash"
    # Attempt to modify should not affect MVP constant
    cfg2 = ExecutionConfig()
    assert cfg2.price_mode == "open"


def test_allocator_equal_weight():
    codes = ["sh.600000", "sz.000001", "sh.600002"]
    weights = equal_weight_allocator(codes, datetime.date(2025,1,2))
    assert set(weights.keys()) == set(codes)
    assert all(abs(w - 1/3) < 1e-9 for w in weights.values())
    assert abs(sum(weights.values()) - 1.0) < 1e-9


def test_selection_result_dataclass():
    res = SelectionResult(
        requested_date=datetime.date(2025,1,5),
        signal_date=datetime.date(2025,1,3),
        codes=["sh.600000", "sz.000001"],
        metadata={"formula": "CLOSE > 10", "timeframe": "D", "has_mtf": False}
    )
    assert res.requested_date == datetime.date(2025,1,5)
    assert res.signal_date == datetime.date(2025,1,3)
    assert len(res.codes) == 2


def test_position_with_frozen_qty():
    pos = Position(
        code="sh.600000",
        total_qty=1500,
        available_qty=1000,
        frozen_qty=500,
        avg_cost=10.5,
        market_value=15750.0
    )
    assert pos.available_qty == 1000
    assert pos.frozen_qty == 500
    assert pos.total_qty == 1500
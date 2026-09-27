"""P2-B.3 position sizing and capital-allocation contracts.

Freeze the public sizing semantics without coupling tests to the execution
engine's implementation details.
"""

import datetime as dt

from core.backtest_engine import BacktestEngine, OrderIntent
from core.backtest_types import (
    FeeConfig,
    MVP_EXECUTION_CONFIG,
    equal_weight_allocator,
    top_n_equal_weight_allocator,
)
from core.portfolio import Portfolio, Position


SIGNAL_DATE = dt.date(2024, 1, 5)


def _engine_with_portfolio(cash, positions=None):
    engine = BacktestEngine(
        calendar=None,
        selection_engine=None,
        raw_price_store=None,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )
    engine.portfolio = Portfolio(initial_cash=cash)
    if positions:
        engine.portfolio.positions.update(positions)
    return engine


def test_equal_weight_allocates_one_over_n_and_sums_to_one():
    codes = ["sz.000003", "sh.000001", "sz.000002"]

    weights = equal_weight_allocator(codes, SIGNAL_DATE)

    assert weights == {
        "sz.000003": 1 / 3,
        "sh.000001": 1 / 3,
        "sz.000002": 1 / 3,
    }
    assert sum(weights.values()) == 1.0


def test_equal_weight_empty_is_empty():
    assert equal_weight_allocator([], SIGNAL_DATE) == {}


def test_top_n_equal_weight_is_deterministic_and_capped():
    codes = ["sz.000003", "sh.000001", "sz.000002", "bj.000004"]

    weights = top_n_equal_weight_allocator(2)(codes, SIGNAL_DATE)

    assert list(weights) == ["bj.000004", "sh.000001"]
    assert len(weights) == 2
    assert sum(weights.values()) == 1.0
    assert all(weight == 0.5 for weight in weights.values())


def test_top_n_equal_weight_uses_fewer_than_n_when_candidates_are_fewer():
    weights = top_n_equal_weight_allocator(20)(["sh.000001", "sz.000002"], SIGNAL_DATE)

    assert weights == {
        "sh.000001": 0.5,
        "sz.000002": 0.5,
    }


def test_target_portfolio_intents_follow_complete_target_weights():
    engine = _engine_with_portfolio(
        1_000_000,
        {
            "sh.600000": Position(
                code="sh.600000",
                total_qty=10_000,
                available_qty=10_000,
                frozen_qty=0,
                avg_cost=10.0,
                market_value=100_000.0,
            )
        },
    )

    intents = engine._generate_intents(
        target_weights={"sh.600000": 0.5, "sz.000001": 0.5},
        execution_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 10.0, "close": 10.0},
        },
    )

    by_code = {intent.code: intent for intent in intents}
    assert by_code["sh.600000"].side == "BUY"
    assert by_code["sz.000001"].side == "BUY"
    assert by_code["sh.600000"].target_qty == 45_000
    assert by_code["sz.000001"].target_qty == 55_000


def test_event_driven_entry_capital_excludes_retained_positions():
    # Equity = 2,000, but only 1,000 cash is free. A new entry must not
    # implicitly spend the retained position's 1,000 market value.
    engine = _engine_with_portfolio(
        1_000,
        {
            "sh.600000": Position(
                code="sh.600000",
                total_qty=100,
                available_qty=100,
                frozen_qty=0,
                avg_cost=10.0,
                market_value=1_000.0,
            )
        },
    )

    intents = engine._generate_event_intents(
        entry_codes=["sz.000001"],
        exit_codes=[],
        execution_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 10.0, "close": 10.0},
        },
    )

    buys = [i for i in intents if i.side == "BUY"]
    assert len(buys) == 1
    assert buys[0].code == "sz.000001"
    assert buys[0].target_qty == 100
    assert buys[0].target_weight == 0.5


def test_event_driven_entry_capital_reuses_executable_exit_proceeds():
    engine = _engine_with_portfolio(
        0,
        {
            "sh.600000": Position(
                code="sh.600000",
                total_qty=100,
                available_qty=100,
                frozen_qty=0,
                avg_cost=10.0,
                market_value=1_000.0,
            )
        },
    )

    intents = engine._generate_event_intents(
        entry_codes=["sz.000001", "sz.000002"],
        exit_codes=["sh.600000"],
        execution_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 10.0, "close": 10.0},
            "sz.000002": {"open": 20.0, "close": 20.0},
        },
    )

    sells = [i for i in intents if i.side == "SELL"]
    buys = [i for i in intents if i.side == "BUY"]

    assert len(sells) == 1
    assert sells[0].code == "sh.600000"
    assert sells[0].target_qty == 100

    assert [(i.code, i.target_qty) for i in buys] == [
        ("sz.000001", 50),
        ("sz.000002", 25),
    ]
    assert all(i.target_weight == 0.5 for i in buys)


def test_event_driven_does_not_double_count_existing_entry_position():
    engine = _engine_with_portfolio(
        1_000,
        {
            "sh.600000": Position(
                code="sh.600000",
                total_qty=100,
                available_qty=100,
                frozen_qty=0,
                avg_cost=10.0,
                market_value=1_000.0,
            )
        },
    )

    intents = engine._generate_event_intents(
        entry_codes=["sh.600000", "sz.000001"],
        exit_codes=[],
        execution_prices={
            "sh.600000": {"open": 10.0, "close": 10.0},
            "sz.000001": {"open": 10.0, "close": 10.0},
        },
    )

    buys = [i for i in intents if i.side == "BUY"]
    assert [(i.code, i.target_qty) for i in buys] == [("sz.000001", 100)]


def test_event_driven_never_allocates_more_than_free_cycle_capital():
    engine = _engine_with_portfolio(1_000)

    intents = engine._generate_event_intents(
        entry_codes=["sh.000001", "sh.000002", "sh.000003"],
        exit_codes=[],
        execution_prices={
            "sh.000001": {"open": 10.0, "close": 10.0},
            "sh.000002": {"open": 10.0, "close": 10.0},
            "sh.000003": {"open": 10.0, "close": 10.0},
        },
    )

    buys = [i for i in intents if i.side == "BUY"]
    assert sum(i.target_qty * 10.0 for i in buys) <= 1_000

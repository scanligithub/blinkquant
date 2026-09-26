import datetime as dt
from types import SimpleNamespace

from core.backtest_engine import BacktestEngine
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)


def _engine():
    engine = BacktestEngine.__new__(BacktestEngine)
    engine.portfolio = SimpleNamespace(
        cash=100_000.0,
        positions={},
    )
    engine.strategy_selector = SimpleNamespace()
    engine.calendar = SimpleNamespace(
        next_trade_day=lambda d: d + dt.timedelta(days=1)
    )
    engine.raw_price_store = SimpleNamespace(
        load_execution_prices=lambda dates: __import__("polars").DataFrame(
            {
                "code": ["AAA", "BBB"],
                "open": [10.0, 20.0],
                "close": [10.0, 20.0],
            }
        )
    )
    engine._selected_thru = None
    return engine


def test_strategy_target_portfolio_generates_full_target_rebalance():
    engine = _engine()
    strategy = StrategyDefinition(
        universe=UniverseDefinition(type="all_a"),
        entry=SignalDefinition("CLOSE > 0"),
        sizing=PositionSizingDefinition(
            method="top_n_equal_weight", max_positions=1
        ),
        rebalance=RebalanceDefinition("daily"),
    )
    engine.strategy_selector.select = lambda strategy, date, backtest_mode=True: SimpleNamespace(
        signal_date=date,
        target_weights={"AAA": 1.0},
        entry_codes=["AAA"],
        exit_codes=[],
    )
    diag = {
        "intents_total": 0,
        "target_gross_by_date": {},
    }
    new_sig, new_exec, intents, prices = engine._phase_post_close_signal(
        dt.date(2024, 1, 2),
        {dt.date(2024, 1, 2)},
        None,
        None,
        20,
        None,
        None,
        diag,
        strategy=strategy,
    )
    assert new_sig == dt.date(2024, 1, 2)
    assert new_exec == dt.date(2024, 1, 3)
    assert [i.code for i in intents] == ["AAA"]
    assert intents[0].side == "BUY"
    assert diag["target_gross_by_date"][dt.date(2024, 1, 3)] == 1.0


def test_event_driven_does_not_sell_when_entry_disappears():
    engine = _engine()
    from core.portfolio import Position

    engine.portfolio.positions = {
        "AAA": Position(
            code="AAA",
            total_qty=100,
            available_qty=100,
            frozen_qty=0,
            avg_cost=10.0,
            market_value=1000.0,
        )
    }
    strategy = StrategyDefinition(
        universe=UniverseDefinition(type="all_a"),
        entry=SignalDefinition("CLOSE > 0"),
        exit=SignalDefinition("CLOSE < 0"),
        mode="event_driven",
    )
    engine.strategy_selector.select = lambda strategy, date, backtest_mode=True: SimpleNamespace(
        signal_date=date,
        target_weights={},
        entry_codes=[],
        exit_codes=[],
    )
    diag = {"intents_total": 0, "target_gross_by_date": {}}
    _, _, intents, _ = engine._phase_post_close_signal(
        dt.date(2024, 1, 2),
        {dt.date(2024, 1, 2)},
        None,
        None,
        20,
        None,
        None,
        diag,
        strategy=strategy,
    )
    assert intents == []


def test_event_driven_explicit_exit_generates_sell():
    engine = _engine()
    from core.portfolio import Position

    engine.portfolio.positions = {
        "AAA": Position(
            code="AAA",
            total_qty=100,
            available_qty=100,
            frozen_qty=0,
            avg_cost=10.0,
            market_value=1000.0,
        )
    }
    intents = engine._generate_event_intents(
        [],
        ["AAA"],
        {"AAA": {"open": 10.0, "close": 10.0}},
    )
    assert len(intents) == 1
    assert intents[0].side == "SELL"
    assert intents[0].target_qty == 100

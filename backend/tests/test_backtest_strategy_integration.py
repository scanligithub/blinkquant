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
def test_p4_1_weekly_pit_top20_schedules_next_open():
    """P4.1 contract: W signal + PIT index + Top20 -> next trading-day Open."""
    import polars as pl

    from core.strategy_selector import StrategySelector
    from core.universe_resolver import UniverseResolver
    from core.backtest_types import SelectionResult
    from core.data_manager import data_manager

    signal_date = dt.date(2024, 1, 5)   # Friday, weekly signal date
    execution_date = dt.date(2024, 1, 8)  # next trading day

    members = [f"S{i:02d}" for i in range(25)]
    resolver = UniverseResolver(
        pl.DataFrame({
            "index_id": ["000300"] * 25,
            "stock_id": members,
            "start_date": [dt.date(2020, 1, 1)] * 25,
            "end_date": [None] * 25,
        })
    )

    class FakeSelectionEngine:
        def execute_selector(self, formula, timeframe, background_tasks,
                             target_date, backtest_mode, raise_on_error,
                             eligible_codes, **kwargs):
            assert timeframe == "W"
            assert target_date == signal_date
            assert eligible_codes == members
            if "MA(CLOSE,5)" in formula and "MA(CLOSE,60)" in formula:
                codes = eligible_codes[:]
            else:
                raise AssertionError(f"unexpected entry formula: {formula}")
            return SelectionResult(
                requested_date=target_date,
                signal_date=target_date,
                codes=codes,
                metadata={},
            )

    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame({
            "date": [dt.date(2024, 1, 5)],
            "code": members,
            "close": [100.0] * len(members),
        })
        strategy = StrategyDefinition(
            universe=UniverseDefinition(type="index", index_id="000300"),
            entry=SignalDefinition(
                condition="MA(CLOSE,5) > MA(CLOSE,60)",
                trigger="condition",
                timeframe="W",
            ),
            sizing=PositionSizingDefinition(
                method="top_n_equal_weight",
                max_positions=20,
            ),
            rebalance=RebalanceDefinition(frequency="weekly"),
        )
        selector = StrategySelector(
            selection_engine=FakeSelectionEngine(),
            universe_resolver=resolver,
        )
        engine = _engine()
        engine.strategy_selector = selector
        engine.calendar.next_trade_day = lambda d: execution_date
        engine.raw_price_store.load_execution_prices = lambda dates: pl.DataFrame({
            "code": members,
            "open": [10.0] * len(members),
            "close": [10.0] * len(members),
        })

        diag = {"intents_total": 0, "target_gross_by_date": {}}
        new_sig, new_exec, intents, _ = engine._phase_post_close_signal(
            signal_date,
            {signal_date},
            None,
            None,
            20,
            None,
            None,
            diag,
            strategy=strategy,
        )
    finally:
        data_manager.df_daily = original

    assert new_sig == signal_date
    assert new_exec == execution_date
    assert len(intents) == 20
    assert all(intent.side == "BUY" for intent in intents)
    assert {intent.code for intent in intents} == set(members[:20])
    assert diag["target_gross_by_date"][execution_date] == 1.0

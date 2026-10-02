"""IA5.6 regression coverage for production SignalTrace wiring."""

import datetime as dt

import polars as pl

from core.backtest_engine import BacktestEngine
from core.backtest_types import FeeConfig
from core.checkpoint import BacktestCheckpoint, load_checkpoint, save_checkpoint
from core.execution import ExecutionEngine, OrderIntent
from core.portfolio import Portfolio, Position
from core.engine import SelectionEngine
from core.signal_trace import CodeTrace, DecisionTrace, ExecutionTrace, SignalTraceData
from core.strategy import StrategyDefinition
from core.strategy_selector import StrategySelectionResult, StrategySelector
from core.data_manager import data_manager


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 3),
                dt.date(2024, 1, 4),
            ],
            "code": ["AAA", "AAA", "AAA"],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.0, 11.0, 12.0],
            "volume": [1000.0, 1000.0, 1000.0],
            "amount": [10000.0, 11000.0, 12000.0],
        }
    )


def test_selection_trace_is_wired_and_cache_safe():
    original = (
        data_manager.df_daily,
        data_manager.df_weekly,
        data_manager.df_monthly,
    )
    try:
        data_manager.df_daily = _frame()
        data_manager.df_weekly = None
        data_manager.df_monthly = None

        selector = StrategySelector(selection_engine=SelectionEngine())
        strategy = StrategyDefinition.from_dict(
            {
                "name": "IA5.6 trace",
                "universe": {"type": "all_a"},
                "entry": {
                    "condition": "CLOSE > MA(CLOSE,2)",
                    "trigger": "condition",
                    "timeframe": "D",
                },
                "sizing": {
                    "method": "top_n_equal_weight",
                    "max_positions": 1,
                },
                "rebalance": {"frequency": "daily"},
                "mode": "target_portfolio",
            }
        )

        plain = selector.select(strategy, dt.date(2024, 1, 4), trace=False)
        traced = selector.select(strategy, dt.date(2024, 1, 4), trace=True)

        assert plain.target_codes == traced.target_codes == ["AAA"]
        assert traced.signal_trace is not None
        assert traced.signal_trace.signal_date == "2024-01-04"
        assert traced.signal_trace.traces
        assert traced.signal_trace.traces[0].code == "AAA"
        assert traced.signal_trace.traces[0].atoms
        atom = traced.signal_trace.traces[0].atoms[0]
        assert atom.operator == ">"
        assert atom.value == 12.0
        assert atom.threshold == 11.5
        assert atom.passed is True
        assert atom.source

        # Execution enrichment mutates the returned trace; a subsequent cache hit
        # must receive a clean copy rather than the already-enriched run object.
        traced.signal_trace.traces[0].execution = ExecutionTrace(
            execution_date=dt.date(2024, 1, 5), price=13.0, side="BUY", qty=100, fee=1.0
        )
        traced_again = selector.select(strategy, dt.date(2024, 1, 4), trace=True)
        assert traced_again.signal_trace is not traced.signal_trace
        assert traced_again.signal_trace.traces[0].execution is None
    finally:
        (
            data_manager.df_daily,
            data_manager.df_weekly,
            data_manager.df_monthly,
        ) = original


def test_backtest_engine_collects_trace_without_changing_default_path():
    trace_data = SignalTraceData(
        signal_date="2024-01-03",
        formula="CLOSE > MA(CLOSE,2)",
        traces=[CodeTrace(code="AAA", passed=True)],
    )

    class _Calendar:
        def next_trade_day(self, date):
            return dt.date(2024, 1, 4)

    class _RawStore:
        def load_execution_prices(self, dates):
            return pl.DataFrame(
                {
                    "code": ["AAA"],
                    "open": [12.0],
                    "close": [12.0],
                }
            )

    class _Selector:
        def select(self, strategy, target_date, *, backtest_mode=True, trace=False):
            return StrategySelectionResult(
                requested_date=target_date,
                signal_date=target_date,
                entry_codes=[],
                exit_codes=[],
                target_codes=[],
                target_weights={},
                signal_trace=trace_data if trace else None,
            )

    strategy = StrategyDefinition.from_dict(
        {
            "name": "IA5.6 fake",
            "universe": {"type": "all_a"},
            "entry": {
                "condition": "CLOSE > MA(CLOSE,2)",
                "trigger": "condition",
                "timeframe": "D",
            },
            "sizing": {
                "method": "top_n_equal_weight",
                "max_positions": 1,
            },
            "rebalance": {"frequency": "daily"},
            "mode": "target_portfolio",
        }
    )
    engine = BacktestEngine(
        calendar=_Calendar(),
        selection_engine=SelectionEngine(),
        raw_price_store=_RawStore(),
        fee_config=FeeConfig(),
        strategy_selector=_Selector(),
    )
    engine.portfolio = Portfolio(initial_cash=1_000_000)
    diag = {
        "rej_counters": {},
        "intents_total": 0,
        "partial_fill_count": 0,
        "target_gross_by_date": {},
    }

    phase_result = engine._phase_post_close_signal(
        dt.date(2024, 1, 3),
        {dt.date(2024, 1, 3)},
        None,
        None,
        20,
        None,
        None,
        diag,
        strategy=strategy,
        collect_signal_trace=True,
    )

    assert phase_result[:2] == (dt.date(2024, 1, 3), dt.date(2024, 1, 4))
    assert engine._signal_traces["2024-01-03"] is trace_data


def test_execution_trace_round_trip_preserves_fee():
    trace = SignalTraceData(
        signal_date="2024-01-03",
        formula="CLOSE > 10",
        traces=[
            CodeTrace(
                code="AAA",
                passed=True,
                execution=ExecutionTrace(
                    execution_date=dt.date(2024, 1, 4),
                    price=12.0,
                    side="BUY",
                    qty=100,
                    fee=3.5,
                ),
            )
        ],
    )
    traces_df, atoms_df = trace.to_parquet()
    restored = SignalTraceData.from_parquet(traces_df, atoms_df)

    assert restored.traces[0].execution is not None
    assert restored.traces[0].execution.fee == 3.5


def test_signal_trace_data_is_json_serializable():
    trace = SignalTraceData(
        signal_date="2024-01-03",
        formula="CLOSE > 10",
        traces=[CodeTrace(code="AAA", passed=True)],
    )
    payload = trace.to_dict()
    assert payload["schema_version"] == "1.0.0"
    assert payload["traces"][0]["code"] == "AAA"



def test_decision_trace_preserves_buy_sell_rejection_and_partial_fill():
    signal_date = dt.date(2024, 1, 3)
    execution_date = dt.date(2024, 1, 4)
    trace = SignalTraceData(
        signal_date=signal_date.isoformat(),
        formula="CLOSE > 10",
        traces=[CodeTrace(code="AAA", passed=True)],
        decisions=[
            DecisionTrace("AAA", "BUY", 100, 0.5, execution_date=execution_date),
            DecisionTrace("AAA", "SELL", 100, 0.5, execution_date=execution_date),
            DecisionTrace("BBB", "SELL", 100, 0.5, execution_date=execution_date),
            DecisionTrace("CCC", "SELL", 200, 0.5, execution_date=execution_date),
        ],
    )

    class _Calendar:
        def next_trade_day(self, date):
            return execution_date

    class _RawStore:
        def load_limit_flags_for_date(self, date, codes):
            return None

    engine = BacktestEngine(
        calendar=_Calendar(),
        selection_engine=SelectionEngine(),
        raw_price_store=_RawStore(),
        fee_config=FeeConfig(),
    )
    engine.portfolio = Portfolio(initial_cash=100_000)
    engine.portfolio.load_initial_positions({
        "AAA": Position("AAA", total_qty=100, available_qty=100, frozen_qty=0),
        "BBB": Position("BBB", total_qty=100, available_qty=0, frozen_qty=100),
        "CCC": Position("CCC", total_qty=100, available_qty=100, frozen_qty=0),
    })
    engine.execution_engine = ExecutionEngine(engine.execution_config, engine.fee_config)
    engine.raw_price_store = _RawStore()
    engine._signal_traces = {signal_date.isoformat(): trace}
    engine._pend_sig = signal_date
    engine._pend_exec = execution_date
    engine._pend_intents = [
        OrderIntent("AAA", "BUY", 100, 0.5),
        OrderIntent("AAA", "SELL", 100, 0.5),
        OrderIntent("BBB", "SELL", 100, 0.5),
        OrderIntent("CCC", "SELL", 200, 0.5),
    ]
    engine._pend_prices = {
        "AAA": {"open": 10.0, "close": 10.0},
        "BBB": {"open": 10.0, "close": 10.0},
        "CCC": {"open": 10.0, "close": 10.0},
    }
    diag = {
        "rej_counters": {},
        "partial_fill_count": 0,
        "zero_price_trade_count": 0,
        "intents_total": 4,
    }

    fills, cur_signal_date, _ = engine._phase_post_execution(execution_date, None, diag)
    assert len(fills) == 3
    assert cur_signal_date == signal_date

    decisions = {(d.code, d.side): d for d in trace.decisions}
    assert decisions[("AAA", "BUY")].status == "FILLED"
    assert decisions[("AAA", "SELL")].status == "FILLED"
    assert decisions[("BBB", "SELL")].status == "REJECTED"
    assert decisions[("BBB", "SELL")].rejection_reason == "FROZEN"
    assert decisions[("CCC", "SELL")].status == "PARTIAL"
    assert decisions[("CCC", "SELL")].executed_qty == 100

    code_trace = trace.traces[0]
    assert sorted(e.side for e in code_trace.executions) == ["BUY", "SELL"]


def test_signal_trace_checkpoint_round_trip_preserves_decisions(tmp_path):
    trace = SignalTraceData(
        signal_date="2024-01-03",
        formula="CLOSE > 10",
        traces=[CodeTrace(code="AAA", passed=True)],
        decisions=[
            DecisionTrace(
                code="AAA",
                side="BUY",
                target_qty=100,
                target_weight=0.5,
                execution_date=dt.date(2024, 1, 4),
                status="REJECTED",
                rejection_reason="LIMIT_BLOCKED",
            )
        ],
    )
    checkpoint = BacktestCheckpoint(
        current_date="2024-01-03",
        cash=99_000.0,
        signal_traces={"2024-01-03": trace.to_dict()},
    )
    directory = tmp_path / "cp"
    save_checkpoint(checkpoint, directory)
    restored = load_checkpoint(directory)

    assert restored.signal_traces["2024-01-03"]["decisions"][0]["status"] == "REJECTED"
    assert restored.signal_traces["2024-01-03"]["decisions"][0]["rejection_reason"] == "LIMIT_BLOCKED"


def test_signal_trace_json_round_trip_preserves_multiple_executions_and_decisions():
    trace = SignalTraceData(
        signal_date="2024-01-03",
        formula="CLOSE > 10",
        traces=[
            CodeTrace(
                code="AAA",
                passed=True,
                executions=[
                    ExecutionTrace(dt.date(2024, 1, 4), 10.0, "SELL", 100, 1.0),
                    ExecutionTrace(dt.date(2024, 1, 4), 10.0, "BUY", 100, 1.0),
                ],
            )
        ],
        decisions=[
            DecisionTrace("AAA", "SELL", 100, 0.0, dt.date(2024, 1, 4), status="FILLED", executed_qty=100),
            DecisionTrace("AAA", "BUY", 100, 0.5, dt.date(2024, 1, 4), status="FILLED", executed_qty=100),
        ],
    )
    restored = SignalTraceData.from_dict(trace.to_dict())

    assert [e.side for e in restored.traces[0].executions] == ["SELL", "BUY"]
    assert {(d.code, d.side) for d in restored.decisions} == {("AAA", "SELL"), ("AAA", "BUY")}



def test_atom_trace_reports_false_comparison_and_boolean_leaves():
    original = (
        data_manager.df_daily,
        data_manager.df_weekly,
        data_manager.df_monthly,
    )
    try:
        data_manager.df_daily = _frame()
        data_manager.df_weekly = None
        data_manager.df_monthly = None

        engine = SelectionEngine()
        trace = engine._generate_trace(
            ["AAA"],
            "(CLOSE > 10) AND (CLOSE < 20)",
            "D",
            dt.date(2024, 1, 4),
            False,
        )
        assert len(trace.traces) == 1
        atoms = trace.traces[0].atoms
        assert len(atoms) == 2
        assert [a.operator for a in atoms] == [">", "<"]
        assert all(a.passed for a in atoms)

        false_trace = engine._generate_trace(
            ["AAA"],
            "CLOSE > 100",
            "D",
            dt.date(2024, 1, 4),
            False,
        )
        false_atom = false_trace.traces[0].atoms[0]
        assert false_atom.operator == ">"
        assert false_atom.threshold == 100.0
        assert false_atom.value == 12.0
        assert false_atom.passed is False
    finally:
        (
            data_manager.df_daily,
            data_manager.df_weekly,
            data_manager.df_monthly,
        ) = original




def test_backtest_engine_restore_keeps_t1_signal_trace_provenance():
    trace = SignalTraceData(
        signal_date="2025-12-31",
        formula="CLOSE > 10",
        traces=[CodeTrace(code="AAA", passed=True)],
        decisions=[
            DecisionTrace(
                code="AAA",
                side="SELL",
                target_qty=100,
                target_weight=0.0,
                execution_date=dt.date(2026, 1, 2),
                status="PENDING",
            )
        ],
    )
    cp = BacktestCheckpoint(
        current_date="2025-12-31",
        cash=100_000.0,
        positions=[{
            "code": "AAA", "total_qty": 100, "available_qty": 100,
            "frozen_qty": 0, "avg_cost": 10.0, "market_value": 1000.0,
        }],
        pending_signal_date="2025-12-31",
        pending_execution_date="2026-01-02",
        pending_intents=[{
            "code": "AAA", "side": "SELL", "target_qty": 100, "target_weight": 0.0,
        }],
        pending_prices={"AAA": {"open": 11.0, "close": 11.0}},
        signal_traces={"2025-12-31": trace.to_dict()},
    )

    class _Calendar:
        def next_trade_day(self, date):
            return dt.date(2026, 1, 2)

    engine = BacktestEngine(
        calendar=_Calendar(),
        selection_engine=SelectionEngine(),
        raw_price_store=object(),
        fee_config=FeeConfig(),
    )
    engine._restore_from_checkpoint(cp)

    restored = engine._signal_traces["2025-12-31"]
    assert restored.signal_date == "2025-12-31"
    assert restored.decisions[0].execution_date == dt.date(2026, 1, 2)
    assert restored.decisions[0].status == "PENDING"
    assert engine._pend_sig == dt.date(2025, 12, 31)
    assert engine._pend_exec == dt.date(2026, 1, 2)

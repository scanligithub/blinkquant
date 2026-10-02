"""IA5.6 regression coverage for production SignalTrace wiring."""

import datetime as dt

import polars as pl

from core.backtest_engine import BacktestEngine
from core.backtest_types import FeeConfig
from core.engine import SelectionEngine
from core.signal_trace import CodeTrace, ExecutionTrace, SignalTraceData
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

import datetime as dt

import polars as pl

from core.backtest_types import SelectionResult
from core.data_manager import data_manager
from core.strategy import (
    PositionSizingDefinition,
    RebalanceDefinition,
    SignalDefinition,
    StrategyDefinition,
    UniverseDefinition,
)
from core.strategy_selector import StrategySelector
from core.universe_resolver import UniverseResolver


def _membership():
    return UniverseResolver(
        pl.DataFrame(
            {
                "index_id": ["IDX", "IDX", "IDX"],
                "stock_id": ["AAA", "BBB", "CCC"],
                "start_date": [
                    dt.date(2020, 1, 1),
                    dt.date(2020, 1, 1),
                    dt.date(2024, 1, 4),
                ],
                "end_date": [
                    dt.date(2024, 1, 4),
                    None,
                    None,
                ],
            }
        )
    )


class FakeSelectionEngine:
    def execute_selector(
        self,
        formula,
        timeframe,
        background_tasks,
        target_date,
        backtest_mode,
        raise_on_error,
        eligible_codes,
        **kwargs,
    ):
        # 模拟：AAA/BBB 当前满足；AAA 在前一交易日满足，BBB 不满足。
        codes = list(eligible_codes) if eligible_codes is not None else ["AAA", "BBB"]
        if formula == "ENTRY":
            if target_date == dt.date(2024, 1, 3):
                codes = [c for c in codes if c == "BBB"]
            else:
                codes = [c for c in codes if c in {"AAA", "BBB"}]
        elif formula == "EXIT":
            codes = [c for c in codes if c == "AAA"]
        return SelectionResult(
            requested_date=target_date,
            signal_date=target_date,
            codes=sorted(codes),
            metadata={},
        )


def _strategy(trigger="condition"):
    return StrategyDefinition(
        universe=UniverseDefinition(type="index", index_id="IDX"),
        entry=SignalDefinition(
            condition="ENTRY",
            trigger=trigger,
            timeframe="D",
        ),
        exit=SignalDefinition(
            condition="EXIT",
            trigger="condition",
            timeframe="D",
        ),
        sizing=PositionSizingDefinition(
            method="top_n_equal_weight",
            max_positions=1,
        ),
        rebalance=RebalanceDefinition(frequency="weekly"),
    )


def test_strategy_selector_wires_pit_universe_and_target_allocation():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame(
            {
                "date": [
                    dt.date(2024, 1, 3),
                    dt.date(2024, 1, 4),
                ],
                "code": ["AAA", "BBB"],
                "close": [100.0, 100.0],
            }
        )
        result = StrategySelector(
            selection_engine=FakeSelectionEngine(),
            universe_resolver=_membership(),
        ).select(_strategy(), dt.date(2024, 1, 4))
    finally:
        data_manager.df_daily = original

    # 2024-01-04 时 AAA 已退出 IDX，CCC 新加入，BBB 持续存在。
    assert result.signal_date == dt.date(2024, 1, 4)
    assert result.entry_codes == ["BBB", "CCC"]
    assert result.target_codes == ["BBB"]
    assert result.target_weights == {"BBB": 1.0}
    assert result.metadata["eligible_count"] == 2


def test_cross_trigger_uses_previous_pit_universe():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame(
            {
                "date": [
                    dt.date(2024, 1, 3),
                    dt.date(2024, 1, 4),
                ],
                "code": ["AAA", "BBB"],
                "close": [100.0, 100.0],
            }
        )
        strategy = _strategy(trigger="cross_above")
        result = StrategySelector(
            selection_engine=FakeSelectionEngine(),
            universe_resolver=_membership(),
        ).select(strategy, dt.date(2024, 1, 4))
    finally:
        data_manager.df_daily = original

    # 前一日 Universe={AAA,BBB}；当日 Universe={BBB,CCC}。
    # CCC 是新进入 Universe 后首次满足 Entry，必须被视为 cross_above。
    assert result.entry_codes == ["CCC"]


def test_all_a_keeps_existing_universe_behavior():
    strategy = StrategyDefinition(
        universe=UniverseDefinition(type="all_a"),
        entry=SignalDefinition(condition="ENTRY"),
        sizing=PositionSizingDefinition(method="equal_weight"),
    )

    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame(
            {
                "date": [dt.date(2024, 1, 4)],
                "code": ["AAA"],
                "close": [100.0],
            }
        )
        result = StrategySelector(
            selection_engine=FakeSelectionEngine(),
        ).select(strategy, dt.date(2024, 1, 4))
    finally:
        data_manager.df_daily = original

    assert result.entry_codes == ["AAA"]
    assert result.target_weights == {"AAA": 1.0}


def test_weekly_cross_uses_previous_week_not_previous_day():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame({
            "date": [dt.date(2024,1,4), dt.date(2024,1,5), dt.date(2024,1,8), dt.date(2024,1,12)],
            "code": ["AAA"] * 4,
            "close": [1.0] * 4,
        })
        selector = StrategySelector(selection_engine=FakeSelectionEngine())
        assert selector._previous_signal_date(dt.date(2024,1,12), "W") == dt.date(2024,1,5)
    finally:
        data_manager.df_daily = original


def test_monthly_cross_uses_previous_month_not_previous_day():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame({
            "date": [dt.date(2024,1,30), dt.date(2024,1,31), dt.date(2024,2,1), dt.date(2024,2,29)],
            "code": ["AAA"] * 4,
            "close": [1.0] * 4,
        })
        selector = StrategySelector(selection_engine=FakeSelectionEngine())
        assert selector._previous_signal_date(dt.date(2024,2,29), "M") == dt.date(2024,1,31)
    finally:
        data_manager.df_daily = original


def test_strategy_selector_allocates_top_n_target_weights():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = pl.DataFrame({
            "date": [dt.date(2024,1,4)],
            "code": ["AAA"],
            "close": [100.0],
        })
        result = StrategySelector(
            selection_engine=FakeSelectionEngine(),
        ).select(
            StrategyDefinition(
                universe=UniverseDefinition(type="all_a"),
                entry=SignalDefinition(condition="ENTRY"),
                sizing=PositionSizingDefinition(
                    method="top_n_equal_weight", max_positions=1
                ),
            ),
            dt.date(2024,1,4),
        )
        assert result.target_codes == ["AAA"]
        assert result.target_weights == {"AAA": 1.0}
    finally:
        data_manager.df_daily = original

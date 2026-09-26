"""统一策略选择流水线（P3/P4.2/P4.2 verified）。

StrategySelector 负责把 StrategyDefinition 转换为单个 as-of 日的策略信号：
    StrategyDefinition
        -> PIT Universe
        -> SelectionEngine
        -> Entry / Exit
        -> StrategySelectionResult

它不负责：
- 调仓日期推进
- 资金/持仓
- 下单成交
- 手续费
这些仍由 BacktestEngine / ExecutionEngine 负责。
"""
from __future__ import annotations

import datetime as dt

import polars as pl
from dataclasses import dataclass, field
from typing import Optional

from .data_manager import data_manager
from .engine import SelectionEngine
from .backtest_types import equal_weight_allocator, top_n_equal_weight_allocator
from .strategy import StrategyDefinition
from .universe_resolver import UniverseResolver


@dataclass(frozen=True)
class StrategySelectionResult:
    """单个 as-of 日的策略信号结果。"""

    requested_date: Optional[dt.date]
    signal_date: dt.date
    entry_codes: list[str]
    exit_codes: list[str]
    target_codes: list[str]
    target_weights: dict[str, float] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


class StrategySelector:
    """StrategyDefinition -> PIT Universe -> SelectionEngine 的统一入口。"""

    def __init__(
        self,
        selection_engine: Optional[SelectionEngine] = None,
        universe_resolver: Optional[UniverseResolver] = None,
    ) -> None:
        self.selection_engine = selection_engine or SelectionEngine()
        self.universe_resolver = universe_resolver
        self._period_frames_ready = set()

    def _eligible_codes(
        self,
        strategy: StrategyDefinition,
        signal_date: dt.date,
    ) -> Optional[list[str]]:
        if strategy.universe.type == "all_a":
            return None

        if self.universe_resolver is None:
            raise ValueError(
                "index strategy requires a UniverseResolver"
            )

        return self.universe_resolver.members(
            strategy.universe.index_id,
            signal_date,
        )

    def _ensure_correct_period_frame(self, timeframe: str) -> None:
        """Build period bars whose date is the actual last trading day.

        Polars group_by_dynamic defaults to a window label rather than the
        last observed trading date.  In a PIT backtest that is fatal for W/M:
        a Friday target can otherwise exclude the current week's bar and make
        current/previous cross evaluations read the same period.
        """
        tf = timeframe.upper()
        if tf not in ("W", "M") or tf in self._period_frames_ready:
            return
        df = data_manager.df_daily
        if df is None or df.is_empty():
            return

        # Signal evaluation only requires OHLCV columns that actually exist.
        # Lightweight selector fixtures may contain date/code/close only.
        aggs = [pl.col("close").last(), pl.col("date").last().alias("_period_date")]
        for column, expr in (
            ("open", pl.col("open").first()),
            ("high", pl.col("high").max()),
            ("low", pl.col("low").min()),
            ("volume", pl.col("volume").sum()),
            ("amount", pl.col("amount").sum()),
        ):
            if column in df.columns:
                aggs.insert(-1, expr)
        every = "1w" if tf == "W" else "1mo"
        period_df = (
            df.sort(["code", "date"])
            .group_by_dynamic("date", every=every, group_by="code")
            .agg(aggs)
            .drop("date")
            .rename({"_period_date": "date"})
            .sort(["code", "date"])
        )
        if tf == "W":
            data_manager.df_weekly = period_df
        else:
            data_manager.df_monthly = period_df
        self._period_frames_ready.add(tf)

    def _previous_signal_date(
        self, signal_date: dt.date, timeframe: str = "D"
    ) -> Optional[dt.date]:
        """返回当前信号周期之前的最后一个可用交易日。

        D: 前一交易日；
        W: 前一 ISO 周最后交易日；
        M: 前一自然月最后交易日。
        """
        df = data_manager.df_daily
        if df is None or df.is_empty():
            return None

        tf = timeframe.upper()
        if tf == "D":
            boundary = signal_date
        elif tf == "W":
            iso = signal_date.isocalendar()
            boundary = signal_date - dt.timedelta(days=iso.weekday)
        elif tf == "M":
            boundary = signal_date.replace(day=1)
        else:
            raise ValueError(f"unsupported timeframe: {timeframe!r}")

        previous = (
            df.filter(pl.col("date") < boundary)
            .select(pl.col("date").max())
            .item()
        )
        return previous

    def _select_signal(
        self,
        signal,
        signal_date: dt.date,
        eligible_codes: Optional[list[str]],
        backtest_mode: bool,
    ):
        timeframe = signal.timeframe.upper()
        self._ensure_correct_period_frame(timeframe)

        # For W/M backtests use the corrected in-memory period frame.  The
        # daily frame has already been loaded from the real QFQ provider by
        # BacktestEngine, so no second data download is required.
        use_provider = not (backtest_mode and timeframe in ("W", "M"))
        result = self.selection_engine.execute_selector(
            signal.condition,
            timeframe,
            None,
            target_date=signal_date,
            backtest_mode=backtest_mode,
            raise_on_error=True,
            eligible_codes=eligible_codes,
            qfq_data_provider=(getattr(self, "_qfq_data_provider", None) if use_provider else None),
            latest_adj=(getattr(self, "_latest_adj", None) if use_provider else None),
        )
        if isinstance(result, dict) and "error" in result:
            raise RuntimeError(result["error"])
        return result

    def _select_trigger(
        self,
        strategy: StrategyDefinition,
        signal,
        signal_date: dt.date,
        backtest_mode: bool,
    ) -> list[str]:
        eligible_codes = self._eligible_codes(strategy, signal_date)
        current_result = self._select_signal(
            signal, signal_date, eligible_codes, backtest_mode
        )
        current = set(current_result.codes)

        if signal.trigger == "condition":
            return sorted(current)

        previous_date = self._previous_signal_date(signal_date, signal.timeframe)
        if previous_date is None:
            return []

        # 对 cross_*，Universe 也必须按各自历史 as-of 日解析，
        # 不能把当前 Universe 套到历史信号日上。
        previous_eligible = self._eligible_codes(strategy, previous_date)
        previous_result = self._select_signal(
            signal,
            previous_date,
            previous_eligible,
            backtest_mode,
        )
        previous = set(previous_result.codes)

        if signal.trigger == "cross_above":
            return sorted(current - previous)
        if signal.trigger == "cross_below":
            return sorted(previous - current)

        raise ValueError(f"unsupported signal trigger: {signal.trigger!r}")

    def select(
        self,
        strategy: StrategyDefinition,
        target_date: dt.date,
        *,
        backtest_mode: bool = True,
    ) -> StrategySelectionResult:
        """计算 strategy 在 target_date 的 Entry/Exit/Target 信号。

        target_portfolio:
            target_codes = entry_codes；BacktestEngine 根据目标组合与当前持仓
            的差异决定买卖。

        event_driven:
            target_codes 仍保留为 entry_codes，便于统一观察；
            BacktestEngine 应使用 entry_codes / exit_codes 作为事件。
        """
        if not isinstance(target_date, dt.date):
            raise TypeError("target_date must be datetime.date")

        df = data_manager.df_daily
        if df is None or df.is_empty():
            raise RuntimeError("Data not loaded.")
        effective_date = (
            df.filter(pl.col("date") <= target_date)
            .select(pl.col("date").max())
            .item()
        )
        if effective_date is None:
            raise RuntimeError(
                f"指定日期 {target_date} 早于数据起点，无可用交易日数据"
            )

        entry_codes = self._select_trigger(
            strategy,
            strategy.entry,
            effective_date,
            backtest_mode,
        )

        exit_codes: list[str] = []
        if strategy.exit is not None:
            exit_codes = self._select_trigger(
                strategy,
                strategy.exit,
                effective_date,
                backtest_mode,
            )

        if strategy.sizing.method == "equal_weight":
            target_weights = equal_weight_allocator(entry_codes, effective_date)
        else:
            allocator = top_n_equal_weight_allocator(strategy.sizing.max_positions)
            target_weights = allocator(entry_codes, effective_date)
        target_codes = sorted(target_weights)

        return StrategySelectionResult(
            requested_date=target_date,
            signal_date=effective_date,
            entry_codes=entry_codes,
            exit_codes=exit_codes,
            target_codes=target_codes,
            target_weights=target_weights,
            metadata={
                "strategy_name": strategy.name,
                "universe_type": strategy.universe.type,
                "index_id": strategy.universe.index_id,
                "eligible_count": (
                    None
                    if strategy.universe.type == "all_a"
                    else len(self._eligible_codes(strategy, effective_date))
                ),
                "mode": strategy.mode,
                "rebalance_frequency": strategy.rebalance.frequency,
                "entry_trigger": strategy.entry.trigger,
                "exit_trigger": None if strategy.exit is None else strategy.exit.trigger,
            },
        )


__all__ = ["StrategySelector", "StrategySelectionResult"]

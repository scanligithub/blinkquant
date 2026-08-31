import datetime
import enum
from dataclasses import dataclass
from typing import Optional, Union
from pathlib import Path
import polars as pl

from core.engine import selection_engine, SelectionEngine
from core.raw_price_store import RawPriceStore
from core.portfolio import Portfolio, Position
from core.execution import ExecutionEngine, OrderIntent, Fill
from core.data_manager import data_manager
from core.backtest_types import (
    FeeConfig, ExecutionConfig, Allocator, MVP_EXECUTION_CONFIG,
    equal_weight_allocator, SelectionResult, FeeSchedule,
    BacktestDataIntegrityError, BacktestLedgerError,
)
from core.checkpoint import BacktestCheckpoint, save_checkpoint, load_checkpoint
from core.corporate_actions import CorporateAction
from core.signal_trace import SignalTraceData, CodeTrace


class EventPhase(enum.Enum):
    """Event State Machine phases (from 2026-08-27 spec)."""
    PRE_OPEN = "pre_open"              # thaw + corporate actions
    POST_CLOSE_SIGNAL = "post_close_signal"  # selection scheduling
    POST_EXECUTION = "post_execution"  # execute pending intents
    MARKET_CLOSE = "market_close"      # load daily prices
    VALUATION = "valuation"            # equity calc + ledger check
    CHECKPOINT = "checkpoint"          # equity curve + position snapshot


class TradingCalendar:
    """交易日历：提供交易日序列、下一个交易日、信号日期范围。"""
    
    def __init__(self):
        self._trade_dates: list[datetime.date] = []
        self._date_set: set = set()
    
    def set_trade_dates(self, dates: list[datetime.date]):
        """设置交易日列表（已排序）。"""
        self._trade_dates = sorted(dates)
        self._date_set = set(self._trade_dates)
    
    def next_trade_day(self, date: datetime.date) -> datetime.date:
        """获取严格晚于 date 的下一个交易日。

        越界（日历不覆盖）时 fail-fast 抛错——静默折叠到同日会违反 T+1 契约
        （真实案例：2024-12-31 信号曾被折叠为同日执行/估值）。
        """
        if not self._trade_dates:
            next_day = date + datetime.timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += datetime.timedelta(days=1)
            return next_day

        for d in self._trade_dates:
            if d > date:
                return d
        raise ValueError(
            f"TradingCalendar 不覆盖 {date} 之后的交易日："
            f"数据窗口必须包含 end_signal_date 的次一交易日（T+1 执行/估值需要）。"
            f"当前日历范围 {self._trade_dates[0]} .. {self._trade_dates[-1]}"
        )
    
    def signal_range(self, start_date: datetime.date, end_signal_date: datetime.date) -> list[datetime.date]:
        """获取 [start_date, end_signal_date] 内的所有交易日。"""
        if not self._trade_dates:
            result = []
            current = start_date
            while current <= end_signal_date:
                if current.weekday() < 5:
                    result.append(current)
                current += datetime.timedelta(days=1)
            return result
        
        return [d for d in self._trade_dates if start_date <= d <= end_signal_date]

    def trade_range(self, start_date: datetime.date, end_date: datetime.date) -> list[datetime.date]:
        """[start, end] 内全部交易日（估值连续性用）。"""
        return self.signal_range(start_date, end_date)

    def weekly_signal_dates(self, start_date: datetime.date,
                            end_signal_date: datetime.date) -> set[datetime.date]:
        """每周最后一个交易日（ISO 周分组，继承日历语义而非 weekday()==4）。

        节假日截短的周自动取该周实际最后交易日。
        """
        in_range = self.trade_range(start_date, end_signal_date)
        weeks: dict[tuple[int, int], datetime.date] = {}
        for d in in_range:
            iso = d.isocalendar()
            key = (iso[0], iso[1])
            prev = weeks.get(key)
            if prev is None or d > prev:
                weeks[key] = d
        return set(weeks.values())


@dataclass
class BacktestResult:
    equity_curve: 'pl.DataFrame'
    trades: 'pl.DataFrame'
    positions_daily: 'pl.DataFrame'
    metrics: dict
    execution_diagnostics: dict = None
    signal_traces: dict = None  # Dict[signal_date_str, SignalTraceData]


class BacktestEngine:
    """
    回测引擎：Signal Calendar 模型，逐日事件循环。
    
    核心流程：
    for signal_date in calendar.signal_range(start_date, end_signal_date):
        execution_date = calendar.next_trade_day(signal_date)
        
        1. 市场状态 as-of(signal_date)
        2. 策略信号 → SelectionEngine(target_date=signal_date) → SelectionResult
        3. 目标组合 → allocator(codes, signal_date) → target_weights
        4. 订单意图 → diff(current_portfolio, target_weights) → OrderIntent[]
        5. 执行 → ExecutionEngine.execute(execution_date, intents) → fills
        7. 组合更新 → Portfolio.apply_fills(fills) → cash/positions/equity
        7. 估值 → raw_close(execution_date) → mark_to_market
        8. 快照 → AccountSnapshot
    """
    
    def __init__(
        self,
        calendar: 'TradingCalendar',
        selection_engine: 'SelectionEngine',
        raw_price_store: 'RawPriceStore',
        fee_config: 'FeeConfig',
        execution_config: 'ExecutionConfig' = None,
        allocator: 'Allocator' = None,
    ):
        self.calendar = calendar
        self.selection_engine = selection_engine
        self.raw_price_store = raw_price_store
        self.fee_config = fee_config
        self.execution_config = execution_config or MVP_EXECUTION_CONFIG
        self.allocator = allocator or equal_weight_allocator
        
        # 组件将在 run() 中初始化
        self.portfolio = None
        self.execution_engine = None
    
    def run(
        self,
        formula: str,
        start_date: datetime.date,
        end_signal_date: datetime.date,
        initial_cash: float = 1_000_000,
        initial_positions: dict[str, Position] = None,
        initial_state: dict = None,
        rebalance_freq: str = "daily",
        ranking_fn=None,
        top_n: int = 20,
        corporate_action_store: 'CorporateActionStore' = None,
        universe_filter: 'UniverseFilter' = None,
        fee_schedule: 'FeeSchedule' = None,
    ) -> 'BacktestResult':
        """
        运行回测。

        Args:
            formula: 选股公式
            start_date: 回测开始日期（signal_date 起始）
            end_signal_date: 最后允许产生信号的日期
            initial_cash: 初始资金
            initial_positions: 初始持仓（可选）
            initial_state: 完整 checkpoint（export_state 输出，含 cash/positions/
                last_close）；给定时代替 initial_positions，并覆盖停牌 carry 价缓存，
                实现 C2 式跨进程断点续跑。
            rebalance_freq: "daily"（每交易日调仓）或 "weekly"（每周最后一个交易日
                产生信号，次一交易日开盘执行）。仅控制 signal/order 生成频率；
                **估值仍逐日进行**，保证跨频率收益/回撤可比。
            ranking_fn: 可选 ranking 函数，用于在 eligible set 内评分排序。
                        None 时使用 code_asc（向后兼容）。
            top_n: ranking 模式下的最大持仓数量
            corporate_action_store: 可选公司行为存储。每个交易日查询当天发生的
                分红/送股/拆股等行为，自动调整 Portfolio 的持股数量、成本和现金。
                None 时跳过公司行为处理（向后兼容）。
            universe_filter: 可选 UniverseFilter，用于在 selection 前过滤 eligible
                codes（如 IPO 不足 N 天、排除 ST 等）。None 时不过滤（向后兼容）。
            fee_schedule: 可选 FeeSchedule，用于按日期查询历史费率。None 时使用
                固定 fee_config（向后兼容）。
"""
        if rebalance_freq not in ("daily", "weekly"):
            raise ValueError(f"rebalance_freq 仅支持 daily/weekly，收到 {rebalance_freq!r}")
        # 初始化组合（Phase 0 契约：initial_positions 可选，默认空仓）
        self.portfolio = Portfolio(initial_cash=initial_cash)
        
        # Handle checkpoint restore (new v1.0 format vs legacy dict)
        is_new_checkpoint = isinstance(initial_state, BacktestCheckpoint)
        legacy_state = None
        if is_new_checkpoint:
            # New format: BacktestCheckpoint object
            cp = initial_state
            legacy_state = {
                "portfolio": {"cash": cp.cash, "positions": cp.positions},
                "last_close": {item["code"]: item["close"] for item in cp.last_close},
                "thru_thaw": cp.thru_thaw,
                "selected_thru": cp.selected_thru,
                "pending": {
                    "signal_date": cp.pending_signal_date,
                    "execution_date": cp.pending_execution_date,
                    "intents": cp.pending_intents,
                    "prices": cp.pending_prices,
                } if cp.pending_signal_date else None,
            }
            # Restore full engine state
            self._restore_from_checkpoint(cp)
        elif initial_state:
            # Legacy format: dict
            legacy_state = initial_state
            self.portfolio.import_state(initial_state)
        elif initial_positions:
            self.portfolio.load_initial_positions(initial_positions)
        
        # 初始化执行引擎（使用注入的配置）
        self.execution_engine = ExecutionEngine(
            exec_config=self.execution_config,
            fee_config=self.fee_config,
        )
        
        # 记录结果
        equity_curve_rows = []
        trades_rows = []
        positions_daily_rows = []
        
        # Diagnostics accumulator (persisted across checkpoints)
        if is_new_checkpoint and hasattr(self, '_diag_accum'):
            diag = self._diag_accum
            # Ensure all required keys exist
            for k in ("rej_counters", "intents_total", "partial_fill_count", "carried_events",
                      "zero_price_trade_count", "t1_violation_count", "negative_cash_count",
                      "accounting_invariant_violations", "target_gross_by_date"):
                if k not in diag:
                    diag[k] = {} if k in ("rej_counters", "target_gross_by_date") else 0
        else:
            diag = {
                "rej_counters": {},
                "intents_total": 0,
                "partial_fill_count": 0,
                "carried_events": 0,
                "zero_price_trade_count": 0,
                "t1_violation_count": 0,
                "negative_cash_count": 0,
                "accounting_invariant_violations": 0,
                "target_gross_by_date": {},
            }
        self._diag_accum = diag  # Persist reference for checkpointing
        
        # 停牌 carry-forward 估值的最后可用价（derived 规则，非官方复牌基准）
        self._last_close: dict[str, float] = {}
        if self.portfolio.positions and not is_new_checkpoint:
            # For new checkpoint, _last_close already restored in _restore_from_checkpoint
            self._prime_last_close(
                codes=list(self.portfolio.positions.keys()),
                before=start_date,
            )
        if legacy_state and legacy_state.get("last_close"):
            # checkpoint 注入优先：覆盖 prime 结果，保证与连续跑逐字段一致
            self._last_close.update(legacy_state["last_close"])
        
        self._initial_state = legacy_state if legacy_state else initial_state
        
        prev_equity = initial_cash
        
        # ---- 调仓日集合与估值日序列（signal/order 频率与估值频率解耦）----
        allowed_signals = set(self.calendar.signal_range(start_date, end_signal_date))
        if rebalance_freq == "weekly":
            allowed_signals &= self.calendar.weekly_signal_dates(start_date, end_signal_date)
        
        if allowed_signals:
            exec_end = self.calendar.next_trade_day(max(allowed_signals))  # 越界 fail-fast
        else:
            exec_end = start_date
        all_days = self.calendar.trade_range(start_date, exec_end)
        
        # 待执行意图（信号日调度，次一交易日开盘执行）
        self._pend_sig = None
        self._pend_exec = None
        self._pend_intents: list = []
        self._pend_prices: dict = {}
        
        # 解冻游标：checkpoint 续跑时避免重复解冻同一交易日
        if not is_new_checkpoint:
            _tt = (legacy_state or {}).get("thru_thaw")
            if isinstance(_tt, str):
                _tt = datetime.date.fromisoformat(_tt)
            self._thru_thaw = _tt
            # 选股游标与在途意图恢复（跨段不重不漏）
            self._selected_thru = None
            st_pending = (legacy_state or {}).get("pending")
            if st_pending:
                ps = st_pending.get("signal_date")
                pe = st_pending.get("execution_date")
                if isinstance(ps, str):
                    ps = datetime.date.fromisoformat(ps)
                if isinstance(pe, str):
                    pe = datetime.date.fromisoformat(pe)
                self._pend_sig, self._pend_exec = ps, pe
                self._pend_intents = [OrderIntent(**o) for o in st_pending.get("intents", [])]
                self._pend_prices = st_pending.get("prices", {})
            if (legacy_state or {}).get("selected_thru"):
                _stt = legacy_state["selected_thru"]
                if isinstance(_stt, str):
                    _stt = datetime.date.fromisoformat(_stt)
                self._selected_thru = _stt
        # For new checkpoint, _thru_thaw, _selected_thru, _pend_* already restored in _restore_from_checkpoint
        
        # Processed corporate actions tracker
        if not is_new_checkpoint:
            self._processed_actions = []
        
        for t in all_days:
            # PRE_OPEN: thaw + corporate actions
            self._phase_pre_open(t, corporate_action_store, fee_schedule, diag)

            # POST_CLOSE_SIGNAL: selection scheduling
            new_sig, new_exec, new_intents, new_prices = self._phase_post_close_signal(
                t, allowed_signals, formula, ranking_fn, top_n, universe_filter,
                corporate_action_store, diag,
            )

            # POST_EXECUTION: execute pending intents
            fills, cur_signal_date, cur_prices = self._phase_post_execution(
                t, fee_schedule, diag,
            )

            # Commit new scheduled intents
            if new_sig is not None:
                self._pend_sig, self._pend_exec = new_sig, new_exec
                self._pend_intents, self._pend_prices = new_intents, new_prices
                self._selected_thru = t

            for fill in fills:
                trades_rows.append({
                    "signal_date": cur_signal_date,
                    "execution_date": t,
                    "code": fill.code, "side": fill.side,
                    "qty": fill.qty, "price": fill.price, "fee": fill.fee,
                })

            # MARKET_CLOSE: load daily prices
            day_px = self._phase_market_close(t, cur_signal_date, cur_prices)

            # VALUATION: equity calc + ledger check
            equity, positions_value = self._phase_valuation(t, day_px, diag)

            # CHECKPOINT: equity curve + position snapshot
            self._phase_checkpoint(
                t, equity, positions_value, cur_signal_date,
                equity_curve_rows, positions_daily_rows,
            )

        # 构建结果
        equity_curve = pl.DataFrame(equity_curve_rows) if equity_curve_rows else pl.DataFrame(schema={
            "date": pl.Date, "equity": pl.Float64, "cash": pl.Float64, "positions_value": pl.Float64, "signal_date": pl.Date
        })
        
        trades = pl.DataFrame(trades_rows) if trades_rows else pl.DataFrame(schema={
            "signal_date": pl.Date, "execution_date": pl.Date, "code": pl.Utf8, "side": pl.Utf8, "qty": pl.Int64, "price": pl.Float64, "fee": pl.Float64
        })
        
        positions_daily = pl.DataFrame(positions_daily_rows) if positions_daily_rows else pl.DataFrame(schema={
            "date": pl.Date, "code": pl.Utf8, "qty": pl.Int64, "cost": pl.Float64, "market_value": pl.Float64
        })
        
        metrics = self._calculate_metrics(pl.DataFrame(equity_curve_rows) if equity_curve_rows else pl.DataFrame())
        
        # 诊断契约：拒单分类计数器挂到引擎实例（脚本/Metrics 层读取）
        self.rej_counters = diag["rej_counters"]
        self.rejections_total = sum(diag["rej_counters"].values())
        
        # Collect signal traces
        signal_traces = getattr(self, '_signal_traces', {})
        
        return BacktestResult(
            equity_curve=pl.DataFrame(equity_curve_rows) if equity_curve_rows else pl.DataFrame(schema={
                "date": pl.Date, "equity": pl.Float64, "cash": pl.Float64, "positions_value": pl.Float64, "signal_date": pl.Date
            }),
            trades=pl.DataFrame(trades_rows) if trades_rows else pl.DataFrame(schema={
                "signal_date": pl.Date, "execution_date": pl.Date, "code": pl.Utf8, "side": pl.Utf8, "qty": pl.Int64, "price": pl.Float64, "fee": pl.Float64
            }),
            positions_daily=pl.DataFrame(positions_daily_rows) if positions_daily_rows else pl.DataFrame(schema={
                "date": pl.Date, "code": pl.Utf8, "qty": pl.Int64, "cost": pl.Float64, "market_value": pl.Float64
            }),
            metrics=metrics,
            execution_diagnostics=diag,
            signal_traces=getattr(self, '_signal_traces', {}),
        )
    
    def export_state(self) -> dict:
        """导出 checkpoint（账户状态 + carry 缓存 + 解冻/选股游标 + 在途意图）。
        
        Legacy format - use export_checkpoint() for new v1.0 format.
        """
        return {
            "portfolio": self.portfolio.export_state(),
            "last_close": dict(self._last_close),
            "thru_thaw": self._thru_thaw.isoformat() if self._thru_thaw else None,
            "selected_thru": self._selected_thru.isoformat() if self._selected_thru else None,
            "pending": {
                "signal_date": self._pend_sig.isoformat() if self._pend_sig else None,
                "execution_date": self._pend_exec.isoformat() if self._pend_exec else None,
                "intents": [
                    {"code": o.code, "side": o.side,
                     "target_qty": o.target_qty, "target_weight": o.target_weight}
                    for o in self._pend_intents
                ],
                "prices": {
                    c: {"open": v["open"], "close": v["close"]}
                    for c, v in self._pend_prices.items()
                },
            } if self._pend_sig is not None else None,
        }

    def export_checkpoint(self, current_date: datetime.date, description: str = "") -> BacktestCheckpoint:
        """Export full checkpoint in v1.0 format."""
        try:
            from core import __version__ as engine_version
        except ImportError:
            engine_version = "unknown"
        
        # Portfolio
        portfolio_state = self.portfolio.export_state()
        
        # Pending orders
        pending_intents = []
        pending_prices = {}
        if self._pend_sig is not None:
            pending_intents = [
                {"code": o.code, "side": o.side,
                 "target_qty": o.target_qty, "target_weight": o.target_weight}
                for o in self._pend_intents
            ]
            pending_prices = {
                c: {"open": v["open"], "close": v["close"]}
                for c, v in self._pend_prices.items()
            }
        
        # Corporate action cursor
        processed_dividends = []
        processed_splits = []
        for action in getattr(self, '_processed_actions', []):
            if action.action_type.value == "DIVIDEND":
                processed_dividends.append({
                    "code": action.code,
                    "ex_date": action.ex_date.isoformat(),
                    "record_date": action.record_date.isoformat() if action.record_date else None,
                    "pay_date": action.pay_date.isoformat() if action.pay_date else None,
                    "amount": action.amount,
                })
            elif action.action_type.value in ("SPLIT", "BONUS"):
                processed_splits.append({
                    "code": action.code,
                    "ex_date": action.ex_date.isoformat(),
                    "ratio": action.ratio,
                })
        
        # Diagnostics
        diagnostics = getattr(self, '_diag_accum', {})
        
        # Last close
        last_close = [{"code": c, "close": v} for c, v in self._last_close.items()]
        
        return BacktestCheckpoint(
            engine_version=engine_version,
            current_date=current_date.isoformat(),
            phase="CHECKPOINT",
            description=description,
            cash=self.portfolio.cash,
            positions=portfolio_state["positions"],
            pending_signal_date=self._pend_sig.isoformat() if self._pend_sig else None,
            pending_execution_date=self._pend_exec.isoformat() if self._pend_exec else None,
            pending_intents=pending_intents,
            pending_prices=pending_prices,
            processed_dividends=processed_dividends,
            processed_splits=processed_splits,
            ca_cursor_date=current_date.isoformat(),
            diagnostics=diagnostics,
            last_close=last_close,
            thru_thaw=self._thru_thaw.isoformat() if self._thru_thaw else None,
            selected_thru=self._selected_thru.isoformat() if self._selected_thru else None,
        )

    def save_checkpoint(self, directory: Union[str, Path], current_date: datetime.date, description: str = "") -> None:
        """Save checkpoint to directory in v1.0 format."""
        cp = self.export_checkpoint(current_date, description)
        save_checkpoint(cp, directory)

    @classmethod
    def load_checkpoint(cls, directory: Union[str, Path]) -> BacktestCheckpoint:
        """Load checkpoint from directory."""
        return load_checkpoint(directory)

    def _restore_from_checkpoint(self, cp: BacktestCheckpoint) -> None:
        """Restore engine state from checkpoint."""
        # Restore portfolio (cash + positions)
        self.portfolio.import_state({
            "cash": cp.cash,
            "positions": cp.positions,
        })
        
        # Restore last_close
        self._last_close = {item["code"]: item["close"] for item in cp.last_close}
        
        # Restore cursors
        if cp.thru_thaw:
            self._thru_thaw = datetime.date.fromisoformat(cp.thru_thaw)
        if cp.selected_thru:
            self._selected_thru = datetime.date.fromisoformat(cp.selected_thru)
        
        # Restore pending orders
        if cp.pending_signal_date:
            self._pend_sig = datetime.date.fromisoformat(cp.pending_signal_date)
            self._pend_exec = datetime.date.fromisoformat(cp.pending_execution_date)
            self._pend_intents = [OrderIntent(**i) for i in cp.pending_intents]
            self._pend_prices = cp.pending_prices
        else:
            self._pend_sig = None
            self._pend_exec = None
            self._pend_intents = []
            self._pend_prices = {}
        
        # Restore CA cursor (store processed action objects)
        self._processed_actions = []
        for d in cp.processed_dividends:
            from core.corporate_actions import ActionType, CorporateAction
            self._processed_actions.append(CorporateAction(
                code=d["code"],
                action_type=ActionType.DIVIDEND,
                ex_date=datetime.date.fromisoformat(d["ex_date"]),
                record_date=datetime.date.fromisoformat(d["record_date"]) if d["record_date"] else None,
                pay_date=datetime.date.fromisoformat(d["pay_date"]) if d["pay_date"] else None,
                amount=d["amount"],
            ))
        for s in cp.processed_splits:
            from core.corporate_actions import ActionType, CorporateAction
            action_type = ActionType.SPLIT if s["ratio"] > 1 else ActionType.BONUS
            self._processed_actions.append(CorporateAction(
                code=s["code"],
                action_type=action_type,
                ex_date=datetime.date.fromisoformat(s["ex_date"]),
                ratio=s["ratio"],
            ))
        
        # Restore diagnostics
        self._diag_accum = cp.diagnostics

    def _prime_last_close(self, codes: list[str], before: datetime.date, lookback_days: int = 60):
        """回看播种初始持仓的最后可用 raw close（供首周期停牌 carry-forward 估值）。

        窗口 [before-lookback, before]（含信号日当日收盘，PIT 安全）；
        仍找不到价的 code 留空，交由严格估值抛错。
        """
        try:
            start = before - datetime.timedelta(days=lookback_days)
            hist = (
                self.raw_price_store.scan_window(start, before)
                .filter(pl.col("code").is_in(codes) & (pl.col("close") > 0))
                .sort(["code", "date"])
                .group_by("code")
                .agg(pl.col("close").last().alias("last_close"))
                .collect()
            )
            for row in hist.iter_rows(named=True):
                self._last_close[row["code"]] = float(row["last_close"])
        except Exception as e:
            logger.warning(f"prime_last_close failed (will fail-fast at valuation if needed): {e}")

    def _generate_intents(self, target_weights: dict[str, float], execution_prices: dict) -> list:
        """生成目标订单意图（Rebalance Planner）。

        职责边界：仅根据 target_value - current_value 与执行价生成目标经济数量。
        不做现金约束、不估算费用——可负担数量/整手取整/涨跌停由 ExecutionEngine
        以注入的 FeeConfig 统一决定，避免两套费用模型不一致。

        语义：
        - BUY：qty = floor(diff / price)，整手取整在执行层完成；
        - 权重为 0 的既有持仓 → 全仓卖出 available_qty（退出目标组合）；
        - 减仓 → qty = floor(-diff / price)，允许零股（A股卖出规则），上限 available_qty。
        """
        intents = []

        total_equity = self.portfolio.cash + sum(p.market_value for p in self.portfolio.positions.values())
        if total_equity <= 0:
            return []

        all_codes = set(target_weights.keys()) | set(self.portfolio.positions.keys())

        # 跨进程确定性：set 迭代顺序受 PYTHONHASHSEED 影响，
        # 多 BUY 现金受限时顺序即决定成交分布 —— 必须排序遍历（验收标准 §7）
        for code in sorted(all_codes):
            weight = target_weights.get(code, 0.0)
            target_value = weight * total_equity
            pos = self.portfolio.positions.get(code)
            current_value = pos.market_value if (pos and pos.total_qty > 0) else 0.0
            diff = target_value - current_value

            price = execution_prices.get(code, {}).get("open", 0)
            if price <= 0:
                continue

            if diff > 0:
                # BUY 目标数量（不做现金上限——执行层以真实费用约束）
                qty = int(diff / price)
                if qty > 0:
                    intents.append(OrderIntent(code=code, side="BUY", target_qty=qty, target_weight=weight))
            elif diff < 0:
                if pos is None or pos.available_qty <= 0:
                    continue
                if weight <= 0:
                    # 退出目标组合：全仓卖出可用数量
                    qty = pos.available_qty
                else:
                    qty = int(-diff / price)
                qty = min(qty, pos.available_qty)
                if qty > 0:
                    intents.append(OrderIntent(code=code, side="SELL", target_qty=qty, target_weight=weight))

        return intents
    
    def _calculate_metrics(self, equity_curve: 'pl.DataFrame') -> dict:
        """计算回测指标（单行净值也给出 total_return/cagr，风险指标需 ≥2 点）。"""
        if equity_curve.is_empty():
            return {}

        eq = equity_curve["equity"]
        first_equity = eq.head(1)[0]
        last_equity = eq.tail(1)[0]
        total_return = (last_equity / first_equity) - 1
        n_days = len(equity_curve)
        years = n_days / 252
        cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

        returns = eq.pct_change().drop_nulls()
        std = returns.std() if len(returns) > 0 else None
        sharpe = (returns.mean() / std * (252 ** 0.5)) if (std is not None and std > 0) else 0

        peak = eq.cum_max()
        drawdown = (eq - peak) / peak
        max_dd = drawdown.min() if len(returns) > 0 else 0

        return {
            "total_return": total_return,
            "cagr": cagr,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "total_days": n_days,
        }

    # ------------------------------------------------------------------
    # Event State Machine phase handlers
    # ------------------------------------------------------------------

    def _phase_pre_open(self, t: datetime.date, corporate_action_store,
                        fee_schedule, diag: dict) -> None:
        """PRE_OPEN: thaw T+1 frozen qty + apply corporate actions."""
        if self._thru_thaw is None or t > self._thru_thaw:
            self.portfolio.daily_thaw()
            self._thru_thaw = t

        if corporate_action_store is not None:
            today_actions = corporate_action_store.query_all(t, t)
            for action in today_actions:
                self.portfolio.apply_corporate_action(action)

    def _phase_post_close_signal(
        self, t: datetime.date, allowed_signals: set,
        formula: str, ranking_fn, top_n: int, universe_filter,
        corporate_action_store, diag: dict,
    ) -> tuple:
        """POST_CLOSE_SIGNAL: selection scheduling for next trade day.

        Returns (new_sig, new_exec, new_intents, new_prices) or (None, None, [], {}).
        """
        new_sig = new_exec = None
        new_intents: list = []
        new_prices: dict = {}
        skip_select = (self._selected_thru is not None and t <= self._selected_thru)
        if t not in allowed_signals or skip_select:
            return None, None, [], {}

        exec_d = self.calendar.next_trade_day(t)
        px_df = self.raw_price_store.load_execution_prices([exec_d])
        new_prices = {
            row["code"]: {"open": row["open"], "close": row["close"]}
            for row in px_df.iter_rows(named=True)
        }

        # Generate SignalTrace for this signal date
        signal_trace = None

        if ranking_fn is not None:
            sel, signal_trace = self.selection_engine.execute_selector_with_trace(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True)
            if hasattr(sel, 'codes') and sel.codes:
                eligible = sel.codes
                if universe_filter is not None and eligible:
                    eligible = universe_filter.filter(eligible, t)
                import datetime as _dt
                lookback_start = t - _dt.timedelta(days=75)
                frame = data_manager.df_daily.filter(
                    (pl.col("date") >= lookback_start) & (pl.col("date") <= t)
                ).filter(pl.col("code").is_in(eligible)) if data_manager.df_daily is not None and eligible else pl.DataFrame()
                ranked = ranking_fn(frame, t) if not frame.is_empty() else pl.DataFrame()
                if not ranked.is_empty():
                    picked = ranked["code"].to_list()[:top_n]
                    weights = {c: 1.0 / len(picked) for c in picked} if picked else {}
                else:
                    weights = {}
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d
        else:
            sel, signal_trace = self.selection_engine.execute_selector_with_trace(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True)
            if not (isinstance(sel, dict) and "error" in sel):
                codes = sel.codes
                if universe_filter is not None and codes:
                    codes = universe_filter.filter(codes, t)
                weights = self.allocator(codes, t)
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d

        # Store signal trace for later enrichment with execution data
        if signal_trace:
            if not hasattr(self, '_signal_traces'):
                self._signal_traces = {}
            self._signal_traces[t.isoformat()] = signal_trace

        return new_sig, new_exec, new_intents, new_prices

    def _phase_post_execution(
        self, t: datetime.date, fee_schedule, diag: dict,
    ) -> tuple:
        """POST_EXECUTION: execute pending intents at open price.

        Returns (fills, cur_signal_date, cur_prices).
        """
        fills = []
        cur_signal_date = None
        cur_prices = None
        if self._pend_sig is not None and t == self._pend_exec:
            cur_signal_date = self._pend_sig
            cur_prices = self._pend_prices
            intent_codes = [i.code for i in self._pend_intents]
            limit_flags = (data_manager.get_limit_flags(t, intent_codes)
                           if intent_codes else {})
            report = self.execution_engine.execute(
                execution_date=t,
                intents=self._pend_intents,
                positions=self.portfolio.positions,
                raw_prices=self._pend_prices,
                cash=self.portfolio.cash,
                limit_flags=limit_flags,
                fee_config=fee_schedule.get_fee_config(t) if fee_schedule is not None else None,
            )
            fills = report.fills
            for r in report.rejections:
                diag["rej_counters"][r.reason] = diag["rej_counters"].get(r.reason, 0) + 1

            fmap = {}
            for f in fills:
                fmap[(f.code, f.side)] = f.qty
                if f.price <= 0:
                    diag["zero_price_trade_count"] += 1
            for it in self._pend_intents:
                fq = fmap.get((it.code, it.side))
                if fq is not None and fq < it.target_qty:
                    diag["partial_fill_count"] += 1

            self.portfolio.apply_fills(fills, t, {})
            if self.portfolio.cash < -1e-6:
                raise BacktestLedgerError(
                    f"execution {t} 后现金为负: {self.portfolio.cash:.2f}"
                )

            # Enrich signal traces with execution data
            if fills and hasattr(self, '_signal_traces'):
                signal_date = self._pend_sig
                if signal_date:
                    trace = self._signal_traces.get(signal_date.isoformat())
                    if trace:
                        # Create a map of execution data by code
                        exec_map = {}
                        for f in fills:
                            if f.code not in exec_map:
                                exec_map[f.code] = {"BUY": None, "SELL": None}
                            exec_map[f.code][f.side] = {
                                "execution_date": t,
                                "price": f.price,
                                "side": f.side,
                                "qty": f.qty,
                                "fee": f.fee,
                            }
                        # Enrich traces with execution data
                        for code_trace in trace.traces:
                            if code_trace.code in exec_map:
                                for side in ("BUY", "SELL"):
                                    if exec_map[code_trace.code][side]:
                                        code_trace.execution = exec_map[code_trace.code][side]
                                        break  # Use first match (BUY preferred)

        return fills, cur_signal_date, cur_prices

    def _phase_market_close(
        self, t: datetime.date, cur_signal_date, cur_prices,
    ) -> dict:
        """MARKET_CLOSE: load daily prices for valuation."""
        if cur_signal_date is not None:
            day_px = {c: {"close": v["close"]} for c, v in cur_prices.items()}
        else:
            px_today = self.raw_price_store.load_execution_prices([t])
            day_px = {row["code"]: {"close": row["close"]}
                      for row in px_today.iter_rows(named=True)}
        for code, v in day_px.items():
            if v["close"] and v["close"] > 0:
                self._last_close[code] = v["close"]
        return day_px

    def _phase_valuation(
        self, t: datetime.date, day_px: dict, diag: dict,
    ) -> tuple:
        """VALUATION: equity calculation + ledger invariant check.

        Returns (equity, positions_value).
        """
        valuation_prices = {}
        for pos in self.portfolio.positions.values():
            if pos.total_qty <= 0:
                continue
            if pos.code in day_px and day_px[pos.code]["close"]:
                valuation_prices[pos.code] = {"close": day_px[pos.code]["close"]}
            elif pos.code in self._last_close:
                valuation_prices[pos.code] = {"close": self._last_close[pos.code]}
                diag["carried_events"] += 1

        equity = self.portfolio.get_equity(valuation_prices, valuation_date=t)

        positions_value = sum(
            pos.total_qty * valuation_prices[code]["close"]
            for code, pos in self.portfolio.positions.items()
            if pos.total_qty > 0
        )
        if abs(equity - (self.portfolio.cash + positions_value)) > 1e-4:
            raise BacktestLedgerError(
                f"valuation {t}: equity({equity:.4f}) != "
                f"cash({self.portfolio.cash:.4f}) + positions({positions_value:.4f})"
            )

        return equity, positions_value

    def _phase_checkpoint(
        self, t: datetime.date, equity: float, positions_value: float,
        cur_signal_date, equity_curve_rows: list, positions_daily_rows: list,
    ) -> None:
        """CHECKPOINT: append equity curve row + position snapshot."""
        equity_curve_rows.append({
            "date": t,
            "equity": equity,
            "cash": self.portfolio.cash,
            "positions_value": positions_value,
            "signal_date": cur_signal_date,
        })
        for code, pos in self.portfolio.positions.items():
            if pos.total_qty > 0:
                positions_daily_rows.append({
                    "date": t, "code": code, "qty": pos.total_qty,
                    "cost": pos.avg_cost, "market_value": pos.market_value,
                })
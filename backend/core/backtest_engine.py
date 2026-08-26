import datetime
from dataclasses import dataclass
from typing import Optional
import polars as pl

from core.engine import selection_engine, SelectionEngine
from core.raw_price_store import RawPriceStore
from core.portfolio import Portfolio, Position
from core.execution import ExecutionEngine, OrderIntent, Fill
from core.data_manager import data_manager
from core.backtest_types import (
    FeeConfig, ExecutionConfig, Allocator, MVP_EXECUTION_CONFIG,
    equal_weight_allocator, SelectionResult,
    BacktestDataIntegrityError, BacktestLedgerError,
)


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


@dataclass
class BacktestResult:
    equity_curve: 'pl.DataFrame'
    trades: 'pl.DataFrame'
    positions_daily: 'pl.DataFrame'
    metrics: dict
    execution_diagnostics: dict = None


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
        """
        # 初始化组合（Phase 0 契约：initial_positions 可选，默认空仓）
        self.portfolio = Portfolio(initial_cash=initial_cash)
        if initial_state:
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
        
        # 拒单分类计数器（Execution Diagnostics 契约，run 结束后经引擎实例读取）
        rej_counters: dict = {}
        
        # Metrics 诊断采集（轻量标量/字典，不改变执行语义）
        diag = {
            "rej_counters": rej_counters,
            "intents_total": 0,
            "partial_fill_count": 0,
            "carried_events": 0,
            "zero_price_trade_count": 0,
            "t1_violation_count": 0,
            "negative_cash_count": 0,           # 违反即抛 BacktestLedgerError，成功路径恒 0
            "accounting_invariant_violations": 0,
            "target_gross_by_date": {},          # execution_date -> Σ target weights
        }
        
        # 停牌 carry-forward 估值的最后可用价（derived 规则，非官方复牌基准）
        self._last_close: dict[str, float] = {}
        if self.portfolio.positions:
            self._prime_last_close(
                codes=list(self.portfolio.positions.keys()),
                before=start_date,
            )
        if initial_state and initial_state.get("last_close"):
            # checkpoint 注入优先：覆盖 prime 结果，保证与连续跑逐字段一致
            self._last_close.update(initial_state["last_close"])

        self._initial_state = initial_state
        
        prev_equity = initial_cash
        
        # 获取信号日期序列
        signal_dates = self.calendar.signal_range(start_date, end_signal_date)
        
        for signal_date in signal_dates:
            execution_date = self.calendar.next_trade_day(signal_date)
            if execution_date <= signal_date:
                raise BacktestLedgerError(
                    f"T+1 边界违反：signal {signal_date} 的 execution_date "
                    f"={execution_date} 不晚于信号日"
                )
            
            # 1. 每日开盘前解冻（T+1 冻结 → T+2 解冻）
            self.portfolio.daily_thaw()
            
            # 2. 策略信号
            selection_result = self.selection_engine.execute_selector(
                formula, "D", None, target_date=signal_date
            )
            
            if isinstance(selection_result, dict) and "error" in selection_result:
                continue
            
            target_codes = selection_result.codes
            
            # 3. 目标组合权重（空目标 = 全现金组合，仍需走统一管线以退出旧仓）
            target_weights = self.allocator(target_codes, signal_date)
            
            # 4. 单次加载本 cycle raw 行情：open 供执行、close 供估值（同一数据贯穿）
            execution_date = self.calendar.next_trade_day(signal_date)
            cycle_prices_df = self.raw_price_store.load_execution_prices([execution_date])
            cycle_prices = {
                row["code"]: {"open": row["open"], "close": row["close"]}
                for row in cycle_prices_df.iter_rows(named=True)
            }
            # 维护最后可用价（仅收正价），供停牌 carry-forward 估值
            for code, px in cycle_prices.items():
                if px["close"] and px["close"] > 0:
                    self._last_close[code] = px["close"]
            
            intents = self._generate_intents(target_weights, cycle_prices)
            diag["intents_total"] += len(intents)
            if target_weights:
                diag["target_gross_by_date"][execution_date] = sum(target_weights.values())
            
            fills = []
            if intents:
                intent_codes = [intent.code for intent in intents]
                limit_flags = data_manager.get_limit_flags(execution_date, intent_codes)
                
                # 5. 执行订单（只产出 Fill + 可解释拒绝标签）
                report = self.execution_engine.execute(
                    execution_date=execution_date,
                    intents=intents,
                    positions=self.portfolio.positions,
                    raw_prices=cycle_prices,
                    cash=self.portfolio.cash,
                    limit_flags=limit_flags,
                )
                fills = report.fills
                for r in report.rejections:
                    rej_counters[r.reason] = rej_counters.get(r.reason, 0) + 1
                
                # partial fill / zero-price 防御计数（意图目标 vs 实际成交）
                fmap = {}
                for f in fills:
                    fmap[(f.code, f.side)] = f.qty
                    if f.price <= 0:
                        diag["zero_price_trade_count"] += 1
                for it in intents:
                    fq = fmap.get((it.code, it.side))
                    if fq is not None and fq < it.target_qty:
                        diag["partial_fill_count"] += 1
                
                # 6. 组合更新（唯一记账入口）+ 现金非负不变量
                self.portfolio.apply_fills(fills, execution_date, {})
                if self.portfolio.cash < -1e-6:
                    raise BacktestLedgerError(
                        f"execution {execution_date} 后现金为负: {self.portfolio.cash:.2f}"
                    )
            
            # 7. 估值三分类：
            #    a) 当日有 raw close → 正常估值
            #    b) 当日缺行情但存在历史价 → 停牌，carry-forward 最后可用价（derived 规则）
            #    c) 从未有任何价格 → BacktestDataIntegrityError（未知资产，fail-fast）
            valuation_prices = {}
            for pos in self.portfolio.positions.values():
                if pos.total_qty <= 0:
                    continue
                if pos.code in cycle_prices and cycle_prices[pos.code]["close"]:
                    valuation_prices[pos.code] = {"close": cycle_prices[pos.code]["close"]}
                elif pos.code in self._last_close:
                    valuation_prices[pos.code] = {"close": self._last_close[pos.code]}
                    diag["carried_events"] += 1
                # 两者皆无 → 不入表，交由 get_equity 严格抛错
            equity = self.portfolio.get_equity(valuation_prices, valuation_date=execution_date)
            
            # 8. 账本恒等式：equity == cash + Σ(total_qty × raw_close)
            positions_value = sum(
                pos.total_qty * valuation_prices[code]["close"]
                for code, pos in self.portfolio.positions.items()
                if pos.total_qty > 0
            )
            if abs(equity - (self.portfolio.cash + positions_value)) > 1e-4:
                raise BacktestLedgerError(
                    f"valuation {execution_date}: equity({equity:.4f}) != "
                    f"cash({self.portfolio.cash:.4f}) + positions({positions_value:.4f})"
                )
            
            # 记录权益曲线（估值日期 = execution_date；保留 signal_date 审计）
            equity_curve_rows.append({
                "date": execution_date,
                "equity": equity,
                "cash": self.portfolio.cash,
                "positions_value": positions_value,
                "signal_date": signal_date,
            })
            
            # 记录持仓快照
            for code, pos in self.portfolio.positions.items():
                if pos.total_qty > 0:
                    positions_daily_rows.append({
                        "date": execution_date,
                        "code": code,
                        "qty": pos.total_qty,
                        "cost": pos.avg_cost,
                        "market_value": pos.market_value,
                    })
            
            # 记录交易
            for fill in fills:
                trades_rows.append({
                    "signal_date": signal_date,
                    "execution_date": execution_date,
                    "code": fill.code,
                    "side": fill.side,
                    "qty": fill.qty,
                    "price": fill.price,
                    "fee": fill.fee,
                })
        
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
        self.rej_counters = rej_counters
        self.rejections_total = sum(rej_counters.values())
        
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
        )
    
    def export_state(self) -> dict:
        """导出 checkpoint（账户状态 + 停牌 carry 价缓存），供断点续跑。

        与 run(initial_state=...) 配对构成 C2 序列化/恢复契约。
        """
        return {
            "portfolio": self.portfolio.export_state(),
            "last_close": dict(self._last_close),
        }

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
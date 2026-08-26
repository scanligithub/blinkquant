import datetime
from dataclasses import dataclass
from typing import Optional
import polars as pl

from core.engine import selection_engine, SelectionEngine
from core.raw_price_store import RawPriceStore
from core.portfolio import Portfolio, Position
from core.execution import ExecutionEngine, OrderIntent, Fill
from core.data_manager import data_manager
from core.backtest_types import FeeConfig, ExecutionConfig, Allocator, MVP_EXECUTION_CONFIG, equal_weight_allocator, SelectionResult


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
        """获取下一个交易日。"""
        if not self._trade_dates:
            next_day = date + datetime.timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += datetime.timedelta(days=1)
            return next_day
        
        for d in self._trade_dates:
            if d > date:
                return d
        return self._trade_dates[-1]
    
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
    ) -> 'BacktestResult':
        """
        运行回测。
        
        Args:
            formula: 选股公式
            start_date: 回测开始日期（signal_date 起始）
            end_signal_date: 最后允许产生信号的日期
            initial_cash: 初始资金
            initial_positions: 初始持仓（可选）
        """
        # 初始化组合（Phase 0 契约：initial_positions 可选，默认空仓）
        self.portfolio = Portfolio(initial_cash=initial_cash)
        if initial_positions:
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
        
        prev_equity = initial_cash
        
        # 获取信号日期序列
        signal_dates = self.calendar.signal_range(start_date, end_signal_date)
        
        for signal_date in signal_dates:
            execution_date = self.calendar.next_trade_day(signal_date)
            
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
            
            # 4. 生成订单意图
            execution_date = self.calendar.next_trade_day(signal_date)
            execution_prices = self.raw_price_store.load_execution_prices([execution_date])
            execution_prices_dict = {
                row["code"]: {"open": row["open"], "close": row["close"]}
                for row in execution_prices.iter_rows(named=True)
            }
            
            intents = self._generate_intents(target_weights, execution_prices_dict)
            
            fills = []
            if intents:
                # 获取限制标记
                all_codes = set()
                for intent in intents:
                    all_codes.add(intent.code)
                limit_flags = data_manager.get_limit_flags(execution_date, list(all_codes))
                
                # 5. 执行订单
                fills = self.execution_engine.execute(
                    execution_date=execution_date,
                    intents=intents,
                    positions=self.portfolio.positions,
                    raw_prices=execution_prices_dict,
                    cash=self.portfolio.cash,
                    limit_flags=limit_flags,
                )
                
                # 7. 组合更新（唯一入口）
                self.portfolio.apply_fills(fills, execution_date, {})
            
            # 7. 估值（使用 execution_date 的 raw_close）
            execution_date = self.calendar.next_trade_day(signal_date)
            raw_prices = self.raw_price_store.load_execution_prices([execution_date])
            raw_prices_dict = {
                row["code"]: {"close": row["close"]}
                for row in raw_prices.iter_rows(named=True)
            }
            
            # 更新持仓市值
            for code, pos in self.portfolio.positions.items():
                if pos.total_qty > 0:
                    raw_close = raw_prices_dict.get(pos.code, {}).get("close", 0)
                    if raw_close > 0:
                        pos.update_market_value(raw_close)
            
            # 记录权益曲线（使用 execution_date 作为估值日期）
            equity = self.portfolio.cash + sum(p.market_value for p in self.portfolio.positions.values())
            equity_curve_rows.append({
                "date": execution_date,  # 估值日期 = execution_date
                "equity": self.portfolio.cash + sum(p.market_value for p in self.portfolio.positions.values()),
                "cash": self.portfolio.cash,
                "positions_value": sum(p.market_value for p in self.portfolio.positions.values()),
                "signal_date": signal_date,  # 保留 signal_date 用于审计
            })
            
            # 记录持仓快照
            for code, pos in self.portfolio.positions.items():
                if pos.total_qty > 0:
                    raw_close = raw_prices_dict.get(pos.code, {}).get("close", 0)
                    if raw_close > 0:
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
        )
    
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

        for code in all_codes:
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
        """计算回测指标。"""
        if equity_curve.is_empty():
            return {}
        
        returns = equity_curve["equity"].pct_change().drop_nulls()
        if len(returns) == 0:
            return {}
        
        total_return = (equity_curve["equity"][-1] / equity_curve["equity"][0]) - 1
        n_days = len(equity_curve)
        years = n_days / 252
        cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        std = returns.std()
        sharpe = (returns.mean() / std * (252 ** 0.5)) if (std is not None and std > 0) else 0
        
        peak = equity_curve["equity"].cum_max()
        drawdown = (equity_curve["equity"] - peak) / peak
        max_dd = drawdown.min()
        
        return {
            "total_return": total_return,
            "cagr": cagr,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "total_days": len(equity_curve),
        }
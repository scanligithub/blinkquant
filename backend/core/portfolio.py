import datetime
from dataclasses import dataclass, field
from typing import Optional
from core.backtest_types import Position as PositionType


@dataclass
class Position:
    """持仓结构（支持 T+1 可卖/冻结语义）。"""
    code: str
    total_qty: int = 0
    available_qty: int = 0
    frozen_qty: int = 0
    avg_cost: float = 0.0
    market_value: float = 0.0
    
    def buy(self, frozen_qty: int, price: float):
        """买入：增加冻结数量，更新平均成本。"""
        if frozen_qty <= 0:
            return
        if self.total_qty == 0:
            self.avg_cost = price
        else:
            total_cost = self.total_qty * self.avg_cost + frozen_qty * price
            self.total_qty += frozen_qty
            self.avg_cost = total_cost / self.total_qty
        self.total_qty += frozen_qty
        self.frozen_qty += frozen_qty
        # market_value 由外部根据 raw_close 更新
    
    def thaw(self, thaw_qty: int):
        """解冻：冻结转可用。"""
        if thaw_qty <= 0:
            return
        thaw_qty = min(thaw_qty, self.frozen_qty)
        self.frozen_qty -= thaw_qty
        self.available_qty += thaw_qty
    
    def sell(self, qty: int, price: float) -> int:
        """卖出：仅使用 available_qty。返回实际卖出数量。"""
        sell_qty = min(qty, self.available_qty)
        if sell_qty <= 0:
            return 0
        self.available_qty -= sell_qty
        self.total_qty -= sell_qty
        # avg_cost 不变（FIFO 简化）
        if self.total_qty == 0:
            self.avg_cost = 0.0
        return sell_qty
    
    def buy(self, frozen_qty: int, price: float):
        """买入：增加冻结数量，更新平均成本。"""
        if frozen_qty <= 0:
            return
        if self.total_qty == 0:
            self.avg_cost = price
        else:
            total_cost = self.total_qty * self.avg_cost + frozen_qty * price
            self.avg_cost = total_cost / (self.total_qty + frozen_qty)
        self.total_qty += frozen_qty
        self.frozen_qty += frozen_qty
    
    def thaw(self, thaw_qty: int):
        """解冻：冻结转可用。"""
        if thaw_qty <= 0:
            return
        thaw_qty = min(thaw_qty, self.frozen_qty)
        self.frozen_qty -= thaw_qty
        self.available_qty += thaw_qty
    
    def sell(self, qty: int, price: float) -> int:
        """卖出：仅使用 available_qty。返回实际卖出数量。"""
        sell_qty = min(qty, self.available_qty)
        if sell_qty <= 0:
            return 0
        self.available_qty -= sell_qty
        self.total_qty -= sell_qty
        if self.total_qty == 0:
            self.avg_cost = 0.0
        return sell_qty
    
    def thaw(self, thaw_qty: int):
        """解冻：冻结转可用。"""
        if thaw_qty <= 0:
            return
        thaw_qty = min(thaw_qty, self.frozen_qty)
        self.frozen_qty -= thaw_qty
        self.available_qty += thaw_qty
    
    def update_market_value(self, raw_close: float):
        """按 raw_close 更新市值。"""
        self.market_value = self.total_qty * raw_close
    
    def __post_init__(self):
        # 确保一致性
        if self.total_qty == 0:
            self.avg_cost = 0.0
            self.available_qty = 0
            self.frozen_qty = 0
        else:
            # 确保 available + frozen = total
            if self.available_qty + self.frozen_qty != self.total_qty:
                # 修正：优先保证 available，剩余算 frozen
                self.frozen_qty = max(0, self.total_qty - self.available_qty)


@dataclass
class AccountSnapshot:
    """逐日账户快照。"""
    date: datetime.date
    cash: float
    positions: dict[str, 'Position']
    equity: float
    daily_pnl: float


class Portfolio:
    """
    投资组合管理：现金、持仓、权益、逐日快照、T+1 解冻。
    """
    
    def __init__(self, initial_cash: float = 1_000_000):
        self.cash: float = initial_cash
        self.positions: dict[str, Position] = {}
        self.initial_cash = initial_cash
        self._daily_pnl: float = 0.0
    
    def get_equity(self, raw_prices: dict[str, dict]) -> float:
        """计算总权益 = 现金 + 持仓市值（按 raw_close）。"""
        positions_value = 0.0
        for code, pos in self.positions.items():
            if pos.total_qty > 0:
                raw_close = raw_prices.get(pos.code, {}).get("close", 0)
                if raw_close > 0:
                    pos.update_market_value(raw_close)
                    positions_value += pos.market_value
        return self.cash + positions_value
    
    def get_equity_curve_value(self, raw_prices: dict[str, dict]) -> float:
        """获取当前权益值（用于 equity curve）。"""
        return self.get_equity(raw_prices)
    
    def apply_fills(self, fills: list, execution_date: datetime.date, raw_prices: dict) -> float:
        """
        应用成交回报，更新持仓和现金。
        返回发生的总费用。
        """
        total_fee = 0.0
        for fill in fills:
            if fill.side == "BUY":
                self.cash -= fill.qty * fill.price + fill.fee
                pos = self.positions.get(fill.code)
                if pos is None:
                    pos = Position(code=fill.code, total_qty=0, available_qty=0, frozen_qty=0, avg_cost=0.0, market_value=0.0)
                    self.positions[fill.code] = pos
                pos.buy(fill.qty, fill.price)
                self.cash -= fill.fee
            elif fill.side == "SELL":
                pos = self.positions.get(fill.code)
                if pos:
                    sold = pos.sell(fill.qty, fill.price)
                    self.cash += fill.qty * fill.price - fill.fee
                    if pos.total_qty == 0:
                        del self.positions[fill.code]
            total_fee += fill.fee
        return 0.0  # fee already deducted in cash
    
    def daily_thaw(self):
        """每日开盘前解冻：T+1 冻结 → T+2 解冻为可用。"""
        for pos in self.positions.values():
            if pos.frozen_qty > 0:
                pos.thaw(pos.frozen_qty)
    
    def get_positions_snapshot(self) -> dict[str, dict]:
        """获取持仓快照用于记录。"""
        return {
            code: {
                "code": pos.code,
                "total_qty": pos.total_qty,
                "available_qty": pos.available_qty,
                "frozen_qty": pos.frozen_qty,
                "avg_cost": pos.avg_cost,
                "market_value": pos.market_value,
            }
            for code, pos in self.positions.items()
        }
    
    def get_total_positions_value(self, raw_prices: dict[str, dict]) -> float:
        """获取持仓总市值。"""
        total = 0.0
        for code, pos in self.positions.items():
            if pos.total_qty > 0:
                raw_close = raw_prices.get(pos.code, {}).get("close", 0)
                if raw_close > 0:
                    total += pos.total_qty * raw_close
        return total
    
    def snapshot(self, date: datetime.date, raw_prices: dict) -> 'AccountSnapshot':
        """生成当日账户快照。"""
        equity = self.get_equity({})
        return AccountSnapshot(
            date=date,
            cash=self.cash,
            positions=self.positions.copy(),
            equity=equity,
            daily_pnl=self._daily_pnl
        )
    
    def update_daily_pnl(self, prev_equity: float, current_equity: float):
        """更新日度盈亏。"""
        self._daily_pnl = current_equity - prev_equity
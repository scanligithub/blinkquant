import datetime
from dataclasses import dataclass
from typing import Optional

from core.backtest_types import BacktestDataIntegrityError


@dataclass
class Position:
    """持仓结构（支持 T+1 可卖/冻结语义）。

    不变量：available_qty + frozen_qty == total_qty（由 __post_init__ 修正）。
    """
    code: str
    total_qty: int = 0
    available_qty: int = 0
    frozen_qty: int = 0
    avg_cost: float = 0.0
    market_value: float = 0.0

    def buy(self, qty: int, price: float):
        """买入：数量计入冻结（T+1 不可卖），加权更新平均成本。"""
        if qty <= 0:
            return
        total_cost = self.total_qty * self.avg_cost + qty * price
        self.total_qty += qty
        self.avg_cost = total_cost / self.total_qty if self.total_qty > 0 else 0.0
        self.frozen_qty += qty

    def thaw(self, thaw_qty: int):
        """解冻：冻结转可用（T+1 新买入 → 次交易日可卖）。"""
        if thaw_qty <= 0:
            return
        thaw_qty = min(thaw_qty, self.frozen_qty)
        self.frozen_qty -= thaw_qty
        self.available_qty += thaw_qty

    def sell(self, qty: int, price: float) -> int:
        """卖出：仅使用 available_qty（T+1 冻结不可卖）。返回实际卖出数量。

        avg_cost 不变（MVP FIFO 简化）；清仓时归零。
        """
        sell_qty = min(qty, self.available_qty)
        if sell_qty <= 0:
            return 0
        self.available_qty -= sell_qty
        self.total_qty -= sell_qty
        if self.total_qty == 0:
            self.avg_cost = 0.0
        return sell_qty

    def update_market_value(self, raw_close: float):
        """按 raw_close 更新市值。"""
        self.market_value = self.total_qty * raw_close

    def __post_init__(self):
        # 不变量 fail-fast：available + frozen 必须等于 total（禁止静默修正掩盖上游 bug）
        if self.total_qty == 0:
            if self.available_qty != 0 or self.frozen_qty != 0:
                raise ValueError(
                    f"持仓不变量违反 {self.code}: total_qty=0 但 "
                    f"available={self.available_qty}/frozen={self.frozen_qty}"
                )
            self.avg_cost = 0.0
        elif self.available_qty + self.frozen_qty != self.total_qty:
            raise ValueError(
                f"持仓不变量违反 {self.code}: "
                f"available({self.available_qty}) + frozen({self.frozen_qty}) "
                f"!= total({self.total_qty})"
            )


@dataclass
class AccountSnapshot:
    """逐日账户快照。"""
    date: datetime.date
    cash: float
    positions: dict[str, 'Position']
    equity: float
    daily_pnl: float


class Portfolio:
    """投资组合：现金、持仓、权益、逐日快照、T+1 解冻。

    所有资金/持仓变更的唯一入口（ExecutionEngine 只产出 Fill，
    现金与持仓的记账一律经 apply_fills / load_initial_positions 完成）。
    """

    def __init__(self, initial_cash: float = 1_000_000):
        self.cash: float = initial_cash
        self.positions: dict[str, Position] = {}
        self.initial_cash = initial_cash
        self._daily_pnl: float = 0.0

    def load_initial_positions(self, positions: dict[str, 'Position']):
        """加载回测初始持仓（Phase 0 契约：initial_positions 可选，默认空仓）。

        加载前校验不变量 available+frozen==total，不一致直接拒绝（fail-fast）。
        """
        for code, pos in positions.items():
            if pos.available_qty + pos.frozen_qty != pos.total_qty:
                raise ValueError(
                    f"初始持仓不变量违反 {code}: "
                    f"available({pos.available_qty}) + frozen({pos.frozen_qty}) "
                    f"!= total({pos.total_qty})"
                )
            self.positions[code] = Position(
                code=pos.code,
                total_qty=pos.total_qty,
                available_qty=pos.available_qty,
                frozen_qty=pos.frozen_qty,
                avg_cost=pos.avg_cost,
                market_value=pos.market_value,
            )

    def export_state(self) -> dict:
        """导出完整账户状态（跨年 checkpoint / restore 契约）。

        last_close 由引擎层随 state 一并序列化（见 BacktestEngine.export_state）。
        """
        return {
            "cash": self.cash,
            "positions": [
                {"code": p.code, "total_qty": p.total_qty,
                 "available_qty": p.available_qty, "frozen_qty": p.frozen_qty,
                 "avg_cost": p.avg_cost, "market_value": p.market_value}
                for p in self.positions.values()
            ],
        }

    def import_state(self, state: dict):
        """恢复账户状态（cash + positions），校验持仓不变量。"""
        self.cash = float(state["cash"])
        positions = {}
        for p in state.get("positions", []):
            pos = Position(
                code=p["code"], total_qty=p["total_qty"],
                available_qty=p["available_qty"], frozen_qty=p["frozen_qty"],
                avg_cost=p["avg_cost"], market_value=p.get("market_value", 0.0),
            )   # Position.__post_init__ 会 fail-fast 校验不变量
            positions[p["code"]] = pos
        self.positions = positions

    def get_equity(self, raw_prices: dict[str, dict], valuation_date=None) -> float:
        """总权益 = 现金 + 持仓市值（按 raw_close 估值）。

        严格语义：任何持仓股在估值日缺失 close 或 close<=0 →
        抛 BacktestDataIntegrityError（数据缺失 ≠ 停牌，不允许静默跳过或沿用陈旧市值）。
        """
        positions_value = 0.0
        for code, pos in self.positions.items():
            if pos.total_qty > 0:
                raw_close = raw_prices.get(code, {}).get("close", 0)
                if not raw_close or raw_close <= 0:
                    raise BacktestDataIntegrityError(code, valuation_date)
                pos.update_market_value(raw_close)
                positions_value += pos.market_value
        return self.cash + positions_value

    def get_equity_curve_value(self, raw_prices: dict[str, dict]) -> float:
        return self.get_equity(raw_prices)

    def apply_fills(self, fills: list, execution_date: datetime.date, raw_prices: dict) -> float:
        """应用成交回报：唯一的资金/持仓变更入口。返回总费用。

        BUY：扣减 cash（金额+费），持仓进冻结；
        SELL：按实际可卖成交，cash 增加（金额-费），清仓则移除。
        """
        total_fee = 0.0
        for fill in fills:
            if fill.side == "BUY":
                cost = fill.qty * fill.price + fill.fee
                self.cash -= cost
                pos = self.positions.get(fill.code)
                if pos is None:
                    pos = Position(code=fill.code)
                    self.positions[fill.code] = pos
                pos.buy(fill.qty, fill.price)
            elif fill.side == "SELL":
                pos = self.positions.get(fill.code)
                if pos is not None:
                    sold = pos.sell(fill.qty, fill.price)
                    if sold > 0:
                        self.cash += sold * fill.price - fill.fee
                        if pos.total_qty == 0:
                            del self.positions[fill.code]
            total_fee += fill.fee
        return total_fee

    def daily_thaw(self):
        """每日开盘前解冻：前一 execution day 买入的冻结数量转为可卖。"""
        for pos in self.positions.values():
            if pos.frozen_qty > 0:
                pos.thaw(pos.frozen_qty)

    def get_positions_snapshot(self) -> dict[str, dict]:
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
        total = 0.0
        for code, pos in self.positions.items():
            if pos.total_qty > 0:
                raw_close = raw_prices.get(code, {}).get("close", 0)
                if raw_close > 0:
                    total += pos.total_qty * raw_close
        return total

    def snapshot(self, date: datetime.date, raw_prices: dict) -> 'AccountSnapshot':
        """生成当日快照（equity 按 raw_prices 严格估值）。"""
        equity = self.get_equity(raw_prices, valuation_date=date)
        return AccountSnapshot(
            date=date,
            cash=self.cash,
            positions=dict(self.positions),
            equity=equity,
            daily_pnl=self._daily_pnl,
        )

    def apply_corporate_action(self, action: 'CorporateAction'):
        """应用公司行为到持仓。

        - 现金分红：增加现金，调低 avg_cost
        - 送股/转增/拆股：调整持股数量和 avg_cost
        - 配股：暂不实现（raise NotImplementedError）
        """
        from core.corporate_actions import ActionType, adjust_qty_for_split, adjust_avg_cost_for_dividend

        if action.code not in self.positions:
            return  # 不在持仓中，忽略

        pos = self.positions[action.code]

        if action.action_type == ActionType.CASH_DIVIDEND:
            dividend_cash = pos.total_qty * action.cash_dividend_per_share
            self.cash += dividend_cash
            pos.avg_cost = adjust_avg_cost_for_dividend(
                pos.avg_cost, action.cash_dividend_per_share)

        elif action.action_type in (ActionType.STOCK_SPLIT, ActionType.BONUS_SHARES):
            new_total, new_cost, new_avail, new_frozen = adjust_qty_for_split(
                pos.total_qty, pos.avg_cost, action.split_ratio, pos.frozen_qty)
            pos.total_qty = new_total
            pos.avg_cost = new_cost
            pos.available_qty = new_avail
            pos.frozen_qty = new_frozen

        elif action.action_type == ActionType.RIGHTS_ISSUE:
            raise NotImplementedError("配股暂未实现")

    def update_daily_pnl(self, prev_equity: float, current_equity: float):
        self._daily_pnl = current_equity - prev_equity
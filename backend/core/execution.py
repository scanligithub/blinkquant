import datetime
from dataclasses import dataclass
from core.backtest_types import FeeConfig, ExecutionConfig, Position

# A 股买入最小申报单位（整手）。卖出允许零股，不做整手取整。
LOT_SIZE = 100


@dataclass
class OrderIntent:
    code: str
    side: str  # "BUY" or "SELL"
    target_qty: int
    target_weight: float


@dataclass
class Fill:
    code: str
    side: str
    qty: int
    price: float
    fee: float


class ExecutionEngine:
    """
    执行引擎：T+1 开盘、先卖后买、费用、部分成交、T+1 持仓约束、整手取整。

    MVP 冻结配置：
    - price_mode = "open"
    - order_sequence = "sell_first"
    - cash_reinvestment = "same_cycle"
    - partial_fill_policy = "keep_cash"

    职责边界：
    - 只产出 Fill；现金/持仓记账一律由 Portfolio.apply_fills() 完成。
    - BUY 数量受三重约束：意图目标数量（向下取整手）→ 真实 FeeConfig
      的逐笔可负担数量 → 涨跌停/停牌。卖出回款在同一 cycle 内可用于买入。
    """

    def __init__(self, exec_config: ExecutionConfig, fee_config: FeeConfig):
        self.config = exec_config
        self.fee_config = fee_config

    def execute(
        self,
        execution_date: datetime.date,
        intents: list[OrderIntent],
        positions: dict[str, Position],
        raw_prices: dict[str, dict],
        cash: float,
        limit_flags: dict[str, dict] = None,
    ) -> list[Fill]:
        """执行订单意图，只返回成交记录，不修改资金/持仓。

        Args:
            execution_date: 成交日期 (T+1)
            intents: 订单意图列表（目标经济数量，允许非整手）
            positions: 当前持仓字典 code -> Position
            raw_prices: 原始价格字典 code -> {"open": float, "close": float}
            cash: 当前可用现金
            limit_flags: 涨跌停/停牌标记（None 表示不启用限制检查）

        Returns:
            fills 列表（执行顺序：先卖单后买单）
        """
        sells = [i for i in intents if i.side == "SELL"]
        buys = [i for i in intents if i.side == "BUY"]

        fills: list[Fill] = []
        available_cash = cash

        # ---- 先卖后买：卖出回款同 cycle 可用于买入 ----
        for intent in sells:
            pos = positions.get(intent.code)
            if not pos or pos.available_qty <= 0:
                continue
            if not self._can_trade(intent.code, "SELL", limit_flags):
                continue
            # 卖出允许零股：数量仅受 available_qty 约束
            fill_qty = min(intent.target_qty, pos.available_qty)
            if fill_qty <= 0:
                continue
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            fee = self._calc_fee(price * fill_qty, "SELL")
            fills.append(Fill(intent.code, "SELL", fill_qty, price, fee))
            available_cash += price * fill_qty - fee

        for intent in buys:
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            if not self._can_trade(intent.code, "BUY", limit_flags):
                continue
            # 整手向下取整（自然抑制微小调仓噪音）
            lot_qty = (intent.target_qty // LOT_SIZE) * LOT_SIZE
            if lot_qty <= 0:
                continue
            # 以真实 FeeConfig 的逐笔订单费用计算最大可买整手数量
            fill_qty = self._max_affordable_lot_qty(price, available_cash, lot_qty)
            if fill_qty <= 0:
                continue
            fee = self._calc_fee(price * fill_qty, "BUY")
            fills.append(Fill(intent.code, "BUY", fill_qty, price, fee))
            # 同一 cycle 内多笔 BUY 顺序扣减可用现金
            available_cash -= price * fill_qty + fee

        return fills

    def _max_affordable_lot_qty(self, price: float, cash: float, cap_qty: int) -> int:
        """按真实 FeeConfig 计算可负担的最大整手买入数量。

        最低佣金按"一笔订单"判断（max(amount*rate, min)），不做每股摊薄近似。
        从上限整手数量逐手下调，直到 cost+fee <= cash。
        """
        if price <= 0 or cash <= 0 or cap_qty < LOT_SIZE:
            return 0
        qty = min(cap_qty, (int(cash / price) // LOT_SIZE) * LOT_SIZE)
        while qty > 0:
            fee = self._calc_fee(price * qty, "BUY")
            if price * qty + fee <= cash:
                return qty
            qty -= LOT_SIZE
        return 0

    def _can_trade(self, code: str, side: str, limit_flags: dict) -> bool:
        """涨跌停/停牌限制：停牌禁交易；涨停禁买；跌停禁卖。

        limit_flags 为 None 时跳过检查（单元测试便利路径）；
        标记缺失（无该 code 条目）视为停牌（fail-closed）。
        """
        if limit_flags is None:
            return True
        flags = limit_flags.get(code)
        if flags is None:
            return False
        if flags.get("is_suspended", False):
            return False
        if side == "BUY" and flags.get("is_limit_up", False):
            return False
        if side == "SELL" and flags.get("is_limit_down", False):
            return False
        return True

    def _calc_fee(self, amount: float, side: str) -> float:
        """单笔订单费用 = max(佣金率×金额, 最低佣金) + 印花税(仅卖出) + 过户费。"""
        fc = self.fee_config
        commission = max(amount * fc.commission_rate, fc.commission_min)
        stamp_tax = amount * fc.stamp_tax_rate if side == "SELL" else 0.0
        transfer = amount * fc.transfer_fee_rate
        total = round(commission, 2) + round(stamp_tax, 2) + round(transfer, 2)
        return round(total, 2)
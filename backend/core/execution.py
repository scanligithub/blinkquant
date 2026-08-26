import datetime
from dataclasses import dataclass, field
from core.backtest_types import FeeConfig, ExecutionConfig, Position

# A 股买入最小申报单位（整手）。卖出允许零股，不做整手取整。
LOT_SIZE = 100

# 拒单原因分类（Execution Diagnostics 契约）。
# 标签仅用于解释"目标为何未成交"，绝不改变成交语义。
R_SUSPENDED = "SUSPENDED"        # 停牌
R_LIMIT_BLOCKED = "LIMIT_BLOCKED"  # 涨停禁买 / 跌停禁卖
R_FROZEN = "FROZEN"              # SELL 但可用数量为 0（T+1 冻结或空仓）
R_CASH_STARVED = "CASH_STARVED"  # BUY 可用现金不足一手
R_NO_PRICE = "NO_PRICE"          # 执行日缺失 raw open
R_ZERO_TARGET = "ZERO_TARGET"    # planner 输出 qty<=0（防御性）
R_BELOW_LOT = "BELOW_LOT"        # BUY 目标不足一手（经济性碎量）


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


@dataclass
class Rejection:
    """一次未成交意图的可解释标签。"""
    code: str
    side: str
    reason: str
    target_qty: int


@dataclass
class ExecutionReport:
    fills: list = field(default_factory=list)
    rejections: list = field(default_factory=list)   # list[Rejection]

    def reason_counters(self) -> dict:
        counters: dict = {}
        for r in self.rejections:
            counters[r.reason] = counters.get(r.reason, 0) + 1
        return counters


class ExecutionEngine:
    """
    执行引擎：T+1 开盘、先卖后买、费用、部分成交、T+1 持仓约束、整手取整。

    MVP 冻结配置：
    - price_mode = "open"
    - order_sequence = "sell_first"
    - cash_reinvestment = "same_cycle"
    - partial_fill_policy = "keep_cash"

    职责边界：
    - 只产出 ExecutionReport(fills, rejections)；现金/持仓记账一律由
      Portfolio.apply_fills() 完成。
    - BUY 数量受三重约束：意图目标数量（向下取整手）→ 真实 FeeConfig
      的逐笔可负担数量 → 涨跌停/停牌。卖出回款在同一 cycle 内可用于买入。
    - 未成交意图必须携带 Rejection 原因（诊断契约），原因标签不影响任何成交路径。
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
    ) -> ExecutionReport:
        """执行订单意图。

        Returns:
            ExecutionReport(fills, rejections)
        """
        sells = [i for i in intents if i.side == "SELL"]
        buys = [i for i in intents if i.side == "BUY"]

        report = ExecutionReport()
        available_cash = cash

        # ---- 先卖后买：卖出回款同 cycle 可用于买入 ----
        for intent in sells:
            pos = positions.get(intent.code)

            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                report.rejections.append(Rejection(intent.code, "SELL", R_NO_PRICE, intent.target_qty))
                continue
            gate = self._trade_gate(intent.code, "SELL", limit_flags)
            if gate is not None:
                report.rejections.append(Rejection(intent.code, "SELL", gate, intent.target_qty))
                continue
            if not pos or pos.available_qty <= 0:
                report.rejections.append(Rejection(intent.code, "SELL", R_FROZEN, intent.target_qty))
                continue

            # 卖出允许零股：数量仅受 available_qty 约束
            fill_qty = min(intent.target_qty, pos.available_qty)
            if fill_qty <= 0:
                report.rejections.append(Rejection(intent.code, "SELL", R_ZERO_TARGET, intent.target_qty))
                continue

            fee = self._calc_fee(price * fill_qty, "SELL")
            report.fills.append(Fill(intent.code, "SELL", fill_qty, price, fee))
            available_cash += price * fill_qty - fee

        for intent in buys:
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                report.rejections.append(Rejection(intent.code, "BUY", R_NO_PRICE, intent.target_qty))
                continue
            gate = self._trade_gate(intent.code, "BUY", limit_flags)
            if gate is not None:
                report.rejections.append(Rejection(intent.code, "BUY", gate, intent.target_qty))
                continue
            if intent.target_qty <= 0:
                report.rejections.append(Rejection(intent.code, "BUY", R_ZERO_TARGET, intent.target_qty))
                continue

            lot_qty = (intent.target_qty // LOT_SIZE) * LOT_SIZE
            if lot_qty <= 0:
                report.rejections.append(Rejection(intent.code, "BUY", R_BELOW_LOT, intent.target_qty))
                continue

            fill_qty = self._max_affordable_lot_qty(price, available_cash, lot_qty)
            if fill_qty <= 0:
                report.rejections.append(Rejection(intent.code, "BUY", R_CASH_STARVED, intent.target_qty))
                continue

            fee = self._calc_fee(price * fill_qty, "BUY")
            report.fills.append(Fill(intent.code, "BUY", fill_qty, price, fee))
            available_cash -= price * fill_qty + fee

        return report

    @staticmethod
    def _trade_gate(code: str, side: str, limit_flags: dict):
        """涨跌停/停牌闸门。可交易返回 None，否则返回拒绝原因标签。

        limit_flags 为 None 时跳过检查；标记缺失视为停牌（fail-closed）。
        """
        if limit_flags is None:
            return None
        flags = limit_flags.get(code)
        if flags is None:
            return R_SUSPENDED
        if flags.get("is_suspended", False):
            return R_SUSPENDED
        if side == "BUY" and flags.get("is_limit_up", False):
            return R_LIMIT_BLOCKED
        if side == "SELL" and flags.get("is_limit_down", False):
            return R_LIMIT_BLOCKED
        return None

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

    def _calc_fee(self, amount: float, side: str) -> float:
        """单笔订单费用 = max(佣金率×金额, 最低佣金) + 印花税(仅卖出) + 过户费。"""
        fc = self.fee_config
        commission = max(amount * fc.commission_rate, fc.commission_min)
        stamp_tax = amount * fc.stamp_tax_rate if side == "SELL" else 0.0
        transfer = amount * fc.transfer_fee_rate
        total = round(commission, 2) + round(stamp_tax, 2) + round(transfer, 2)
        return round(total, 2)
import datetime
from dataclasses import dataclass
from typing import Optional
from core.backtest_types import FeeConfig, ExecutionConfig, Position


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
    执行引擎：T+1 开盘、先卖后买、费用、部分成交、T+1 持仓约束。
    
    MVP 冻结配置：
    - price_mode = "open"
    - order_sequence = "sell_first"
    - cash_reinvestment = "same_cycle"
    - partial_fill_policy = "keep_cash"
    """
    
    def __init__(self, exec_config: ExecutionConfig, fee_config: FeeConfig):
        self.config = exec_config
        self.fee_config = fee_config
    
    def execute(
        self,
        execution_date: datetime.date,
        intents: list[OrderIntent],
        positions: dict[str, Position],
        raw_prices: dict[str, dict],  # code -> {"open": float, "close": float}
        cash: float,
    ) -> tuple[list[Fill], float]:
        """
        执行订单意图。
        
        Args:
            execution_date: 成交日期 (T+1)
            intents: 订单意图列表
            positions: 当前持仓字典 code -> Position
            raw_prices: 原始价格字典 code -> {"open": float, "close": float}
            cash: 当前可用现金
            
        Returns:
            (fills列表, 剩余现金)
        """
        # 1. 分离买卖意图
        sells = [i for i in intents if i.side == "SELL"]
        buys = [i for i in intents if i.side == "BUY"]
        
        # 2. 先执行卖单
        remaining_cash = cash
        fills = []
        
        for intent in sells:
            pos = positions.get(intent.code)
            if not pos or pos.available_qty <= 0:
                continue
            
            # 检查涨跌停限制（需要从 raw_prices 或 position 获取 limit 标记）
            # 简化：这里假设 raw_prices 已经反映了限制，或由上层过滤
            fill_qty = min(intent.target_qty, pos.available_qty)
            if fill_qty <= 0:
                continue
            
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            fee = self._calc_fee(price * fill_qty, "SELL")
            fills.append(Fill(intent.code, "SELL", fill_qty, price, fee))
            remaining_cash += price * fill_qty - fee
        
        # 3. 执行买单（使用更新后的现金）
        for intent in buys:
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            # 计算单股成本（含费用）
            unit_cost = price * (1 + self.fee_config.commission_rate) + self.fee_config.transfer_fee_rate * price
            # 加上印花税（买入无印花税）
            unit_cost = max(price * (1 + self.fee_config.commission_rate), price + self.fee_config.commission_min) + price * self.fee_config.transfer_fee_rate
            
            # 简化：单股总成本 = price * (1 + commission_rate) + transfer_fee
            # 更精确：commission = max(price * qty * rate, min_commission)
            # 单股近似成本
            est_commission_per_share = max(price * self.fee_config.commission_rate, self.fee_config.commission_min / max(1, 100))  # 粗略估算
            unit_cost = price + est_commission_per_share + price * self.fee_config.transfer_fee_rate
            
            # 计算最大可买数量
            max_affordable_qty = int(remaining_cash / unit_cost) if unit_cost > 0 else 0
            fill_qty = min(intent.target_qty, max_affordable_qty)
            
            if fill_qty <= 0:
                continue
            
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            fee = self._calc_fee(price * fill_qty, "BUY")
            cost = price * fill_qty + fee
            
            if cost > remaining_cash + 1e-9:  # 浮点误差容忍
                # 重新计算可买数量
                fill_qty = 0
                for q in range(1, intent.target_qty + 1):
                    test_fee = self._calc_fee(price * q, "BUY")
                    if price * q + self._calc_fee(price * q, "BUY") <= remaining_cash:
                        fill_qty = q
                    else:
                        break
                if fill_qty <= 0:
                    continue
                fee = self._calc_fee(price * fill_qty, "BUY")
            
            fills.append(Fill(intent.code, "BUY", fill_qty, price, fee))
            remaining_cash -= price * fill_qty + fee
        
        return fills, remaining_cash
    
    def _calc_fee(self, amount: float, side: str) -> float:
        """计算交易费用。
        
        Args:
            amount: 交易金额
            side: "BUY" 或 "SELL"
            
        Returns:
            总费用（佣金 + 印花税 + 过户费），保留2位小数
        """
        fc = self.fee_config
        commission = max(amount * fc.commission_rate, fc.commission_min)
        stamp_tax = amount * fc.stamp_tax_rate if side == "SELL" else 0.0
        transfer = amount * fc.transfer_fee_rate
        # 使用 Decimal 避免浮点误差，或先 round 中间值
        total = round(commission, 2) + round(stamp_tax, 2) + round(transfer, 2)
        return round(total, 2)
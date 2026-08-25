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
    
    关键原则：只返回 fills，不修改 cash/positions。
    所有资金/持仓变更由 Portfolio.apply_fills() 统一处理。
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
    ) -> list[Fill]:
        """
        执行订单意图，只返回成交记录，不修改资金/持仓。
        
        处理逻辑：先卖后买，卖出所得资金当日可用于买入。
        
        Args:
            execution_date: 成交日期 (T+1)
            intents: 订单意图列表
            positions: 当前持仓字典 code -> Position
            raw_prices: 原始价格字典 code -> {"open": float, "close": float}
            cash: 当前可用现金
            
        Returns:
            fills列表（按执行顺序：先卖单后买单）
        """
        # 1. 分离买卖意图
        sells = [i for i in intents if i.side == "SELL"]
        buys = [i for i in intents if i.side == "BUY"]
        
        fills = []
        available_cash = cash
        
        # 1. 先执行卖单
        for intent in sells:
            pos = positions.get(intent.code)
            if not pos or pos.available_qty <= 0:
                continue
            
            # 检查涨跌停/停牌限制
            if not self._can_trade(intent.code, "SELL", execution_date, raw_prices):
                continue
            
            fill_qty = min(intent.target_qty, pos.available_qty)
            if fill_qty <= 0:
                continue
            
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            fee = self._calc_fee(price * fill_qty, "SELL")
            fills.append(Fill(intent.code, "SELL", fill_qty, price, fee))
            # 卖出所得资金立即可用于买入
            available_cash += price * fill_qty - fee
        
        # 2. 执行买单（使用包含卖出回款的可用现金）
        for intent in buys:
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            # 检查涨跌停/停牌
            if not self._can_trade(intent.code, "BUY", execution_date, raw_prices):
                continue
            
            # 计算最大可买数量（基于当前可用现金）
            max_affordable_qty = self._estimate_max_buy_qty(
                intent.code, self.config, self.fee_config, 
                raw_prices.get(intent.code, {}).get("open", 0),
                available_cash
            )
            
            fill_qty = min(intent.target_qty, max_affordable_qty)
            if fill_qty <= 0:
                continue
            
            price = raw_prices.get(intent.code, {}).get("open", 0)
            if price <= 0:
                continue
            
            fee = self._calc_fee(price * fill_qty, "BUY")
            fills.append(Fill(intent.code, "BUY", fill_qty, price, fee))
        
        return fills
    
    def _estimate_max_buy_qty(self, code: str, exec_config: 'ExecutionConfig', fee_config: FeeConfig, price: float, available_cash: float) -> int:
        """估算最大可买数量（供意图生成参考，实际由 Portfolio 决定）"""
        if price <= 0 or available_cash <= 0:
            return 0
        # 粗略估算：单股成本 = price * (1 + commission_rate) + transfer_fee
        est_commission_rate = fee_config.commission_rate
        est_transfer_rate = fee_config.transfer_fee_rate
        est_commission_min = fee_config.commission_min
        unit_cost = price * (1 + est_commission_rate) + price * fee_config.transfer_fee_rate
        unit_cost = max(unit_cost, price + fee_config.commission_min / 100)  # 粗略估算最低佣金分摊
        return max(0, int(available_cash / unit_cost))
    
    def _can_trade(self, code: str, side: str, execution_date: datetime.date, raw_prices: dict) -> bool:
        """检查涨跌停/停牌限制。
        
        实际实现需要从 data_manager 获取 limit_up/down/suspended 字段。
        这里简化：假设 raw_prices 已包含 limit 信息，或由上层过滤。
        """
        # TODO: 实际实现需要从 Position 或数据源获取 limit_up/down/suspended 标记
        # 暂时返回 True，后续完善
        return True
    
    def _calc_fee(self, amount: float, side: str) -> float:
        """计算交易费用。
        
        Args:
            amount: 交易金额
            side: "BUY" 或 "SELL"
            
        Returns:
            总费用（佣金 + 印花税 + 过户费），保留2位小数
        """
        fc = self.fee_config
        commission = max(amount * self.fee_config.commission_rate, self.fee_config.commission_min)
        stamp_tax = amount * self.fee_config.stamp_tax_rate if side == "SELL" else 0.0
        transfer = amount * self.fee_config.transfer_fee_rate
        # 使用 Decimal 避免浮点误差，或先 round 中间值
        total = round(commission, 2) + round(amount * self.fee_config.stamp_tax_rate if side == "SELL" else 0.0, 2) + round(amount * self.fee_config.transfer_fee_rate, 2)
        return round(total, 2)


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
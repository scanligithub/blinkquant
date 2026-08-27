"""
RQAlpha Adapter for blinkquant

Maps blinkquant SelectionResult → RQAlpha orders
Maps RQAlpha Trade/Fill → blinkquant Fill/Trade
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import date
import rqalpha
from rqalpha.api import order_shares, order_target_value
from rqalpha.const import ORDER_TYPE, SIDE


@dataclass
class BlinkIntent:
    """blinkquant OrderIntent 简化版"""
    code: str
    side: str          # "BUY" / "SELL"
    target_qty: int
    target_weight: float = 0.0
    target_price: float = 0.0  # 0 = market


@dataclass
class BlinkFill:
    """blinkquant Fill"""
    code: str
    side: str
    qty: int
    price: float
    fee: float
    signal_date: date
    execution_date: date


class BlinkquantRQAdapter:
    """
    blinkquant ↔ RQAlpha 语义转换层
    
    核心契约：
    - blinkquant signal_date=T → RQAlpha 在 T+1 开盘价成交
    - RQAlpha Trade → blinkquant Fill (逐字段映射)
    """
    
    def __init__(self, calendar):
        self.calendar = calendar
        self._pending_orders: List[BlinkIntent] = []
        self._fills: List[BlinkFill] = []
    
    def create_orders_from_selection(self, selection_result, signal_date: date, 
                                      execution_date: date, prices: Dict) -> List:
        """
        将 SelectionResult 转换为 RQAlpha 订单
        
        Args:
            selection_result: blinkquant SelectionResult
            signal_date: T (信号日期)
            execution_date: T+1 (执行日期)
            prices: {code: {"open": ..., "close": ...}} 执行日价格
            
        Returns:
            RQAlpha order 对象列表
        """
        orders = []
        codes = selection_result.codes
        
        if not codes:
            return orders
        
        # 等权分配
        weight = 1.0 / len(codes)
        
        for code in codes:
            price_info = prices.get(code, {})
            exec_price = price_info.get("open", 0)
            
            if exec_price <= 0:
                continue
            
            # 这里简化：假设固定金额 100000 / N
            # 实际应该根据 portfolio 价值计算
            target_value = 100000 * weight
            target_qty = int(target_value / exec_price / 100) * 100  # 整百股
            
            if target_qty <= 0:
                continue
            
            # 标准化代码格式：blinkquant "sh.600000" → RQAlpha "600000.XSHG"
            rq_code = self._normalize_code(code)
            
            intent = BlinkIntent(
                code=rq_code,
                side="BUY",
                target_qty=target_qty,
                target_weight=weight
            )
            self._pending_orders.append(intent)
            
            # RQAlpha 使用 order_target_value 简化
            # 实际应该用 order_shares 精确控制数量
            orders.append({
                "code": rq_code,
                "quantity": target_qty,
                "side": SIDE.BUY,
                "price": 0,  # 0 = market
                "order_type": ORDER_TYPE.MARKET,
                "signal_date": signal_date,
                "execution_date": execution_date,
            })
        
        return orders
    
    def on_trade(self, trade) -> BlinkFill:
        """
        RQAlpha Trade 回调 → blinkquant Fill
        
        逐字段映射：
        - code: 标准化回 blinkquant 格式
        - side: BUY/SELL
        - qty: 成交数量
        - price: 成交价格
        - fee: 手续费+印花税+过户费
        - signal_date: 从 pending 关联
        - execution_date: trade.datetime.date()
        """
        # 从 pending 找到对应 signal_date
        signal_date = None
        for intent in self._pending_orders:
            if intent.code == trade.order_book_id:
                signal_date = getattr(trade, '_signal_date', None)
                break
        
        fill = BlinkFill(
            code=self._denormalize_code(trade.order_book_id),
            side="BUY" if trade.side == SIDE.BUY else "SELL",
            qty=trade.last_quantity,
            price=trade.last_price,
            fee=trade.commission + trade.tax,
            signal_date=signal_date,
            execution_date=trade.datetime.date() if hasattr(trade, 'datetime') else None
        )
        self._fills.append(fill)
        return fill
    
    def _normalize_code(self, blink_code: str) -> str:
        """blinkquant 'sh.600000' → RQAlpha '600000.XSHG'"""
        if blink_code.startswith("sh."):
            return blink_code[3:] + ".XSHG"
        elif blink_code.startswith("sz."):
            return blink_code[3:] + ".XSHE"
        elif blink_code.startswith("bj."):
            return blink_code[3:] + ".XBSE"
        return blink_code
    
    def _denormalize_code(self, rq_code: str) -> str:
        """RQAlpha '600000.XSHG' → blinkquant 'sh.600000'"""
        if rq_code.endswith(".XSHG"):
            return "sh." + rq_code[:6]
        elif rq_code.endswith(".XSHE"):
            return "sz." + rq_code[:6]
        elif rq_code.endswith(".XBSE"):
            return "bj." + rq_code[:6]
        return rq_code
    
    def get_fills(self) -> List[BlinkFill]:
        return self._fills
    
    def clear_pending(self):
        self._pending_orders.clear()


class BlinkquantRQDataSource:
    """
    blinkquant Parquet → RQAlpha DataSource Adapter
    
    最小实现：实现 RQAlpha 需要的核心数据接口
    """
    
    def __init__(self, parquet_root: str):
        self.parquet_root = parquet_root
        # 实际实现需要读取 Parquet 并实现 BaseDataSource 接口
        pass
    
    # 需要实现的接口（RQAlpha BaseDataSource 抽象方法）：
    # - get_bar(instrument, dt, frequency)
    # - get_price(instrument, start_date, end_date, frequency)
    # - get_instruments()
    # - get_trading_calendar()
    # - get_dividends()
    # - get_splits()
    # - is_suspended(instrument, dt)
    # - is_st_stock(instrument, dt)
    pass
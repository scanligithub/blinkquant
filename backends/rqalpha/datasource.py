"""
RQAlpha Custom DataSource using blinkquant Parquet data

实现 RQAlpha BaseDataSource 接口，直接读取 blinkquant Parquet 文件
"""

from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd
import os


class BlinkquantParquetDataSource(BaseDataSource):
    """
    blinkquant Parquet 数据源适配器
    
    直接读取 blinkquant 的年份分片 Parquet 文件，
    实现 RQAlpha BaseDataSource 接口
    """
    
    def __init__(self, parquet_root: str, total_nodes: int = 1, node_index: int = 0):
        super().__init__(None)  # path 不再使用
        self.parquet_root = parquet_root
        self.total_nodes = total_nodes
        self.node_index = node_index
        
        # 缓存
        self._instruments_cache: Dict[str, Instrument] = {}
        self._calendar_cache: List[date] = []
        self._daily_cache: Dict[str, pd.DataFrame] = {}
        self._adjust_factor_cache: Dict[str, pd.DataFrame] = {}
        
        # 加载基础数据
        self._load_basic_info()
    
    def _load_basic_info(self):
        """加载股票基本信息、交易日历"""
        # 读取 stock_basic.parquet
        basic_path = os.path.join(self.parquet_root, "stock_basic.parquet")
        if os.path.exists(basic_path):
            df = pd.read_parquet(basic_path)
            for _, row in df.iterrows():
                code = row['code']
                self._instruments_cache[code] = Instrument(
                    order_book_id=code,
                    symbol=code.split('.')[-1],
                    abbrev_symbol=row.get('name', ''),
                    industry_code=row.get('industry', ''),
                    industry_name=row.get('industry_name', ''),
                    listed_date=row.get('list_date', date(2000, 1, 1)),
                    de_listed_date=row.get('delist_date', None),
                    type=INSTRUMENT_TYPE.CS,
                    status='Active'
                )
        
        # 读取交易日历
        cal_path = os.path.join(self.parquet_root, "trading_dates.parquet")
        if os.path.exists(cal_path):
            df = pd.read_parquet(cal_path)
            self._calendar_cache = sorted(df['date'].dt.date.tolist())
    
    def get_instruments(self) -> List[Instrument]:
        return list(self._instruments_cache.values())
    
    def get_instrument(self, order_book_id: str) -> Optional[Instrument]:
        return self._instruments_cache.get(order_book_id)
    
    def get_trading_calendar(self) -> List[date]:
        return self._calendar_cache
    
    def get_bar(self, instrument: str, dt: datetime, frequency: str) -> Optional[Dict]:
        """获取单根 K 线"""
        date_str = dt.date()
        df = self._get_daily_data(instrument)
        if df is None:
            return None
        
        row = df[df['date'] == date_str]
        if row.empty:
            return None
        
        row = row.iloc[0]
        return {
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']),
            'datetime': dt,
            'frequency': frequency,
        }
    
    def get_price(self, instrument: str, start_date: date, end_date: date, 
                  frequency: str, fields: List[str] = None) -> pd.DataFrame:
        """获取价格序列"""
        df = self._get_daily_data(instrument)
        if df is None:
            return pd.DataFrame()
        
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df = df.loc[mask].copy()
        
        if fields:
            df = df[fields]
        
        return df
    
    def _get_daily_data(self, instrument: str) -> Optional[pd.DataFrame]:
        """读取日线数据（带分片分片加载）"""
        if instrument in self._daily_cache:
            return self._daily_cache[instrument]
        
        # 确定年份分片
        year = 2024  # 简化：假设都在 2024 年
        path = os.path.join(self.parquet_root, f"stock_kline_{year}.parquet")
        
        if not os.path.exists(path):
            return None
        
        # 读取分片（按 node 分片）
        df = pd.read_parquet(path)
        
        # 分片过滤
        if self.total_nodes > 1:
            df = df[df['code'].apply(lambda x: hash(x) % self.total_nodes == self.node_index)]
        
        # 过滤单只股票
        df = df[df['code'] == instrument].copy()
        df = df.sort_values('date')
        
        if df.empty:
            return None
        
        self._daily_cache[instrument] = df
        return df
    
    def get_adjust_factor(self, instrument: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取复权因子"""
        # 简化：返回 1.0
        calendar = self._calendar_cache
        dates = [d for d in calendar if start_date <= d <= end_date]
        return pd.DataFrame({
            'date': dates,
            'factor': [1.0] * len(dates)
        })
    
    def is_suspended(self, instrument: str, dt: date) -> bool:
        """是否停牌"""
        df = self._get_daily_data(instrument)
        if df is None:
            return True
        row = df[df['date'] == dt]
        if row.empty:
            return True
        # 成交量为 0 视为停牌
        return row.iloc[0]['volume'] == 0
    
    def is_st_stock(self, instrument: str, dt: date) -> bool:
        """是否 ST"""
        inst = self._instruments_cache.get(instrument)
        if inst:
            return 'ST' in inst.abbrev_symbol or '*ST' in inst.abbrev_symbol
        return False
    
    def get_dividends(self, instrument: str, start_date: date, end_date: date) -> pd.DataFrame:
        """分红数据"""
        # 简化：返回空
        return pd.DataFrame()
    
    def get_splits(self, instrument: str, start_date: date, end_date: date) -> pd.DataFrame:
        """拆分数据"""
        return pd.DataFrame()
    
    def get_yield_curve(self, start_date: date, end_date: date) -> pd.DataFrame:
        """收益率曲线"""
        return pd.DataFrame()
    
    def get_risk_free_rate(self, start_date: date, end_date: date) -> float:
        return 0.03
    
    def get_settlement_date(self, dt: date) -> date:
        return dt
    
    def get_instrument_industry(self, instrument: str) -> Optional[str]:
        inst = self._instruments_cache.get(instrument)
        return inst.industry_code if inst else None
    
    def get_all_securities(self, types: List[str] = None, date: date = None) -> List[str]:
        return list(self._instruments_cache.keys())
    
    def get_margin_info(self, instrument: str, dt: date) -> Dict[str, float]:
        return {'margin_rate': 0.5, 'short_margin_rate': 0.5}
    
    def get_short_stock_interest(self, instrument: str, dt: date) -> float:
        return 0.0
    
    def get_future_info(self, instrument: str) -> Optional[Dict]:
        return None
    
    def get_settle_price(self, instrument: str, dt: date) -> Optional[float]:
        return None
    
    def get_trading_dates(self, start_date: date, end_date: date) -> List[date]:
        return [d for d in self._calendar_cache if start_date <= d <= end_date]


# 工厂函数
def create_blinkquant_data_source(parquet_root: str, total_nodes: int = 1, node_index: int = 0):
    """创建 blinkquant 数据源"""
    return BlinkquantParquetDataSource(parquet_root, total_nodes, node_index)
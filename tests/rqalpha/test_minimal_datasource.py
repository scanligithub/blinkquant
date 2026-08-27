"""
最小化 RQAlpha 测试：使用自定义 DataSource 避免 bundle 下载
"""

import pytest
from rqalpha import run_func
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.data.data_proxy import DataProxy
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd


class MinimalDataSource(BaseDataSource):
    """最小化数据源，用于测试 timing 语义"""
    
    def __init__(self):
        from types import SimpleNamespace
        import os
        # 创建 bundle 目录
        bundle_path = os.path.join(os.path.expanduser("~"), ".rqalpha", "bundle")
        os.makedirs(bundle_path, exist_ok=True)
        config = SimpleNamespace(data_bundle_path=bundle_path, future_info={})
        super().__init__(config)
        self._instruments = {
            "600000.XSHG": Instrument(
                order_book_id="600000.XSHG",
                symbol="600000",
                abbrev_symbol="测试股票",
                industry_code="",
                industry_name="",
                listed_date=date(2000, 1, 1),
                de_listed_date=None,
                type=INSTRUMENT_TYPE.CS,
                status="Active"
            )
        }
        self._calendar = [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
            date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9),
            date(2024, 1, 10), date(2024, 1, 11), date(2024, 1, 12)
        ]
        self._daily_data = self._generate_daily_data()
    
    def _generate_daily_data(self):
        """生成合成日线数据"""
        data = {}
        base_price = 10.0
        for i, d in enumerate(self._calendar):
            # 简单的价格走势
            price = base_price + i * 0.1
            data[d] = {
                'open': price,
                'high': price + 0.2,
                'low': price - 0.1,
                'close': price + 0.05,
                'volume': 1000000,
            }
        return data
    
    def get_instruments(self):
        return list(self._instruments.values())
    
    def get_instrument(self, order_book_id):
        return self._instruments.get(order_book_id)
    
    def get_trading_calendar(self):
        return self._calendar
    
    def get_bar(self, instrument, dt, frequency):
        d = dt.date()
        if d not in self._daily_data:
            return None
        row = self._daily_data[d]
        return {
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'datetime': dt,
            'frequency': frequency,
        }
    
    def get_price(self, instrument, start_date, end_date, frequency, fields=None):
        dates = [d for d in self._calendar if start_date <= d <= end_date]
        rows = []
        for d in dates:
            row = self._daily_data[d]
            rows.append({
                'date': d,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
            })
        df = pd.DataFrame(rows)
        if fields:
            df = df[fields]
        return df
    
    def get_trading_calendar(self):
        return self._calendar
    
    def get_trading_dates(self, start_date, end_date):
        return [d for d in self._calendar if start_date <= d <= end_date]
    
    def is_suspended(self, instrument, dt):
        return False
    
    def is_st_stock(self, instrument, dt):
        return False
    
    def get_dividends(self, instrument, start_date, end_date):
        return pd.DataFrame()
    
    def get_splits(self, instrument, start_date, end_date):
        return pd.DataFrame()
    
    def get_yield_curve(self, start_date, end_date):
        return pd.DataFrame()
    
    def get_risk_free_rate(self, start_date, end_date):
        return 0.03
    
    def get_settlement_date(self, dt):
        return dt
    
    def get_instrument_industry(self, instrument):
        return None
    
    def get_all_securities(self, types=None, date=None):
        return list(self._instruments.keys())
    
    def get_margin_info(self, instrument, dt):
        return {'margin_rate': 0.5, 'short_margin_rate': 0.5}
    
    def get_short_stock_interest(self, instrument, dt):
        return 0.0
    
    def get_future_info(self, instrument):
        return None
    
    def get_settle_price(self, instrument, dt):
        return None


import pytest
from rqalpha import run_func
from rqalpha.data.data_proxy import DataProxy


class TestMinimalTiming:
    """最小化 timing 测试"""
    
    def test_minimal_config(self):
        """测试最小配置能否跑通"""
        # 创建自定义 DataSource
        data_source = MinimalDataSource()
        data_proxy = DataProxy(data_source)
        
        # 直接测试 DataProxy
        bar = data_proxy.get_bar("600000.XSHG", datetime(2024, 1, 2), "1d")
        assert bar is not None
        assert bar['open'] == 10.0
        print(f"Bar: {bar}")
        
        prices = data_proxy.get_price("600000.XSHG", date(2024, 1, 2), date(2024, 1, 5), "1d", ["open", "close"])
        print(f"Prices:\n{prices}")
        
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-k", "test_minimal_config"])
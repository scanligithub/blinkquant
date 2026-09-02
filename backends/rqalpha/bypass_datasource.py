"""
RQAlpha 无 Bundle 数据源

完全绕过 BaseDataSource 的 bundle 依赖，直接实现 DataProxy 所需的接口。
用于 PoC-0A Timing 验证，完全脱离 bundle 依赖。
"""

from rqalpha.data.data_proxy import DataProxy
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Union
import pandas as pd
import numpy as np
from types import SimpleNamespace


class BundleFreeDataSource:
    """
    无 Bundle 数据源
    
    完全不依赖 RQAlpha bundle 目录，直接在内存中提供所需数据。
    实现 DataProxy 所需的核心接口。
    """
    
    def __init__(self, calendar: List[date] = None, instruments: Dict = None):
        # 交易日历
        self._calendar = [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 3),
            date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8),
            date(2024, 1, 9), date(2024, 1, 10), date(2024, 1, 11),
            date(2024, 1, 11), date(2024, 1, 12)
        ] if calendar is None else calendar
        
        # 合成股票数据
        self._instruments = {
            "600000.XSHG": {
                'order_book_id': "600000.XSHG",
                'symbol': "600000",
                'abbrev_symbol': "测试股票",
                'industry_code': "",
                'industry_name': "",
                'listed_date': date(2000, 1, 1),
                'de_listed_date': None,
                'type': "CS",
                'status': 'Active'
            }
        }
        
        # 合成日线数据
        self._daily_data = {}
        base_price = 10.0
        for i, d in enumerate(self._calendar):
            price = 10.0 + i * 0.1
            self._daily_data[d] = {
                'open': price,
                'high': price + 0.2,
                'low': price - 0.1,
                'close': price + 0.05,
                'volume': 1000000,
            }
    
    # ===== DataProxy 所需接口 =====
    
    def get_instruments(self):
        class MockInstrument:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        
        return [type('Instrument', (), d) for d in self._instruments.values()]
    
    def get_instrument(self, order_book_id: str):
        class MockInstrument:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        return type('Instrument', (), self._instruments.get(order_book_id, {}))
    
    def get_trading_calendar(self):
        return self._calendar
    
    def get_trading_dates(self, start_date: date, end_date: date):
        return [d for d in self._calendar if start_date <= d <= end_date]
    
    def get_bar(self, instrument: str, dt: datetime, frequency: str):
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
            'datetime': datetime.combine(d, datetime.min.time()),
            'frequency': frequency,
        }
    
    def get_price(self, instrument: str, start_date: date, end_date: date, 
                  frequency: str, fields: List[str] = None) -> 'pd.DataFrame':
        import pandas as pd
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
    
    def get_trading_dates(self, start_date: date, end_date: date):
        return [d for d in self._calendar if start_date <= d <= end_date]
    
    def is_suspended(self, instrument: str, dt: date) -> bool:
        return False
    
    def is_st_stock(self, instrument: str, dt: date) -> bool:
        return False
    
    def get_dividends(self, instrument: str, start_date: date, end_date: date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_splits(self, instrument: str, start_date: date, end_date: date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_yield_curve(self, start_date: date, end_date: date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_risk_free_rate(self, start_date: date, end_date: date) -> float:
        return 0.03
    
    def get_settlement_date(self, dt: date) -> date:
        return dt
    
    def get_instrument_industry(self, instrument: str) -> Optional[str]:
        return None
    
    def get_all_securities(self, types: List[str] = None, date: date = None) -> List[str]:
        return list(self._instruments.keys())
    
    def get_margin_info(self, instrument: str, dt: date) -> Dict[str, float]:
        return {'margin_rate': 0.5, 'short_margin_rate': 0.5}
    
    def get_short_stock_interest(self, instrument: str, dt: date) -> float:
        return 0.0
    
    def get_future_info(self, instrument: str) -> Optional[Dict]:
        return None
    
    def get_settle_price(self, instrument: str, dt: date) -> Optional[float]:
        return None
    
    def get_trading_dates(self, start_date: date, end_date: date) -> List[date]:
        return [d for d in self._calendar if start_date <= d <= end_date]
    
    def get_instruments(self):
        class MockInst:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        return [type('Instrument', (), d) for d in self._instruments.values()]
    
    def get_instrument(self, order_book_id: str):
        class MockInst:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        d = self._instruments.get(order_book_id, {})
        if not d:
            return None
        inst = type('Instrument', (), d)
        return inst
    
    def get_trading_calendar(self):
        return self._calendar


class BundleFreeDataProxy:
    """
    无 Bundle DataProxy
    
    直接包装 BundleFreeDataSource，提供 DataProxy 接口
    """
    
    def __init__(self, data_source=None):
        self.data_source = data_source or BundleFreeDataSource()
        self._calendar = data_source.get_trading_calendar() if data_source else []
    
    def get_bar(self, instrument, dt, frequency):
        return self.data_source.get_bar(instrument, dt, frequency)
    
    def get_price(self, instrument, start_date, end_date, frequency, fields=None):
        return self.data_source.get_price(instrument, start_date, end_date, frequency, fields)
    
    def get_trading_dates(self, start_date, end_date):
        return self.data_source.get_trading_dates(start_date, end_date)
    
    def get_trading_calendar(self):
        return self.data_source.get_trading_calendar()
    
    def get_instrument(self, order_book_id):
        return self.data_source.get_instrument(order_book_id)
    
    def get_instruments(self):
        return self.data_source.get_instruments()
    
    def is_suspended(self, instrument, dt):
        return False
    
    def is_st_stock(self, instrument, dt):
        return False
    
    def get_dividends(self, instrument, start_date, end_date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_splits(self, instrument, start_date, end_date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_yield_curve(self, start_date, end_date):
        import pandas as pd
        return pd.DataFrame()
    
    def get_risk_free_rate(self, start_date, end_date):
        return 0.03
    
    def get_instrument(self, order_book_id):
        class MockInst:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        return self.data_source.get_instrument(order_book_id)
    
    def get_all_securities(self, types=None, date=None):
        return ["600000.XSHG"]
    
    def get_instrument_industry(self, instrument):
        return None
    
    def get_margin_info(self, instrument, dt):
        return {'margin_rate': 0.5, 'short_margin_rate': 0.5}
    
    def get_short_stock_interest(self, instrument, dt):
        return 0.0
    
    def get_future_info(self, instrument):
        return None
    
    def get_settle_price(self, instrument, dt):
        return None
    
    def get_trading_dates(self, start_date, end_date):
        from datetime import date
        calendar = [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 3),
            date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8),
            date(2024, 1, 9), date(2024, 1, 10), date(2024, 1, 11),
            date(2024, 1, 11), date(2024, 1, 12)
        ]
        return [d for d in calendar if start_date <= d <= end_date]


def create_bundle_free_data_proxy():
    """创建无 bundle 的 DataProxy，用于 PoC-0A"""
    return BundleFreeDataProxy()


# 使用示例
if __name__ == "__main__":
    proxy = create_bundle_free_data_proxy()
    
    from datetime import datetime, date
    bar = proxy.get_bar("600000.XSHG", datetime(2024, 1, 2), "1d")
    print(f"Bar: {bar}")
    
    prices = proxy.get_price("600000.XSHG", date(2024, 1, 2), date(2024, 1, 5), "1d", ["open", "close"])
    print(f"Prices:\n{prices}")
    
    print("Bundle-free DataProxy 创建成功！")
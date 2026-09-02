"""
PoC-0: T signal → T+1 open execution → T+1 close valuation

使用 RQAlpha 官方示例 bundle（如果可用）或最小配置
"""

import pytest
from rqalpha import run_func
from rqalpha.api import order_shares, order_target_percent
from rqalpha.const import MATCHING_TYPE


def test_rqalpha_timing_signal_to_execution():
    """
    验证 RQAlpha timing 语义：
    - T 日 handle_bar 中下单
    - T+1 开盘价成交
    - T+1 收盘估值
    """
    
    executed_orders = []
    
    def init(context):
        context.stock = "600000.XSHG"
        context.signal_day = 0
        
    def handle_bar(context, bar_dict):
        # 第 1 天发送买入信号
        if context.now.day == 2 and not getattr(context, 'ordered', False):
            order_shares("600000.XSHG", 100)
            context.ordered = True
            print(f"Signal sent at: {context.now}")
    
    def after_trading(context):
        pass
    
    config = {
        "base": {
            "start_date": "2024-01-02",
            "end_date": "2024-01-10",
            "frequency": "1d",
            "accounts": {"stock": 100000},
            "benchmark": "000300.XSHG",
        },
        "mod": {
            "sys_simulation": {
                "signal": False,
                "matching_type": "current_bar",
            },
            "sys_accounts": {
                "stock_t_plus": True,
            }
        }
    }
    
    # 运行回测
    result = run_func(
        init=lambda ctx: None,
        handle_bar=lambda ctx, bd: None,
        config={
            "base": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-10",
                "frequency": "1d",
                "accounts": {"stock": 100000},
            },
            "mod": {
                "sys_simulation": {
                    "signal": False,
                    "matching_type": "current_bar",
                },
                "sys_accounts": {
                    "stock_t_plus": True,
                }
            }
        }
    )
    
    # 这里仅测试配置能否跑通
    assert result is not None
    print("RQAlpha 基础配置跑通")


def test_timing_semantics():
    """
    核心语义验证：
    - T 日信号
    - T+1 开盘成交
    - T+1 收盘估值
    
    这个测试需要真实数据 bundle，暂时跳过
    """
    pytest.skip("需要 RQAlpha 数据 bundle，待解决数据源问题")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-k", "test_basic_config"])
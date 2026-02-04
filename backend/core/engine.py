import polars as pl
from .data_manager import data_manager
from .security import blink_parser
from .data_types import AShareDataSchema

class SelectionEngine:
    """
    负责执行选股逻辑
    """
    def execute_selector(self, formula: str, timeframe: str = 'D'):
        # 1. 获取对应周期的数据
        if timeframe == 'W':
            df = data_manager.df_weekly
        elif timeframe == 'M':
            df = data_manager.df_monthly
        else:
            df = data_manager.df_daily

        if df is None:
            return []

        # 2. 解析公式为 Polars 表达式
        try:
            expr = blink_parser.parse_expression(formula)
        except Exception as e:
            return {"error": str(e)}

        # 3. 执行过滤 (分布式节点只处理自己的分片)
        # 我们取最后一天的数据进行选股
        last_date = df.select(pl.col(AShareDataSchema.DATE).max()).item()
        
        result = (
            df.filter(pl.col(AShareDataSchema.DATE) == last_date)
            .filter(expr)
            .select(pl.col(AShareDataSchema.CODE))
            .collect() # 执行计算
        )

        return result[AShareDataSchema.CODE].to_list()

selection_engine = SelectionEngine()

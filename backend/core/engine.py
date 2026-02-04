import polars as pl
from .data_manager import data_manager
from .security import blink_parser

class SelectionEngine:
    def execute_selector(self, formula: str, timeframe: str = 'D'):
        # 1. 确定数据周期
        if timeframe == 'W':
            df_stock, df_sect = data_manager.df_weekly, data_manager.df_sector_weekly
        elif timeframe == 'M':
            df_stock, df_sect = data_manager.df_monthly, data_manager.df_sector_monthly
        else:
            df_stock, df_sect = data_manager.df_daily, data_manager.df_sector_daily

        if df_stock is None or data_manager.df_mapping is None:
            return []

        # 2. 准备板块数据 (重命名列以防冲突)
        sect_cols = df_sect.select([
            pl.col("date"),
            pl.col("code").alias("sector_code"),
            pl.col("close").alias("s_close"),
            pl.col("open").alias("s_open"),
            pl.col("pctChg").alias("s_pctChg")
        ])

        # 3. 关联数据: 个股 -> 映射表 -> 板块指数
        # 仅取最后一天数据进行选股以节省内存
        last_date = df_stock.select(pl.col("date").max()).item()
        
        # 在 engine.py 中的关联逻辑 (确认代码如下即可，无需再次修改)
        combined_df = (
            df_stock.filter(pl.col("date") == last_date)
            .join(data_manager.df_mapping, on="code", how="left") # 这里的 code 现在能匹配上了
            .join(sect_cols.filter(pl.col("date") == last_date), on=["date", "sector_code"], how="left")
        )

        # 4. 执行选股公式
        try:
            expr = blink_parser.parse_expression(formula)
            result = combined_df.filter(expr).select("code").collect()
            return result["code"].to_list()
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

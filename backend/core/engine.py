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

        if df_stock is None: return []

        # 2. 转换为 LazyFrame 以获得最佳性能并允许 collect()
        lf_stock = df_stock.lazy()
        
        # 3. 关联板块数据 (如果 mapping 存在)
        if data_manager.df_mapping is not None and df_sect is not None:
            sect_cols = df_sect.lazy().select([
                pl.col("date"),
                pl.col("code").alias("sector_code"),
                pl.col("close").alias("s_close"),
                pl.col("open").alias("s_open"),
                pl.col("pctChg").alias("s_pctChg")
            ])
            
            lf_stock = (
                lf_stock.join(data_manager.df_mapping.lazy(), on="code", how="left")
                .join(sect_cols, on=["date", "sector_code"], how="left")
            )

        # 4. 【核心逻辑】：先在全量时间轴上计算指标
        try:
            expr = blink_parser.parse_expression(formula)
            # 将表达式结果存入临时列 "_signal"
            lf_stock = lf_stock.with_columns(expr.alias("_signal"))
            
            # 5. 【最后过滤】：只取最后一天且信号为 True 的股票
            last_date = df_stock.select(pl.col("date").max()).item()
            
            result_df = (
                lf_stock.filter(pl.col("date") == last_date)
                .filter(pl.col("_signal") == True)
                .select("code")
                .collect() # 此时执行计算
            )
            return result_df["code"].to_list()
            
        except Exception as e:
            print(f"Engine Error: {str(e)}") # 后端打印详细错误
            return {"error": str(e)}

selection_engine = SelectionEngine()

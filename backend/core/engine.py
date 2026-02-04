import polars as pl
from .data_manager import data_manager
from .security import blink_parser
from .data_types import AShareDataSchema

class SelectionEngine:
    def execute_selector(self, formula: str, timeframe: str = 'D'):
        # 1. 确定数据周期
        if timeframe == 'W':
            df_stock, df_sect = data_manager.df_weekly, data_manager.df_sector_weekly
        elif timeframe == 'M':
            df_stock, df_sect = data_manager.df_monthly, data_manager.df_sector_monthly
        else:
            df_stock, df_sect = data_manager.df_daily, data_manager.df_sector_daily

        if df_stock is None: 
            return {"error": "Stock data not loaded"}

        # 2. 转换为 LazyFrame 以获得最佳性能
        lf_stock = df_stock.lazy()
        
        # 3. 动态关联板块数据
        if data_manager.df_mapping is not None and df_sect is not None:
            # 【核心修复】：动态构建板块选择列，防止因缺失 pctChg 导致崩溃
            sect_select_cols = [
                pl.col("date"),
                pl.col("code").alias("sector_code"),
                pl.col("close").alias("s_close"),
                pl.col("open").alias("s_open"),
                pl.col("high").alias("s_high"),
                pl.col("low").alias("s_low"),
            ]
            
            # 只有当板块数据中有涨跌幅时才加入
            if "pctChg" in df_sect.columns:
                sect_select_cols.append(pl.col("pctChg").alias("s_pctChg"))
            else:
                # 容错：如果缺失，则用 0 或 null 填充，防止 AST 解析时字段不存在报错
                sect_select_cols.append(pl.lit(None).alias("s_pctChg"))

            sect_cols_lazy = df_sect.lazy().select(sect_select_cols)
            
            # 执行关联
            lf_stock = (
                lf_stock.join(data_manager.df_mapping.lazy(), on="code", how="left")
                .join(sect_cols_lazy, on=["date", "sector_code"], how="left")
            )

        # 4. 执行公式计算
        try:
            expr = blink_parser.parse_expression(formula)
            # 在全量时间序列上计算指标 (如 MA60)
            lf_stock = lf_stock.with_columns(expr.alias("_signal"))
            
            # 5. 获取最新交易日并过滤信号
            last_date = df_stock.select(pl.col("date").max()).item()
            
            result_df = (
                lf_stock.filter(pl.col("date") == last_date)
                .filter(pl.col("_signal") == True)
                .select("code")
                .collect() # 此时正式执行并行计算
            )
            
            return result_df["code"].to_list()
            
        except Exception as e:
            # 在后端日志打印具体错误，方便调试
            print(f"Engine Selection Error: {str(e)}")
            return {"error": str(e)}

selection_engine = SelectionEngine()

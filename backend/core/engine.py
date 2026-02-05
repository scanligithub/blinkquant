import polars as pl
import re
from .data_manager import data_manager
from .security import blink_parser

class SelectionEngine:
    def __init__(self):
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _bg_jit_mount(self, formula: str):
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            func_name, field_name, p_val = func.upper(), field.upper(), int(param)
            cache_key = f"{func_name}_{field_name}_{p_val}"
            if data_manager.df_daily is not None and cache_key not in data_manager.df_daily.columns:
                try:
                    if func_name in data_manager.INDICATOR_MAP:
                        expr = data_manager.INDICATOR_MAP[func_name](pl.col(field_name.lower()), p_val)
                        data_manager.mount_jit_column(cache_key, expr)
                except: pass

    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        # 1. 确定数据源
        df = data_manager.df_daily if timeframe == 'D' else data_manager.df_weekly
        if df is None: return {"error": "Data loading..."}
        
        lf = df.lazy()

        # 2. 动态关联板块行情
        if data_manager.df_mapping is not None and data_manager.df_sector_daily is not None:
            # 这里的 sect_select 定义了要从板块表拿哪些列
            sect_select = [
                pl.col("date"), 
                pl.col("code").alias("sector_code"), 
                pl.col("close").alias("s_close"), 
                pl.col("pctChg").alias("s_pctChg")
            ]
            
            s_df = data_manager.df_sector_daily
            if timeframe == 'W': s_df = data_manager.df_sector_weekly
            elif timeframe == 'M': s_df = data_manager.df_sector_monthly
            
            # --- 修复位置：将 meta.name() 改为 meta.output_name() --- 
            available_sector_cols = [c for c in sect_select if c.meta.output_name() in s_df.columns]
            
            lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                    .join(s_df.lazy().select(available_sector_cols), on=["date", "sector_code"], how="left"))

        try:
            # 3. 解析 AST 公式
            expr = blink_parser.parse_expression(formula)
            lf = lf.with_columns(expr.alias("_signal"))

            # 4. 只取最后一天且信号为 True 的股票
            last_date = df.select(pl.col("date").max()).item()
            
            # 增加 drop_nulls() 防止因为指标计算窗口不足导致的 False Negative
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal") == True)
                         .select("code")
                         .collect())
            
            if timeframe == 'D':
                background_tasks.add_task(self._bg_jit_mount, formula)
                
            return result_df["code"].to_list()
        except Exception as e:
            return {"error": f"Engine Error: {str(e)}"}

selection_engine = SelectionEngine()

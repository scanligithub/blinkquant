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
        df = data_manager.df_daily if timeframe == 'D' else data_manager.df_weekly
        if df is None: return {"error": "Data loading..."}
        
        lf = df.lazy()
        # 关联板块逻辑
        if data_manager.df_mapping is not None and data_manager.df_sector_daily is not None:
            sect_select = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close"), pl.col("pctChg").alias("s_pctChg")]
            s_df = data_manager.df_sector_daily
            if timeframe == 'W': s_df = data_manager.df_sector_weekly
            elif timeframe == 'M': s_df = data_manager.df_sector_monthly
            
            lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                    .join(s_df.lazy().select([c for c in sect_select if c.meta.name() in s_df.columns]), on=["date", "sector_code"], how="left"))

        try:
            expr = blink_parser.parse_expression(formula)
            lf = lf.with_columns(expr.alias("_signal"))
            last_date = df.select(pl.col("date").max()).item()
            result_df = lf.filter(pl.col("date") == last_date).filter(pl.col("_signal") == True).select("code").collect()
            
            if timeframe == 'D':
                background_tasks.add_task(self._bg_jit_mount, formula)
            return result_df["code"].to_list()
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

import polars as pl
import re
import logging
from .data_manager import data_manager
from .security import blink_parser

logger = logging.getLogger(__name__)

class SelectionEngine:
    def __init__(self):
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _prepare_hot_jit(self, formula: str, timeframe: str):
        """同步热挂载：使用统一列名 (Pure Name)"""
        # 1. 确定目标 DataFrame
        df_attr = {'D':'df_daily', 'W':'df_weekly', 'M':'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)
        if df is None: return
        
        matches = self.metric_pattern.findall(formula)
        new_exprs = []
        
        for func, field, param in matches:
            func_name, field_name, p_val = func.upper(), field.upper(), int(param)
            
            # 统一列名: MA_CLOSE_20 (即使在周线表中也叫这个)
            pure_col_name = f"{func_name}_{field_name}_{p_val}"
            
            # 如果内存里没有这一列，则计算并挂载
            if pure_col_name not in df.columns:
                try:
                    if func_name in data_manager.INDICATOR_MAP:
                        expr = data_manager.INDICATOR_MAP[func_name](pl.col(field_name.lower()), p_val).alias(pure_col_name)
                        new_exprs.append(expr)
                        logger.info(f"Hot-JIT [{timeframe}]: Computing {pure_col_name}")
                except Exception as e:
                    logger.warning(f"JIT Error {pure_col_name}: {e}")

        if new_exprs:
            updated_df = df.with_columns(new_exprs)
            setattr(data_manager, df_attr, updated_df)
            logger.info(f"Hot-JIT [{timeframe}]: Mounted {len(new_exprs)} pure columns.")

    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        # 1. 热挂载
        self._prepare_hot_jit(formula, timeframe)
        
        # 2. 获取数据源
        df_attr = {'D':'df_daily', 'W':'df_weekly', 'M':'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)
        
        s_df_attr = {'D':'df_sector_daily', 'W':'df_sector_weekly', 'M':'df_sector_monthly'}.get(timeframe, 'df_sector_daily')
        s_df = getattr(data_manager, s_df_attr)

        if df is None: return {"error": "Data not loaded."}
        lf = df.lazy()

        # 3. 关联板块
        if data_manager.df_mapping is not None and s_df is not None:
            try:
                sector_exprs = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close")]
                if "pctChg" in s_df.columns: sector_exprs.append(pl.col("pctChg").alias("s_pctChg"))
                
                s_lazy = s_df.lazy().select(sector_exprs)
                lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                        .join(s_lazy, on=["date", "sector_code"], how="left"))
            except: pass

        try:
            # 4. 解析 (此时 Parser 会在 df 中找到 MA_CLOSE_20)
            expr = blink_parser.parse_expression(formula, timeframe)
            
            lf = lf.with_columns(expr.alias("_signal"))
            
            # 获取最后交易日
            if df.is_empty(): return []
            last_date = df.select(pl.col("date").max()).item()
            
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal").fill_null(False) == True)
                         .select("code")
                         .collect())
            
            return result_df["code"].to_list()
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

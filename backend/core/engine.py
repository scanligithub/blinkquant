import polars as pl
import re
import logging
from .data_manager import data_manager
from .security import blink_parser

logger = logging.getLogger(__name__)

class SelectionEngine:
    def __init__(self):
        # 匹配算子: MA(CLOSE, 20)
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _prepare_hot_jit(self, formula: str, timeframe: str):
        """同步热挂载：在解析公式前，强制在目标周期表中计算并缓存指标列"""
        # 1. 确定目标表和后缀
        df_attr, suffix = {
            'D': ('df_daily', ''),
            'W': ('df_weekly', '_W'),
            'M': ('df_monthly', '_M')
        }.get(timeframe, ('df_daily', ''))

        df = getattr(data_manager, df_attr)
        if df is None: return
        
        matches = self.metric_pattern.findall(formula)
        new_exprs = []
        
        for func, field, param in matches:
            func_name, field_name, p_val = func.upper(), field.upper(), int(param)
            # 构造对应周期的缓存键
            cache_key = f"{func_name}_{field_name}_{p_val}{suffix}"
            
            # 如果内存里没有这一列，准备同步计算
            if cache_key not in df.columns:
                try:
                    if func_name in data_manager.INDICATOR_MAP:
                        expr = data_manager.INDICATOR_MAP[func_name](pl.col(field_name.lower()), p_val).alias(cache_key)
                        new_exprs.append(expr)
                        logger.info(f"Hot-JIT [{timeframe}]: Computing {cache_key}")
                except Exception as e:
                    logger.warning(f"JIT Error {cache_key}: {e}")

        # 如果发现新指标，执行同步挂载
        if new_exprs:
            updated_df = df.with_columns(new_exprs)
            setattr(data_manager, df_attr, updated_df)
            logger.info(f"Hot-JIT [{timeframe}]: {len(new_exprs)} columns mounted.")

    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        # 1. 针对当前周期执行热挂载 (JIT)
        self._prepare_hot_jit(formula, timeframe)
        
        # 2. 获取（可能已被更新的）数据源
        df_attr = {'D':'df_daily', 'W':'df_weekly', 'M':'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)
        
        # 获取对应的板块表
        s_df_attr = {'D':'df_sector_daily', 'W':'df_sector_weekly', 'M':'df_sector_monthly'}.get(timeframe, 'df_sector_daily')
        s_df = getattr(data_manager, s_df_attr)

        if df is None: return {"error": "Data table not found."}
        
        lf = df.lazy()

        # 3. 关联板块行情 (防御性 Join)
        if data_manager.df_mapping is not None and s_df is not None:
            try:
                sector_exprs = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close")]
                if "pctChg" in s_df.columns: 
                    sector_exprs.append(pl.col("pctChg").alias("s_pctChg"))
                
                s_lazy = s_df.lazy().select(sector_exprs)
                lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                        .join(s_lazy, on=["date", "sector_code"], how="left"))
            except Exception as e:
                logger.warning(f"Sector join failed: {e}")

        try:
            # 4. 解析公式 (此时 security.py 能实时感知到新 mount 的列)
            expr = blink_parser.parse_expression(formula, timeframe)
            lf = lf.with_columns(expr.alias("_signal"))
            
            # 取当前周期的最后交易日
            last_date = df.select(pl.col("date").max()).item()
            
            # 执行计算
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal").fill_null(False) == True)
                         .select("code")
                         .collect())
            
            return result_df["code"].to_list()
        except Exception as e:
            logger.error(f"Engine Run Error: {e}")
            return {"error": str(e)}

selection_engine = SelectionEngine()

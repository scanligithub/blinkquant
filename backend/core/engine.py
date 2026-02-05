import polars as pl
import re
import logging
from .data_manager import data_manager
from .security import blink_parser

logger = logging.getLogger(__name__)

class SelectionEngine:
    def __init__(self):
        # 用于提取公式中的指标以便后台静默预计算
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _bg_jit_mount(self, formula: str):
        """后台任务：将公式中涉及的新指标挂载到内存，下次查询即命中缓存"""
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            func_name, field_name, p_val = func.upper(), field.upper(), int(param)
            cache_key = f"{func_name}_{field_name}_{p_val}"
            
            if data_manager.df_daily is not None and cache_key not in data_manager.df_daily.columns:
                try:
                    if func_name in data_manager.INDICATOR_MAP:
                        expr = data_manager.INDICATOR_MAP[func_name](pl.col(field_name.lower()), p_val)
                        data_manager.mount_jit_column(cache_key, expr)
                except Exception as e:
                    logger.warning(f"JIT Background failed for {cache_key}: {e}")

    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        # 1. 确定数据源
        if timeframe == 'D':
            df = data_manager.df_daily
            s_df = data_manager.df_sector_daily
        elif timeframe == 'W':
            df = data_manager.df_weekly
            s_df = data_manager.df_sector_weekly
        else:
            df = data_manager.df_monthly
            s_df = data_manager.df_sector_monthly

        if df is None:
            return {"error": "Nodes are still loading data or resampled data is missing."}
        
        lf = df.lazy()

        # 2. 动态关联板块行情 (如需使用 S_CLOSE 等字段)
        if data_manager.df_mapping is not None and s_df is not None:
            sect_select = [
                pl.col("date"), 
                pl.col("code").alias("sector_code"), 
                pl.col("close").alias("s_close"), 
                pl.col("pctChg").alias("s_pctChg")
            ]
            
            # --- 修复：Polars 0.20+ 使用 meta.output_name() 代替 meta.name() ---
            available_sector_cols = [c for c in sect_select if c.meta.output_name() in s_df.columns]
            
            lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                    .join(s_df.lazy().select(available_sector_cols), on=["date", "sector_code"], how="left"))

        try:
            # 3. 解析 AST 公式为 Polars 表达式
            expr = blink_parser.parse_expression(formula)
            
            # 4. 执行过滤逻辑
            # .fill_null(False) 非常关键：MA(250) 在前 249 天是 null，如果不 fill_null，这些行在过滤时会消失
            lf = lf.with_columns(expr.alias("_signal")).with_columns(pl.col("_signal").fill_null(False))

            # 确定当前分片的最后交易日
            # 注意：不同股票分片可能有不同的最后交易日（取决于更新同步），取全局最大
            last_date = df.select(pl.col("date").max()).item()
            
            # 最终执行：过滤最后一天 + 信号为真
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal") == True)
                         .select("code")
                         .collect())
            
            # 5. 触发异步预计算优化
            if timeframe == 'D':
                background_tasks.add_task(self._bg_jit_mount, formula)
                
            return result_df["code"].to_list()

        except Exception as e:
            logger.error(f"Selection Execution Error: {e}")
            return {"error": f"Selection Engine Error: {str(e)}"}

selection_engine = SelectionEngine()

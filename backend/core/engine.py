import polars as pl
import re
import logging
from .data_manager import data_manager
from .security import blink_parser

logger = logging.getLogger(__name__)

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
        # 1. 确定主数据源
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
            return {"error": "Main data not loaded."}
        
        lf = df.lazy()

        # 2. 关联板块行情 (Safe Join 模式)
        if data_manager.df_mapping is not None and s_df is not None:
            try:
                # 准备板块表的 LazyFrame
                # 我们不再进行前置 select 过滤，而是直接 rename。如果列不存在，Polars 会在这里报错。
                # 通过 alias 确保列名符合 AST 引擎的期望
                s_lazy = s_df.lazy().select([
                    pl.col("date"),
                    pl.col("code").alias("sector_code"),
                    pl.col("close").alias("s_close"),
                    pl.col("pctChg").alias("s_pctChg")
                ])

                # 先关联映射表 (code -> sector_code)
                lf = lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                
                # 再关联板块行情 (date, sector_code)
                lf = lf.join(s_lazy, on=["date", "sector_code"], how="left")
            except Exception as join_err:
                logger.warning(f"Join sector data skipped due to schema mismatch: {join_err}")
                # 如果关联失败，我们继续执行，只是公式里若引用 S_CLOSE 会报错，这比直接挂掉好。

        try:
            # 3. 解析 AST 公式
            expr = blink_parser.parse_expression(formula)
            
            # 4. 执行信号计算
            # 增加 fill_null(False) 是为了防止指标计算不足(如MA250)产生的 null 导致过滤失败
            lf = lf.with_columns(expr.alias("_signal"))
            
            # 获取当前数据的最后一天
            # 注意：如果板块数据和个股数据最后一天不一致，join 结果会是 null
            last_date = df.select(pl.col("date").max()).item()
            
            # 执行过滤并收集结果
            # 添加 .fill_null(False) 确保信号列没有空值
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal").fill_null(False) == True)
                         .select("code")
                         .collect())
            
            # 5. 触发异步 JIT
            if timeframe == 'D':
                background_tasks.add_task(self._bg_jit_mount, formula)
                
            return result_df["code"].to_list()

        except Exception as e:
            logger.error(f"Selection Execution Error: {e}")
            return {"error": f"Selection Engine Error: {str(e)}"}

selection_engine = SelectionEngine()

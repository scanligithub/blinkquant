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

        # 2. 关联板块行情 (防御性改名)
        if data_manager.df_mapping is not None and s_df is not None:
            try:
                # 建立一个我们希望获取的列名清单
                sector_exprs = [
                    pl.col("date"),
                    pl.col("code").alias("sector_code"),
                    pl.col("close").alias("s_close"),
                ]
                
                # 只有当数据源里确实有 pctChg 时，才尝试加载 s_pctChg
                if "pctChg" in s_df.columns:
                    sector_exprs.append(pl.col("pctChg").alias("s_pctChg"))
                
                s_lazy = s_df.lazy().select(sector_exprs)

                # 联接映射表和板块行情
                lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                        .join(s_lazy, on=["date", "sector_code"], how="left"))
            except Exception as join_err:
                logger.warning(f"Sector join failed (skipping): {join_err}")

        try:
            # 3. 解析 AST 公式
            expr = blink_parser.parse_expression(formula)
            
            # 4. 执行信号计算
            lf = lf.with_columns(expr.alias("_signal"))
            
            # 获取当前分片的最后一天
            # 注意：如果 df 为空会报错，这里增加判断
            if df.is_empty():
                return []
                
            last_date = df.select(pl.col("date").max()).item()
            
            # 执行过滤并收集结果
            # 使用 fill_null(False) 解决指标计算不足时的空值问题
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

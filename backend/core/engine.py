import polars as pl
import re
from .data_manager import data_manager
from .security import blink_parser

class SelectionEngine:
    def __init__(self):
        # 优化正则：不区分大小写，支持常用算子
        self.metric_pattern = re.compile(
            r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', 
            re.IGNORECASE
        )

    def _bg_jit_mount(self, formula: str):
        """后台静默执行：不占用用户等待时间"""
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            func_name, field_name, p_val = func.upper(), field.upper(), int(param)
            cache_key = f"{func_name}_{field_name}_{p_val}"
            
            # 只有内存中不存在时，才执行耗时的计算和挂载
            if data_manager.df_daily is not None and cache_key not in data_manager.df_daily.columns:
                try:
                    if func_name in data_manager.INDICATOR_MAP:
                        # 生成计算表达式
                        expr = data_manager.INDICATOR_MAP[func_name](pl.col(field_name.lower()), p_val)
                        # 调用 DataManager 的安全挂载
                        data_manager.mount_jit_column(cache_key, expr)
                except Exception as e:
                    print(f"Background JIT Failed for {cache_key}: {e}")

    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        df = data_manager.df_daily if timeframe == 'D' else data_manager.df_weekly
        if df is None: return {"error": "Data not ready"}

        try:
            # 1. 解析公式并生成 LazyFrame
            expr = blink_parser.parse_expression(formula)
            lf = df.lazy().with_columns(expr.alias("_signal"))
            
            # 2. 执行计算获取结果 (这是唯一的耗时点)
            last_date = df.select(pl.col("date").max()).item()
            result_df = lf.filter(pl.col("date") == last_date).filter(pl.col("_signal") == True).select("code").collect()
            
            # 3. 将“即时驻留”任务注册到后台，直接 Return 结果给用户
            if timeframe == 'D':
                background_tasks.add_task(self._bg_jit_mount, formula)

            return result_df["code"].to_list()
            
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

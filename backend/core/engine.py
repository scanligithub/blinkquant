import polars as pl
import re
from .data_manager import data_manager
from .security import blink_parser
from .data_types import AShareDataSchema

class SelectionEngine:
    def __init__(self):
        # 匹配公式中的标准指标模式，用于 JIT 提取
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _extract_jit_tasks(self, formula: str):
        """从公式中提取可能需要即时挂载的指标名及其参数"""
        tasks = []
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            key = f"{func.upper()}_{field.upper()}_{param}"
            tasks.append({
                "key": key,
                "func": func.upper(),
                "field": field.lower(),
                "param": int(param)
            })
        return tasks

    def execute_selector(self, formula: str, timeframe: str = 'D'):
        df_stock = data_manager.df_daily # 目前主要针对日线做 JIT
        if df_stock is None: return {"error": "Data not loaded"}

        # 1. 提取 JIT 任务清单
        jit_tasks = self._extract_jit_tasks(formula)
        
        # 2. 转换 LazyFrame 并关联板块 (保持原有逻辑)
        lf_stock = df_stock.lazy()
        # ... (此处省略 join 板块数据的代码，与之前相同)

        try:
            # 3. AST 解析与计算
            # security.py 内部会自动判断是否命中缓存，并更新 data_manager 的访问计数
            expr = blink_parser.parse_expression(formula)
            lf_stock = lf_stock.with_columns(expr.alias("_signal"))
            
            last_date = df_stock.select(pl.col("date").max()).item()
            result_df = lf_stock.filter(pl.col("date") == last_date).filter(pl.col("_signal") == True).select("code").collect()
            
            # 4. 【核心改进】即时进化逻辑 (JIT Mount)
            # 对于公式中出现的、但当前内存中没有的指标，立即执行计算并挂载
            for task in jit_tasks:
                if task["key"] not in df_stock.columns:
                    # 获取该指标对应的 Polars 表达式
                    # 这里直接利用 blink_parser 的映射逻辑或 data_manager 的 INDICATOR_MAP
                    indicator_expr = blink_parser.get_raw_expression(task["func"], task["field"], task["param"])
                    if indicator_expr is not None:
                        data_manager.mount_jit_column(task["key"], indicator_expr)

            return result_df["code"].to_list()
            
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

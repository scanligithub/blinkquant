import polars as pl
import re
from .data_manager import data_manager
from .security import blink_parser
from .data_types import AShareDataSchema

class SelectionEngine:
    def __init__(self):
        # 识别算子模式: MA(CLOSE, 20)
        self.metric_pattern = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

    def _trigger_jit_caching(self, formula: str):
        """解析公式并尝试将新指标挂载到内存"""
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            func, field, param = func.upper(), field.upper(), int(param)
            key = f"{func}_{field}_{param}"
            
            # 如果内存里已经有了，security 已经处理过 hits 了，直接跳过
            if key in data_manager.df_daily.columns:
                continue
                
            # 否则，动态生成表达式并挂载
            try:
                # 利用 data_manager 中定义的算子映射
                if func in data_manager.INDICATOR_MAP:
                    expr = data_manager.INDICATOR_MAP[func](pl.col(field.lower()), param)
                    data_manager.mount_jit_column(key, expr)
            except Exception as e:
                print(f"JIT processing failed for {key}: {e}")

    def execute_selector(self, formula: str, timeframe: str = 'D'):
        if data_manager.df_daily is None: return {"error": "System loading..."}
        
        # 1. 设置数据源
        df = data_manager.df_daily if timeframe == 'D' else data_manager.df_weekly
        lf = df.lazy()

        # 2. 关联板块数据 (逻辑同之前)
        if data_manager.df_mapping is not None and data_manager.df_sector_daily is not None:
            sect_select = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close")]
            lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                    .join(data_manager.df_sector_daily.lazy().select(sect_select), on=["date", "sector_code"], how="left"))

        try:
            # 3. 计算选股信号
            expr = blink_parser.parse_expression(formula)
            lf = lf.with_columns(expr.alias("_signal"))
            
            last_date = df.select(pl.col("date").max()).item()
            result = lf.filter(pl.col("date") == last_date).filter(pl.col("_signal") == True).select("code").collect()
            
            # 4. 【关键步骤】触发 JIT 即时驻留
            # 选股完成后，顺便把没缓存的指标存进内存
            if timeframe == 'D':
                self._trigger_jit_caching(formula)

            return result["code"].to_list()
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()

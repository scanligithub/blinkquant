import os
import polars as pl
import psycopg2
import psutil
import gc
import time
from .data_types import AShareDataSchema

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.repo_id = "scanli/stocka-data"

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.df_sector_daily = None
        self.df_mapping = None

        # --- 内存管理配置 ---
        self.RSS_LIMIT_GB = 14.2  # 严苛防御线
        self.SAFE_LEVEL_GB = 12.0 # 触发置换后的目标水位
        # 永不删除的基础列 (L0)
        self.IMMORTAL_COLS = {
            AShareDataSchema.DATE, AShareDataSchema.CODE, 
            AShareDataSchema.OPEN, AShareDataSchema.HIGH, 
            AShareDataSchema.LOW, AShareDataSchema.CLOSE, 
            AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT, 
            AShareDataSchema.PCT_CHG, AShareDataSchema.TURN,
            'sector_code', 's_close', 's_pctChg'
        }
        # 列访问统计: {col_name: {"hits": int, "last_used": float}}
        self.column_metadata = {}

    def update_col_usage(self, col_name: str):
        """记录列的访问，用于 LRU 置换算法"""
        if col_name in self.IMMORTAL_COLS: return
        if col_name not in self.column_metadata:
            self.column_metadata[col_name] = {"hits": 0, "last_used": time.time()}
        self.column_metadata[col_name]["hits"] += 1
        self.column_metadata[col_name]["last_used"] = time.time()

    def _get_current_rss(self):
        return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3)

    def ensure_capacity(self, incoming_count=1):
        """置换算法核心：如果内存不足，删除最少使用的动态指标"""
        current_rss = self._get_current_rss()
        
        if current_rss < self.RSS_LIMIT_GB:
            return True

        print(f"Memory Pressure! RSS: {current_rss:.2f}GB. Starting Eviction...")
        
        # 1. 识别可删除的候选列 (不属于 IMMORTAL 的所有列)
        dynamic_cols = [c for c in self.df_daily.columns if c not in self.IMMORTAL_COLS]
        if not dynamic_cols: return False

        # 2. 排序：按 hits (升序) 和 last_used (升序) 排序 -> 最少使用且最久没用的排在前面
        candidates = sorted(
            dynamic_cols, 
            key=lambda c: (
                self.column_metadata.get(c, {}).get("hits", 0),
                self.column_metadata.get(c, {}).get("last_used", 0)
            )
        )

        # 3. 执行删除，直到水位安全
        cols_to_drop = []
        for col in candidates:
            cols_to_drop.append(col)
            # 预估每列释放 22MB (5.5M rows * 4 bytes)
            if (current_rss - len(cols_to_drop) * 0.022) < self.SAFE_LEVEL_GB:
                break
        
        if cols_to_drop:
            self.df_daily = self.df_daily.drop(cols_to_drop)
            for c in cols_to_drop: self.column_metadata.pop(c, None)
            gc.collect() # 强制释放内存碎片
            print(f"Evicted {len(cols_to_drop)} columns: {cols_to_drop}")
        
        return self._get_current_rss() < self.RSS_LIMIT_GB

    def mount_jit_column(self, col_name: str, expression: pl.Expr):
        """即时挂载新指标到内存"""
        try:
            if col_name in self.df_daily.columns: return
            
            # 检查并腾出空间
            if not self.ensure_capacity():
                print(f"Skip JIT mount for {col_name}: Memory exhausted.")
                return

            # 计算并水平拼接
            self.df_daily = self.df_daily.with_columns(expression.alias(col_name))
            self.update_col_usage(col_name)
            print(f"JIT Evolution: {col_name} is now cached in memory.")
        except Exception as e:
            print(f"JIT Mount Failed for {col_name}: {e}")

    def load_data(self):
        # ... (之前的加载逻辑保持不变)
        # 结尾处初始化基础列的元数据
        self.column_metadata = {c: {"hits": 0, "last_used": time.time()} for c in self.df_daily.columns if c not in self.IMMORTAL_COLS}
        print(f"Node {self.node_index}: Boot sequence complete.")

data_manager = DataManager()

import os
import polars as pl
import psycopg2
import psutil
import gc
import time
from .data_types import AShareDataSchema
from huggingface_hub import hf_hub_download, list_repo_files

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
        self.RSS_LIMIT_GB = 14.5  # 严苛防御线
        self.SAFE_LEVEL_GB = 12.5 # 触发置换后的目标水位
        
        # 永不删除的基础列 (L0)
        self.IMMORTAL_COLS = {
            AShareDataSchema.DATE, AShareDataSchema.CODE, 
            AShareDataSchema.OPEN, AShareDataSchema.HIGH, 
            AShareDataSchema.LOW, AShareDataSchema.CLOSE, 
            AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT, 
            AShareDataSchema.PCT_CHG, AShareDataSchema.TURN,
            'sector_code', 's_close', 's_pctChg', 's_open', 's_high', 's_low'
        }
        
        # 允许预计算的算子映射
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
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

    def ensure_capacity(self):
        """置换算法核心：如果内存不足，按照 LRU 逻辑删除动态指标"""
        current_rss = self._get_current_rss()
        if current_rss < self.RSS_LIMIT_GB:
            return True

        print(f"Memory Warning! RSS: {current_rss:.2f}GB. Starting Eviction...")
        
        # 1. 识别可删除列
        dynamic_cols = [c for c in self.df_daily.columns if c not in self.IMMORTAL_COLS]
        if not dynamic_cols: return False

        # 2. 排序候选者：hits 少的优先，last_used 久远的优先
        candidates = sorted(
            dynamic_cols, 
            key=lambda c: (
                self.column_metadata.get(c, {}).get("hits", 0),
                self.column_metadata.get(c, {}).get("last_used", 0)
            )
        )

        # 3. 逐步删除直到达到安全水位
        cols_to_drop = []
        for col in candidates:
            cols_to_drop.append(col)
            # 预估每列释放约 22MB
            if (current_rss - len(cols_to_drop) * 0.022) < self.SAFE_LEVEL_GB:
                break
        
        if cols_to_drop:
            self.df_daily = self.df_daily.drop(cols_to_drop)
            for c in cols_to_drop: self.column_metadata.pop(c, None)
            gc.collect() 
            print(f"Evicted {len(cols_to_drop)} columns: {cols_to_drop}")
        
        return self._get_current_rss() < self.RSS_LIMIT_GB

    def mount_jit_column(self, col_name: str, expression: pl.Expr):
        """即时挂载新指标"""
        if self.df_daily is None or col_name in self.df_daily.columns: return
        
        # 内存防御检查
        if not self.ensure_capacity():
            print(f"JIT Aborted: Memory full even after eviction.")
            return

        try:
            # 挂载新列并初始化元数据
            self.df_daily = self.df_daily.with_columns(expression.alias(col_name))
            self.update_col_usage(col_name)
            print(f"JIT Cache Success: {col_name}")
        except Exception as e:
            print(f"JIT Cache Error [{col_name}]: {e}")

    def load_data(self):
        print(f"Node {self.node_index}: Data loading sequence...")
        try:
            all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
            
            # 1. 加载个股
            stock_files = sorted([f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")])
            daily_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in stock_files]
            if daily_dfs:
                self.df_daily = pl.concat(daily_dfs).filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
                self.df_daily = self.df_daily.with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))

            # 2. 加载板块与映射 (略过重复逻辑...)
            sector_files = sorted([f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")])
            s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
            if s_dfs:
                self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))

            # 3. 映射表加载
            mapping_files = sorted([f for f in all_files if "constituents" in f and f.endswith(".parquet")])
            if mapping_files:
                path = hf_hub_download(repo_id=self.repo_id, filename=mapping_files[-1], repo_type="dataset", token=self.hf_token)
                self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

            # 4. 重采样
            self._resample_all()
            
            # 5. 初始进化
            self._evolve_indicators()

            # --- 核心修复点：None 检查 ---
            if self.df_daily is not None:
                self.column_metadata = {
                    c: {"hits": 0, "last_used": time.time()} 
                    for c in self.df_daily.columns 
                    if c not in self.IMMORTAL_COLS
                }
                print(f"Node {self.node_index}: Boot sequence complete.")
            else:
                print(f"Node {self.node_index}: ERROR - Daily data failed to load.")

        except Exception as e:
            print(f"Loading Failed: {e}")

    def _evolve_indicators(self):
        """开机拉取数据库热度指标"""
        if self.df_daily is None or not self.postgres_url: return
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("SELECT metric_key FROM metrics_stats ORDER BY usage_count DESC LIMIT 150")
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()
            
            exprs = []
            for key in top_keys:
                p = key.split('_')
                if len(p) == 3 and p[0] in self.INDICATOR_MAP:
                    exprs.append(self.INDICATOR_MAP[p[0]](pl.col(p[1].lower()), int(p[2])).alias(key))
            if exprs:
                self.df_daily = self.df_daily.with_columns(exprs)
                print(f"Pre-boot evolution: {len(exprs)} columns added.")
        except Exception as e:
            print(f"Evolve failed: {e}")

    def _resample_all(self):
        # 实现基本的重采样以填充 df_weekly/df_monthly (代码略，保持之前版本即可)
        pass

data_manager = DataManager()

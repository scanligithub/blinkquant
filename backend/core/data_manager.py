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
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        self.df_mapping = None

        # --- 内存管理配置 ---
        self.RSS_LIMIT_GB = 14.5
        self.SAFE_LEVEL_GB = 12.5
        self.IMMORTAL_COLS = {
            AShareDataSchema.DATE, AShareDataSchema.CODE, 
            AShareDataSchema.OPEN, AShareDataSchema.HIGH, 
            AShareDataSchema.LOW, AShareDataSchema.CLOSE, 
            AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT, 
            AShareDataSchema.PCT_CHG, AShareDataSchema.TURN,
            'sector_code', 's_close', 's_pctChg', 's_open', 's_high', 's_low'
        }
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }
        self.column_metadata = {}

    def update_col_usage(self, col_name: str):
        if col_name in self.column_metadata:
            self.column_metadata[col_name]["hits"] += 1
            self.column_metadata[col_name]["last_used"] = time.time()

    def _get_rss(self):
        return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3)

    def ensure_capacity(self):
        current_rss = self._get_rss()
        if current_rss < self.RSS_LIMIT_GB:
            return True
        dynamic_cols = [c for c in self.df_daily.columns if c not in self.IMMORTAL_COLS]
        if not dynamic_cols: return False
        candidates = sorted(dynamic_cols, key=lambda c: self.column_metadata.get(c, {}).get("hits", 0))
        to_drop = candidates[:3]
        if to_drop:
            self.df_daily = self.df_daily.drop(to_drop)
            for c in to_drop: self.column_metadata.pop(c, None)
            gc.collect()
        return self._get_rss() < self.RSS_LIMIT_GB

    def mount_jit_column(self, col_name: str, expression: pl.Expr):
        if self.df_daily is None or col_name in self.df_daily.columns: return
        self.ensure_capacity()
        try:
            self.df_daily = self.df_daily.with_columns(expression.alias(col_name))
            self.column_metadata[col_name] = {"hits": 1, "last_used": time.time()}
            print(f"JIT Success: {col_name}")
        except Exception as e:
            print(f"JIT Error {col_name}: {e}")

    def load_data(self):
        print(f"Node {self.node_index}: Starting full data load...")
        try:
            all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
            
            # 1. 个股行情加载 (真实的下载与过滤逻辑)
            stock_files = sorted([f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")])
            daily_dfs = []
            for f in stock_files:
                path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
                # 使用 scan_parquet 配合 filter 减少内存峰值
                lf = pl.scan_parquet(path)
                lf = lf.filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
                daily_dfs.append(lf.collect())
            
            if daily_dfs:
                self.df_daily = pl.concat(daily_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
                # 清洗数值列
                num_cols = [AShareDataSchema.OPEN, AShareDataSchema.HIGH, AShareDataSchema.LOW, AShareDataSchema.CLOSE, AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT, AShareDataSchema.PCT_CHG, AShareDataSchema.TURN]
                self.df_daily = self.df_daily.with_columns([pl.col(c).cast(pl.Float32, strict=False) for c in num_cols if c in self.df_daily.columns])
                print(f"Stocks loaded: {len(self.df_daily)} rows.")

            # 2. 板块数据加载 (全量)
            sector_files = sorted([f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")])
            s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
            if s_dfs:
                self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
                print(f"Sectors loaded: {len(self.df_sector_daily)} rows.")

            # 3. 映射表加载
            mapping_files = sorted([f for f in all_files if "constituents" in f and f.endswith(".parquet")])
            if mapping_files:
                path = hf_hub_download(repo_id=self.repo_id, filename=mapping_files[-1], repo_type="dataset", token=self.hf_token)
                self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

            # 4. 执行重采样与进化
            self._resample_all()
            self._evolve_indicators()

            # 5. 元数据初始化
            if self.df_daily is not None:
                self.column_metadata = {c: {"hits": 0, "last_used": time.time()} for c in self.df_daily.columns if c not in self.IMMORTAL_COLS}
                print(f"Node {self.node_index}: Boot sequence complete.")
            else:
                print(f"Node {self.node_index}: Data fail to Load!")
        except Exception as e:
            print(f"Loading Global Error: {e}")

    def _evolve_indicators(self):
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
            if exprs: self.df_daily = self.df_daily.with_columns(exprs)
        except Exception as e: print(f"Evolve failed: {e}")

    def _resample_all(self):
        if self.df_daily is None: return
        rules = [pl.col(AShareDataSchema.OPEN).first(), pl.col(AShareDataSchema.HIGH).max(), pl.col(AShareDataSchema.LOW).min(), pl.col(AShareDataSchema.CLOSE).last(), pl.col(AShareDataSchema.VOLUME).sum(), pl.col(AShareDataSchema.AMOUNT).sum()]
        self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(rules)
        self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(rules)

data_manager = DataManager()

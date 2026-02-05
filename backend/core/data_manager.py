import os
import polars as pl
import psycopg2
import logging
import time
from .data_types import AShareDataSchema
from huggingface_hub import hf_hub_download, list_repo_files

logger = logging.getLogger(__name__)

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
        self.df_mapping = None
        self.df_sector_daily = None
        
        self.column_metadata = {}
        # 核心指标算子定义
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def load_data(self):
        """分布式启动序列"""
        logger.info(f"Node {self.node_index}: Starting load...")
        try:
            all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
            
            # 1. 加载个股日线并分片
            stock_files = sorted([f for f in all_files if "stock_kline_" in f])
            daily_dfs = []
            for f in stock_files:
                path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
                lf = pl.scan_parquet(path)
                # 分片逻辑
                lf = lf.filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
                daily_dfs.append(lf.collect())
            
            if daily_dfs:
                self.df_daily = pl.concat(daily_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
                logger.info(f"Stocks Loaded: {len(self.df_daily)} rows")

            # 2. 加载板块数据
            sector_files = sorted([f for f in all_files if "sector_kline_" in f])
            s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
            if s_dfs:
                self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

            # 3. 加载映射表
            map_file = [f for f in all_files if "sector_constituents_2026" in f]
            if map_file:
                path = hf_hub_download(repo_id=self.repo_id, filename=map_file[0], repo_type="dataset", token=self.hf_token)
                self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

            # 4. 进化：从数据库同步热点指标
            self._evolve_from_db()
            
            # 5. 初始化元数据
            if self.df_daily is not None:
                immortal = {"date", "code", "open", "high", "low", "close", "volume", "amount"}
                self.column_metadata = {c: {"hits": 0} for c in self.df_daily.columns if c not in immortal}

        except Exception as e:
            logger.error(f"Load Error: {e}")

    def _evolve_from_db(self):
        """自进化：预计算高频指标"""
        if not self.postgres_url or self.df_daily is None: return
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("SELECT metric_key FROM metrics_stats ORDER BY usage_count DESC LIMIT 150")
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()

            if not top_keys: return
            logger.info(f"Evolving {len(top_keys)} indicators from DB...")
            
            exprs = []
            for key in top_keys:
                p = key.split('_') # MA_CLOSE_20
                if len(p) == 3 and p[0] in self.INDICATOR_MAP:
                    func, field, param = p[0], p[1].lower(), int(p[2])
                    if field in self.df_daily.columns:
                        exprs.append(self.INDICATOR_MAP[func](pl.col(field), param).alias(key))
            
            if exprs:
                self.df_daily = self.df_daily.with_columns(exprs)
                logger.info("Evolution complete.")
        except Exception as e:
            logger.error(f"Evolution failed: {e}")

    def mount_jit_column(self, col_name: str, expression: pl.Expr):
        """运行时动态挂载指标"""
        if self.df_daily is None or col_name in self.df_daily.columns: return
        try:
            self.df_daily = self.df_daily.with_columns(expression.alias(col_name))
            if col_name not in self.column_metadata:
                self.column_metadata[col_name] = {"hits": 1}
        except: pass

data_manager = DataManager()

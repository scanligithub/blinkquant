import os
import polars as pl
import psycopg2
import logging
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
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def load_data(self):
        """标准加载流程 (略，保持不变)"""
        try:
            # ... (这部分代码与上一版相同，负责加载 parquet 和 resample) ...
            # 为了节省篇幅，这里假设前面的 list_repo_files, scan_parquet, resample 都已就绪
            # 直接调用 load_data 的框架逻辑
            self._load_raw_parquet() 
            self._resample_all()
            self._evolve_from_db()
        except Exception as e:
            logger.error(f"Load Error: {e}")

    # 将之前的 load_data 拆解为 _load_raw_parquet 便于阅读
    def _load_raw_parquet(self):
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # 1. Stocks
        stock_files = sorted([f for f in all_files if "stock_kline_" in f])
        daily_dfs = []
        for f in stock_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            lf = pl.scan_parquet(path).filter((pl.col("code").hash() % self.total_nodes) == self.node_index)
            daily_dfs.append(lf.collect())
        if daily_dfs:
            self.df_daily = pl.concat(daily_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

        # 2. Sectors
        sector_files = sorted([f for f in all_files if "sector_kline_" in f])
        s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

        # 3. Mapping
        map_file = [f for f in all_files if "sector_constituents" in f]
        if map_file:
            path = hf_hub_download(repo_id=self.repo_id, filename=map_file[-1], repo_type="dataset", token=self.hf_token)
            self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

    def _resample_all(self):
        if self.df_daily is None: return
        aggs = [pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(), pl.col("volume").sum(), pl.col("amount").sum()]
        
        self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
        self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        
        if self.df_sector_daily is not None:
            self.df_sector_weekly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)

    def _evolve_from_db(self):
        """从数据库进化：清洗后缀，使用统一名称挂载"""
        if not self.postgres_url: return
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("SELECT metric_key FROM metrics_stats ORDER BY usage_count DESC LIMIT 200")
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()

            groups = {'D': [], 'W': [], 'M': []}
            for k in top_keys:
                if k.endswith('_W'): groups['W'].append(k)
                elif k.endswith('_M'): groups['M'].append(k)
                else: groups['D'].append(k)

            self._apply_evolution('df_daily', groups['D'], "")
            self._apply_evolution('df_weekly', groups['W'], "_W")
            self._apply_evolution('df_monthly', groups['M'], "_M")
        except Exception as e:
            logger.error(f"Evolution failed: {e}")

    def _apply_evolution(self, df_attr, db_keys, suffix):
        df = getattr(self, df_attr)
        if df is None or not db_keys: return
        
        exprs = []
        for db_key in db_keys:
            # 1. 剥离后缀：MA_CLOSE_20_W -> MA_CLOSE_20
            pure_name = db_key
            if suffix and db_key.endswith(suffix):
                pure_name = db_key[:-len(suffix)]
            
            # 2. 解析参数
            parts = pure_name.split('_')
            if len(parts) == 3:
                func, field, param = parts[0], parts[1].lower(), int(parts[2])
                if field in df.columns:
                    # 3. 挂载为统一名称
                    exprs.append(self.INDICATOR_MAP[func](pl.col(field), param).alias(pure_name))
        
        if exprs:
            setattr(self, df_attr, df.with_columns(exprs))
            logger.info(f"Evolved {len(exprs)} metrics on {df_attr} (Aliased as pure names)")

data_manager = DataManager()

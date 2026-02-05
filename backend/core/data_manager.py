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

        # 三大核心数据表
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        
        # 辅助数据表
        self.df_mapping = None
        self.df_sector_daily = None
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        
        # 指标定义算子
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def load_data(self):
        """算力节点分布式启动序列"""
        try:
            all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
            
            # 1. 加载个股日线 (带分片逻辑)
            stock_files = sorted([f for f in all_files if "stock_kline_" in f])
            daily_dfs = []
            for f in stock_files:
                path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
                lf = pl.scan_parquet(path)
                lf = lf.filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
                daily_dfs.append(lf.collect())
            
            if daily_dfs:
                self.df_daily = pl.concat(daily_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
                logger.info(f"Node {self.node_index}: Stocks Loaded ({len(self.df_daily)} rows)")

            # 2. 加载板块数据
            sector_files = sorted([f for f in all_files if "sector_kline_" in f])
            s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
            if s_dfs:
                self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

            # 3. 执行重采样 (生成周线与月线)
            self._resample_all()

            # 4. 加载映射表 (个股 -> 板块)
            map_file = [f for f in all_files if "sector_constituents" in f]
            if map_file:
                path = hf_hub_download(repo_id=self.repo_id, filename=map_file[-1], repo_type="dataset", token=self.hf_token)
                self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

            # 5. 全周期自进化指标预计算
            self._evolve_from_db()

        except Exception as e:
            logger.error(f"Critical Loading Error: {e}")

    def _resample_all(self):
        """同步生成个股与板块的周线/月线数据"""
        if self.df_daily is None: return
        
        # 重采样规则
        aggs = [
            pl.col("open").first(), pl.col("high").max(), 
            pl.col("low").min(), pl.col("close").last(), 
            pl.col("volume").sum(), pl.col("amount").sum()
        ]
        
        # 个股重采样
        self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
        self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        
        # 板块重采样
        if self.df_sector_daily is not None:
            self.df_sector_weekly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        
        logger.info("Resampling Complete (D -> W, M)")

    def _evolve_from_db(self):
        """自进化：跨周期预计算"""
        if not self.postgres_url: return
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("SELECT metric_key FROM metrics_stats ORDER BY usage_count DESC LIMIT 200")
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()

            # 指标分组
            groups = {'D': [], 'W': [], 'M': []}
            for k in top_keys:
                if k.endswith('_W'): groups['W'].append(k)
                elif k.endswith('_M'): groups['M'].append(k)
                else: groups['D'].append(k)

            # 分别应用
            self._apply_evolution('df_daily', groups['D'], "")
            self._apply_evolution('df_weekly', groups['W'], "_W")
            self._apply_evolution('df_monthly', groups['M'], "_M")
        except Exception as e:
            logger.error(f"Evolution failed: {e}")

    def _apply_evolution(self, df_attr, keys, suffix):
        df = getattr(self, df_attr)
        if df is None or not keys: return
        
        exprs = []
        for key in keys:
            # 去除后缀解析参数，如 MA_CLOSE_20_W -> MA, CLOSE, 20
            clean_key = key.replace('_W', '').replace('_M', '')
            parts = clean_key.split('_')
            if len(parts) == 3:
                func, field, param = parts[0], parts[1].lower(), int(parts[2])
                if field in df.columns:
                    exprs.append(self.INDICATOR_MAP[func](pl.col(field), param).alias(key))
        
        if exprs:
            setattr(self, df_attr, df.with_columns(exprs))
            logger.info(f"Evolved {len(exprs)} metrics on {df_attr}")

data_manager = DataManager()

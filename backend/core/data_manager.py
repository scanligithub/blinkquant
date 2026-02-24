import os
import gc
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
        """启动加载流程"""
        try:
            # 1. 加载并优化日线数据
            self._load_raw_parquet()
            
            # 显式 GC
            gc.collect()
            logger.info(f"Node {self.node_index}: Raw data loaded. Memory optimized.")

            # 2. 生成周/月线
            self._resample_all()
            
            # 3. 进化指标 (分批处理)
            self._evolve_from_db()
            
            gc.collect()
            logger.info(f"Node {self.node_index}: Boot sequence complete.")
        except Exception as e:
            logger.error(f"Load Error: {e}")

    def _load_raw_parquet(self):
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # --- 1. Stocks (Lazy Loading + Streaming) ---
        stock_files = sorted([f for f in all_files if "stock_kline_" in f])
        lazy_frames = []
        
        for f in stock_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            # 仅构建 LazyFrame，不立即加载
            lf = pl.scan_parquet(path).filter((pl.col("code").hash() % self.total_nodes) == self.node_index)
            lazy_frames.append(lf)
            
        if lazy_frames:
            # 使用 streaming=True 避免内存峰值
            # 并在加载后立即转换日期格式
            self.df_daily = pl.concat(lazy_frames).collect(streaming=True)
            self.df_daily = self.df_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
            self._optimize_memory(self.df_daily, "df_daily")

        # --- 2. Sectors (Direct Load) ---
        # 板块数据量较小，可以直接加载，但同样做一下优化
        sector_files = sorted([f for f in all_files if "sector_kline_" in f])
        s_dfs = []
        for f in sector_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            s_dfs.append(pl.read_parquet(path))
            
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
            self._optimize_memory(self.df_sector_daily, "df_sector_daily")

        # --- 3. Mapping ---
        map_file = [f for f in all_files if "sector_constituents" in f]
        if map_file:
            path = hf_hub_download(repo_id=self.repo_id, filename=map_file[-1], repo_type="dataset", token=self.hf_token)
            self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

    def _optimize_memory(self, df: pl.DataFrame, name: str):
        """将 Float64 强制转换为 Float32 以节省 50% 内存"""
        if df is None: return
        
        # 查找所有 Float64 列
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64]
        if f64_cols:
            df = df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
            
            # 回写到类属性
            if name == "df_daily": self.df_daily = df
            elif name == "df_sector_daily": self.df_sector_daily = df
            
            logger.info(f"Optimized {name}: Converted {len(f64_cols)} columns to Float32")

    def _resample_all(self):
        if self.df_daily is None: return
        aggs = [pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(), pl.col("volume").sum(), pl.col("amount").sum()]
        
        # Resample logic
        self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
        self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        
        if self.df_sector_daily is not None:
            self.df_sector_weekly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(aggs)

    def _evolve_from_db(self):
        """自进化：分批广播，防止内存溢出"""
        if not self.postgres_url: return
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT metric_key FROM metrics_stats 
                WHERE metric_key NOT LIKE '%_W' AND metric_key NOT LIKE '%_M'
                ORDER BY usage_count DESC LIMIT 150
            """)
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()

            if not top_keys: return

            target_dfs = [('df_daily', self.df_daily), ('df_weekly', self.df_weekly), ('df_monthly', self.df_monthly)]
            
            # --- 分批处理 (Batch Processing) ---
            BATCH_SIZE = 20
            
            for attr_name, df in target_dfs:
                if df is None: continue
                
                # 按批次遍历指标
                total_added = 0
                for i in range(0, len(top_keys), BATCH_SIZE):
                    batch = top_keys[i : i + BATCH_SIZE]
                    exprs = []
                    
                    for key in batch:
                        parts = key.split('_')
                        if len(parts) == 3:
                            func, field, param = parts[0], parts[1].lower(), int(parts[2])
                            if field in df.columns and key not in df.columns:
                                try:
                                    exprs.append(self.INDICATOR_MAP[func](pl.col(field), param).alias(key))
                                except: pass
                    
                    if exprs:
                        df = df.with_columns(exprs)
                        total_added += len(exprs)
                        # 每一批处理完，显式 GC
                        gc.collect()

                # 更新 DataManager 引用
                setattr(self, attr_name, df)
                logger.info(f"Broadcast Evolution: Added {total_added} metrics to {attr_name}")

        except Exception as e:
            logger.error(f"Evolution failed: {e}")

data_manager = DataManager()

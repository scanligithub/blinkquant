import os
import polars as pl
import psycopg2
from huggingface_hub import hf_hub_download, list_repo_files
import psutil
from .data_types import AShareDataSchema

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL") # Vercel Postgres 自动注入
        self.repo_id = "scanli/stocka-data"

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.df_sector_daily = None
        self.df_mapping = None
        
        # 允许预计算的算子白名单
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def _clean_numeric_columns(self, df):
        numeric_cols = [
            AShareDataSchema.OPEN, AShareDataSchema.HIGH, AShareDataSchema.LOW, 
            AShareDataSchema.CLOSE, AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT,
            AShareDataSchema.TURN, AShareDataSchema.PCT_CHG
        ]
        existing_cols = [c for c in numeric_cols if c in df.columns]
        return df.with_columns([
            pl.col(c).cast(pl.Float32, strict=False) for c in existing_cols
        ])

    def load_data(self):
        print(f"Node {self.node_index}: Loading sequence starting...")
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # 1. 加载个股数据 (与之前相同)
        stock_files = sorted([f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")])
        daily_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in stock_files]
        if daily_dfs:
            self.df_daily = pl.concat(daily_dfs).filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
            self.df_daily = self.df_daily.with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
            self.df_daily = self._clean_numeric_columns(self.df_daily)

        # 2. 加载板块数据
        sector_files = sorted([f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")])
        s_dfs = [pl.read_parquet(hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")) for f in sector_files]
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
            self.df_sector_daily = self._clean_numeric_columns(self.df_sector_daily)

        # 3. 加载映射表
        mapping_files = sorted([f for f in all_files if "constituents" in f and f.endswith(".parquet")])
        if mapping_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=mapping_files[-1], repo_type="dataset", token=self.hf_token)
            self.df_mapping = pl.read_parquet(path).select([pl.col("stock_code").alias("code"), pl.col("sector_code")])

        # 4. 执行重采样
        self._resample_all()
        
        # 5. 【Step 5 核心】自进化指标挂载
        self._evolve_indicators()

    def _evolve_indicators(self):
        """
        从 Vercel Postgres 获取高频指标并预计算挂载
        """
        if self.df_daily is None or not self.postgres_url:
            return

        print(f"Node {self.node_index}: Starting Self-Evolution process...")
        try:
            # 连接数据库获取 Top 150
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            cur.execute("SELECT metric_key FROM metrics_stats ORDER BY usage_count DESC LIMIT 150")
            top_metrics = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            if not top_metrics:
                print("No evolution data found in DB. Skipping.")
                return

            evolve_exprs = []
            for key in top_metrics:
                # 检查内存水位 (防御红线: 4GB)
                available_gb = psutil.virtual_memory().available / (1024**3)
                if available_gb < 4.0:
                    print(f"Memory warning! Stopping evolution at {key}. Avail: {available_gb:.2f}GB")
                    break
                
                # 解析 Key 格式: TYPE_FIELD_PARAM (e.g., MA_CLOSE_20)
                parts = key.split('_')
                if len(parts) != 3: continue
                
                func_name, field, param = parts[0], parts[1].lower(), int(parts[2])
                
                if func_name in self.INDICATOR_MAP and field in self.df_daily.columns:
                    expr = self.INDICATOR_MAP[func_name](pl.col(field), param)
                    evolve_exprs.append(expr.alias(key))
            
            # 批量并行挂载
            if evolve_exprs:
                self.df_daily = self.df_daily.with_columns(evolve_exprs)
                print(f"Evolution Complete. Successfully pre-calculated {len(evolve_exprs)} indicators.")

        except Exception as e:
            print(f"Evolution Failed: {str(e)}")

    def _resample_all(self):
        # (保持原有的重采样逻辑不变...)
        pass

data_manager = DataManager()

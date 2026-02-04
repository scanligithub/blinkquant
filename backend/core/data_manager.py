import os
import polars as pl
from huggingface_hub import hf_hub_download, list_repo_files
from .data_types import AShareDataSchema

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.repo_id = "scanli/stocka-data"

        # 内存存储
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        
        self.df_sector_daily = None
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        
        # 个股与板块的映射 (stock_code -> sector_code)
        self.df_mapping = None

    def load_data(self):
        print(f"Node {self.node_index}: Starting data loading sequence...")
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # 1. 加载个股数据 (分片)
        stock_files = sorted([f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")])
        daily_dfs = []
        for file in stock_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=file, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            lf = pl.scan_parquet(path)
            # 分片过滤逻辑
            lf = lf.filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
            daily_dfs.append(lf.collect())
            
        if daily_dfs:
            self.df_daily = pl.concat(daily_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
            print(f"Stock Data Loaded. Rows: {len(self.df_daily)}")

        # 2. 加载板块数据 (全量复制)
        sector_files = sorted([f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")])
        s_dfs = []
        for file in sector_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=file, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            s_dfs.append(pl.read_parquet(path))
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d"))
            print(f"Sector Data Loaded. Rows: {len(self.df_sector_daily)}")

        # 3. 加载成分股映射 (Replicated)
        mapping_files = sorted([f for f in all_files if "constituents" in f and f.endswith(".parquet")])
        if mapping_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=mapping_files[-1], repo_type="dataset", token=self.hf_token)
            
            # 【修正点】：将 stock_code 重命名为 code，确保与个股行情表的字段名一致
            self.df_mapping = pl.read_parquet(path).select([
                pl.col("stock_code").alias("code"), 
                pl.col("sector_code")
            ])
            print(f"Stock-Sector Mapping Loaded. Count: {len(self.df_mapping)}")

        # 4. 执行全量重采样
        self._resample_all()

    def _resample_all(self):
        print("Resampling Weekly/Monthly data for Stocks and Sectors...")
        agg_rules = [
            pl.col(AShareDataSchema.OPEN).first(),
            pl.col(AShareDataSchema.HIGH).max(),
            pl.col(AShareDataSchema.LOW).min(),
            pl.col(AShareDataSchema.CLOSE).last(),
            pl.col(AShareDataSchema.VOLUME).sum(),
            pl.col(AShareDataSchema.AMOUNT).sum(),
            pl.col(AShareDataSchema.PCT_CHG).sum(),
        ]

        if self.df_daily is not None:
            self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(agg_rules)
            self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(agg_rules)

        if self.df_sector_daily is not None:
            self.df_sector_weekly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(agg_rules)
            self.df_sector_monthly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(agg_rules)
        print("Resampling Complete.")

data_manager = DataManager()

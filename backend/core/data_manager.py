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

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.df_sector_daily = None
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        self.df_mapping = None

    def _clean_numeric_columns(self, df):
        """强制转换数值列，处理可能的字符串脏数据"""
        numeric_cols = [
            AShareDataSchema.OPEN, AShareDataSchema.HIGH, AShareDataSchema.LOW, 
            AShareDataSchema.CLOSE, AShareDataSchema.VOLUME, AShareDataSchema.AMOUNT,
            AShareDataSchema.TURN, AShareDataSchema.PCT_CHG
        ]
        # 只转换存在于当前 df 中的列
        existing_cols = [c for c in numeric_cols if c in df.columns]
        return df.with_columns([
            pl.col(c).cast(pl.Float64, strict=False) for c in existing_cols
        ])

    def load_data(self):
        print(f"Node {self.node_index}: Starting data loading sequence...")
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # 1. 加载个股数据
        stock_files = sorted([f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")])
        daily_dfs = []
        for file in stock_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=file, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            lf = pl.scan_parquet(path)
            lf = lf.filter((pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index)
            daily_dfs.append(lf.collect())
            
        if daily_dfs:
            self.df_daily = pl.concat(daily_dfs).with_columns(
                pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d")
            )
            self.df_daily = self._clean_numeric_columns(self.df_daily)
            print(f"Stock Data Loaded. Rows: {len(self.df_daily)}")

        # 2. 加载板块数据
        sector_files = sorted([f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")])
        s_dfs = []
        for file in sector_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=file, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            s_dfs.append(pl.read_parquet(path))
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(
                pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d")
            )
            self.df_sector_daily = self._clean_numeric_columns(self.df_sector_daily)
            print(f"Sector Data Loaded. Rows: {len(self.df_sector_daily)}")

        # 3. 加载映射表
        mapping_files = sorted([f for f in all_files if "constituents" in f and f.endswith(".parquet")])
        if mapping_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=mapping_files[-1], repo_type="dataset", token=self.hf_token)
            self.df_mapping = pl.read_parquet(path).select([
                pl.col("stock_code").alias("code"), 
                pl.col("sector_code")
            ])
            print("Stock-Sector Mapping Loaded.")

        # 4. 执行重采样
        self._resample_all()

    def _resample_all(self):
        print("Resampling Weekly/Monthly data...")
        
        def get_rules(df):
            rules = [
                pl.col(AShareDataSchema.OPEN).first(),
                pl.col(AShareDataSchema.HIGH).max(),
                pl.col(AShareDataSchema.LOW).min(),
                pl.col(AShareDataSchema.CLOSE).last(),
                pl.col(AShareDataSchema.VOLUME).sum(),
                pl.col(AShareDataSchema.AMOUNT).sum(),
            ]
            if AShareDataSchema.PCT_CHG in df.columns: rules.append(pl.col(AShareDataSchema.PCT_CHG).sum())
            if AShareDataSchema.TURN in df.columns: rules.append(pl.col(AShareDataSchema.TURN).sum())
            return rules

        if self.df_daily is not None:
            rules = get_rules(self.df_daily)
            self.df_weekly = self.df_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(rules)
            self.df_monthly = self.df_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(rules)

        if self.df_sector_daily is not None:
            rules = get_rules(self.df_sector_daily)
            self.df_sector_weekly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1w", by="code").agg(rules)
            self.df_sector_monthly = self.df_sector_daily.sort("date").group_by_dynamic("date", every="1mo", by="code").agg(rules)
        print("Resampling Complete.")

data_manager = DataManager()

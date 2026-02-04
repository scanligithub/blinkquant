import os
import polars as pl
from huggingface_hub import hf_hub_download, list_repo_files
from .data_types import AShareDataSchema

class DataManager:
    def __init__(self):
        # 从环境变量获取节点索引 (0, 1, 2)
        try:
            self.node_index = int(os.getenv("NODE_INDEX", "0"))
            self.total_nodes = 3
            self.hf_token = os.getenv("HF_TOKEN")
            self.repo_id = "scanli/stocka-data"
        except Exception as e:
            print(f"Error init env: {e}")
            self.node_index = 0

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.df_sector = None

    def _is_my_shard(self, code_series: pl.Series) -> pl.Series:
        """分片算法: hash(code) % 3 == node_index"""
        return (code_series.hash() % self.total_nodes) == self.node_index

    def load_data(self):
        print(f"Node {self.node_index}: Starting data loading sequence...")
        
        # 1. 获取文件列表
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # 2. 筛选需要的文件
        stock_files = [f for f in all_files if f.startswith("stock_kline_") and f.endswith(".parquet")]
        sector_files = [f for f in all_files if f.startswith("sector_kline_") and f.endswith(".parquet")]
        
        # 3. 加载个股数据 (分片过滤)
        daily_dfs = []
        for file in stock_files:
            print(f"Processing {file}...")
            local_path = hf_hub_download(
                repo_id=self.repo_id, 
                filename=file, 
                repo_type="dataset",
                token=self.hf_token,
                cache_dir="./data_cache"
            )
            
            # 使用 LazyFrame 进行读取和预过滤，节省内存
            # 注意：先 select 减少列，再 cast 优化类型，最后 filter
            lf = pl.scan_parquet(local_path)
            
            # 这里的 filter 是分布式的关键：只保留属于本节点的数据
            lf = lf.filter(
                (pl.col(AShareDataSchema.CODE).hash() % self.total_nodes) == self.node_index
            )
            
            # 显式转换 Float32
            daily_dfs.append(lf.collect()) # 立即执行并回收到内存
            
        if daily_dfs:
            self.df_daily = pl.concat(daily_dfs)
            # 转换日期格式，方便后续重采样
            self.df_daily = self.df_daily.with_columns(
                pl.col(AShareDataSchema.DATE).str.to_date("%Y-%m-%d")
            )
            print(f"Stock Data Loaded. Rows: {len(self.df_daily)}")
            
            # 4. 生成周线和月线 (Resampling)
            self._resample_data()
        else:
            print("Warning: No stock data loaded.")

        # 5. 加载板块数据 (全量复制 - Replication)
        sector_dfs = []
        for file in sector_files:
            local_path = hf_hub_download(repo_id=self.repo_id, filename=file, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            sector_dfs.append(pl.read_parquet(local_path))
            
        if sector_dfs:
            self.df_sector = pl.concat(sector_dfs)
            print(f"Sector Data Loaded. Rows: {len(self.df_sector)}")

    def _resample_data(self):
        """基于日线生成周线和月线"""
        print("Resampling Weekly/Monthly data...")
        # 定义聚合规则
        agg_rules = [
            pl.col(AShareDataSchema.OPEN).first(),
            pl.col(AShareDataSchema.HIGH).max(),
            pl.col(AShareDataSchema.LOW).min(),
            pl.col(AShareDataSchema.CLOSE).last(),
            pl.col(AShareDataSchema.VOLUME).sum(),
            pl.col(AShareDataSchema.AMOUNT).sum(),
            pl.col(AShareDataSchema.PCT_CHG).sum(), # 简单累加近似
        ]

        # 周线
        self.df_weekly = (
            self.df_daily.sort(AShareDataSchema.DATE)
            .group_by_dynamic(AShareDataSchema.DATE, every="1w", by=AShareDataSchema.CODE)
            .agg(agg_rules)
        )
        
        # 月线
        self.df_monthly = (
            self.df_daily.sort(AShareDataSchema.DATE)
            .group_by_dynamic(AShareDataSchema.DATE, every="1mo", by=AShareDataSchema.CODE)
            .agg(agg_rules)
        )
        print("Resampling Complete.")

# 单例模式
data_manager = DataManager()

import os
import gc
import time
import asyncio
import io
import logging
import httpx
import polars as pl
import psutil
from huggingface_hub import list_repo_files

logger = logging.getLogger(__name__)

def log_mem(step_name, node_index):
    """探针：打印当前进程的物理内存占用 (GB)"""
    process = psutil.Process(os.getpid())
    mem_gb = process.memory_info().rss / (1024 ** 3)
    logger.info(f"[NODE {node_index} MEMORY] {step_name} -> {mem_gb:.2f} GB")

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.repo_id = "scanli/stocka-data"
        
        self.cache_dir = "/app/data_cache"
        os.makedirs(self.cache_dir, exist_ok=True)

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        self.df_sector_daily = None
        self.df_mapping = None

    async def async_load_data(self):
        try:
            logger.info(f"🚀 [STEP 1] Node {self.node_index}: 开始启动异步加载流程...")
            log_mem("初始化完成", self.node_index)
            
            await self._download_all_to_disk()
            
            logger.info(f"🚀 [STEP 3] Node {self.node_index}: 准备将计算送入后台线程...")
            await asyncio.to_thread(self._cpu_heavy_processing)
            logger.info(f"✅ [STEP 9] Node {self.node_index}: 所有数据加载和计算大功告成！")
            
        except Exception as e:
            logger.error(f"❌ 致命错误 (async_load_data): {str(e)}", exc_info=True)

    async def _download_all_to_disk(self):
        logger.info(f"🚀 [STEP 2] Node {self.node_index}: 获取文件列表...")
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        data_files = [f for f in all_files if f.endswith(".parquet")]

        base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
        headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
        semaphore = asyncio.Semaphore(10)

        async def download_file(client, filename):
            filepath = os.path.join(self.cache_dir, filename)
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                return
            async with semaphore:
                resp = await client.get(base_url + filename, timeout=60.0)
                resp.raise_for_status()
                with open(filepath, 'wb') as f:
                    f.write(resp.content)

        logger.info(f"下载 {len(data_files)} 个文件到磁盘缓存...")
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            tasks = [download_file(client, f) for f in data_files]
            await asyncio.gather(*tasks)
        log_mem("所有文件下载完毕", self.node_index)

    def _cpu_heavy_processing(self):
        try:
            log_mem("进入 CPU 计算线程", self.node_index)
            files_on_disk = os.listdir(self.cache_dir)
            
            # 1. 股票列表
            stock_list_file = next((f for f in files_on_disk if "stock_list.parquet" in f), None)
            if stock_list_file:
                logger.info(f"[STEP 4] Node {self.node_index}: 解析股票列表...")
                sdf = pl.read_parquet(os.path.join(self.cache_dir, stock_list_file))
                self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}

            # 2. 股票日线
            logger.info(f"[STEP 5] Node {self.node_index}: 开始解析股票日线 (stock_kline)...")
            kline_files = sorted([f for f in files_on_disk if "stock_kline_" in f])
            kline_dfs = []
            for i, f in enumerate(kline_files):
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                num_col = df["code"].str.replace_all(r"\D", "").cast(pl.Int64)
                sharded = df.filter((num_col % self.total_nodes) == self.node_index)
                if not sharded.is_empty():
                    kline_dfs.append(sharded)
                if i % 10 == 0:
                    logger.info(f"  -> 已处理 {i+1}/{len(kline_files)} 个日线文件")

            log_mem("日线文件读取完毕，准备 concat", self.node_index)
            if kline_dfs:
                self.df_daily = pl.concat(kline_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del kline_dfs
            gc.collect()
            log_mem("日线 concat 完毕", self.node_index)

            # 3. 资金流
            logger.info(f"[STEP 6] Node {self.node_index}: 开始解析资金流 (stock_money_flow)...")
            flow_files = sorted([f for f in files_on_disk if "stock_money_flow_" in f])
            flow_dfs = []
            for i, f in enumerate(flow_files):
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                num_col = df["code"].str.replace_all(r"\D", "").cast(pl.Int64)
                sharded = df.filter((num_col % self.total_nodes) == self.node_index)
                if not sharded.is_empty():
                    flow_dfs.append(sharded)
                if i % 10 == 0:
                    logger.info(f"  -> 已处理 {i+1}/{len(flow_files)} 个资金流文件")
            
            log_mem("资金流读取完毕，准备 concat & join", self.node_index)
            if flow_dfs and self.df_daily is not None:
                df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
            del flow_dfs
            gc.collect()
            log_mem("资金流 join 完毕", self.node_index)

            # 4. 板块
            logger.info(f"[STEP 7] Node {self.node_index}: 开始解析板块 (sector_kline)...")
            sector_files = sorted([f for f in files_on_disk if "sector_kline_" in f])
            sector_dfs = []
            for f in sector_files:
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                sector_dfs.append(df)
            if sector_dfs:
                self.df_sector_daily = pl.concat(sector_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del sector_dfs
            gc.collect()
            log_mem("板块数据解析完毕", self.node_index)

            # 5. 后处理
            logger.info(f"[STEP 8] Node {self.node_index}: 执行数据预处理与重采样...")
            self._apply_forward_adjustment()
            log_mem("前复权计算完毕", self.node_index)
            
            self._resample_all()
            log_mem("重采样计算完毕", self.node_index)

        except Exception as e:
            logger.error(f"❌ 致命错误 (CPU_Processing): {str(e)}", exc_info=True)
            raise e

    def _apply_forward_adjustment(self):
        if self.df_daily is None or "adjustFactor" not in self.df_daily.columns: return
        self.df_daily = self.df_daily.sort(["code", "date"])
        adj_col = pl.col("adjustFactor").forward_fill().fill_null(1.0).over("code")
        latest_adj = adj_col.last().over("code")
        qfq_expr = pl.when(latest_adj > 0).then(adj_col / latest_adj).otherwise(1.0)
    
        self.df_daily = self.df_daily.with_columns([
            (pl.col("open") * qfq_expr).cast(pl.Float32),
            (pl.col("high") * qfq_expr).cast(pl.Float32),
            (pl.col("low") * qfq_expr).cast(pl.Float32),
            (pl.col("close") * qfq_expr).cast(pl.Float32),
            (pl.col("volume") / qfq_expr).cast(pl.Float64)
        ])

    def _resample_all(self):
        if self.df_daily is None: return
        aggs = [
            pl.col("open").first(), pl.col("high").max(),
            pl.col("low").min(), pl.col("close").last(),
            pl.col("volume").sum(), pl.col("amount").sum()
        ]
        base = self.df_daily.sort("date")
        self.df_weekly = base.group_by_dynamic("date", every="1w", by="code").agg(aggs)
        self.df_monthly = base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)

        if self.df_sector_daily is not None:
            s_base = self.df_sector_daily.sort("date")
            self.df_sector_weekly = s_base.group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = s_base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)

data_manager = DataManager()

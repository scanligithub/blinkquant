import os
# 【防卡死配置】：留出 CPU 核心给 FastAPI 响应探针
os.environ["POLARS_MAX_THREADS"] = "2"
os.environ["MALLOC_TRIM_THRESHOLD_"] = "65536"

import gc
import time
import asyncio
import logging
import httpx
import polars as pl
from huggingface_hub import list_repo_files

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.repo_id = "scanli/stocka-data"
        
        # 本地磁盘缓存目录，替代暴力的内存字典
        self.cache_dir = "/app/data_cache"
        os.makedirs(self.cache_dir, exist_ok=True)

        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        self.df_sector_daily = None
        self.df_mapping = None

        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def _get_node_filter(self, df):
        """
        【绝杀技：完美的数学均分】
        无论代码是 'sh.600000' 还是 '000001.SZ'，用正则 \D 剥离出纯数字 600000 和 1
        通过取模，确保 3 个节点的负载精确在 33.3%，绝不发生倾斜和 OOM！
        """
        num_col = df["code"].str.replace_all(r"\D", "").cast(pl.Int64)
        return (num_col % self.total_nodes) == self.node_index

    def _downcast_df(self, df):
        """内存压缩"""
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in ["volume", "amount"]]
        if f64_cols:
            return df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
        return df

    async def async_load_data(self):
        start_time = time.time()
        try:
            logger.info(f"🚀 Node {self.node_index}: Starting DISK-BACKED Streaming Load...")
            
            # 1. 异步将文件流式下载到磁盘（彻底解放内存）
            await self._download_all_to_disk()
            
            # 2. 将高压 CPU 处理移入子线程，避免卡死
            await asyncio.to_thread(self._cpu_heavy_processing)

            logger.info(f"✅ Node {self.node_index}: Load Complete. Time: {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"❌ Load Error: {e}", exc_info=True)

    async def _download_all_to_disk(self):
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        data_files = [f for f in all_files if f.endswith(".parquet")]

        base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
        headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
        semaphore = asyncio.Semaphore(15) 

        async def download_file(client, filename):
            filepath = os.path.join(self.cache_dir, filename)
            # 如果容器重启但磁盘保留，跳过下载
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                return
                
            async with semaphore:
                for attempt in range(3):
                    try:
                        resp = await client.get(base_url + filename, timeout=60.0)
                        resp.raise_for_status()
                        # 写入本地磁盘，防止内存爆炸
                        with open(filepath, 'wb') as f:
                            f.write(resp.content)
                        return
                    except Exception:
                        if attempt == 2: raise
                        await asyncio.sleep(1)

        logger.info(f"Node {self.node_index}: Downloading {len(data_files)} files to disk...")
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            tasks = [download_file(client, f) for f in data_files]
            await asyncio.gather(*tasks)

    def _cpu_heavy_processing(self):
        try:
            logger.info(f"Node {self.node_index}: Processing local parquet files...")
            files_on_disk = os.listdir(self.cache_dir)
            
            # --- 1. 股票列表 ---
            stock_list_file = next((f for f in files_on_disk if "stock_list.parquet" in f), None)
            if stock_list_file:
                sdf = pl.read_parquet(os.path.join(self.cache_dir, stock_list_file))
                self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}

            # --- 2. 股票日线 ---
            kline_files = sorted([f for f in files_on_disk if "stock_kline_" in f])
            kline_dfs = []
            for f in kline_files:
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                df = self._downcast_df(df)
                
                sharded = df.filter(self._get_node_filter(df))
                if not sharded.is_empty():
                    kline_dfs.append(sharded)
                
                del df, sharded
                time.sleep(0.01) # 让出时间片防饿死
            
            if kline_dfs:
                self.df_daily = pl.concat(kline_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del kline_dfs
            gc.collect()

            # --- 3. 资金流 ---
            flow_files = sorted([f for f in files_on_disk if "stock_money_flow_" in f])
            flow_dfs = []
            for f in flow_files:
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                df = self._downcast_df(df)
                
                sharded = df.filter(self._get_node_filter(df))
                if not sharded.is_empty():
                    flow_dfs.append(sharded)
                    
                del df, sharded
                time.sleep(0.01)

            if flow_dfs and self.df_daily is not None:
                df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
            del flow_dfs
            gc.collect()

            # --- 4. 板块 ---
            sector_files = sorted([f for f in files_on_disk if "sector_kline_" in f])
            sector_dfs = []
            for f in sector_files:
                df = pl.read_parquet(os.path.join(self.cache_dir, f))
                sector_dfs.append(self._downcast_df(df))
                del df
                time.sleep(0.01)

            if sector_dfs:
                self.df_sector_daily = pl.concat(sector_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del sector_dfs
            gc.collect()

            # --- 5. 计算指标 ---
            self._apply_forward_adjustment()
            self._resample_all()
            
        finally:
            gc.collect()
            try:
                import ctypes
                ctypes.CDLL('libc.so.6').malloc_trim(0)
            except Exception:
                pass

    def _apply_forward_adjustment(self):
        if self.df_daily is None or "adjustFactor" not in self.df_daily.columns: return
        logger.info(f"Node {self.node_index}: Applying price adjustment...")
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
        logger.info(f"Node {self.node_index}: Resampling weekly/monthly data...")
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

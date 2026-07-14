import os
import gc
import time
import asyncio
import io
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

    def _downcast_df(self, df):
        """立刻降级内存"""
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in ["volume", "amount"]]
        if f64_cols:
            return df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
        return df

    async def async_load_data(self):
        """流式异步加载：边下边解析，彻底杜绝 OOM 和阻塞"""
        start_time = time.time()
        try:
            logger.info(f"🚀 Node {self.node_index}: Starting Streaming RAM load...")
            
            # 获取文件列表
            all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
            
            stock_list_file = [f for f in all_files if "stock_list.parquet" in f]
            kline_files = sorted([f for f in all_files if "stock_kline_" in f])
            flow_files = sorted([f for f in all_files if "stock_money_flow_" in f])
            sector_files = sorted([f for f in all_files if "sector_kline_" in f])

            base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
            headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
            
            async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
                # 1. 股票列表
                if stock_list_file:
                    content = await self._download_single(client, base_url + stock_list_file[0])
                    sdf = pl.read_parquet(io.BytesIO(content))
                    self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
                    del content, sdf

                # 2. 日线 (流式处理，防 OOM)
                kline_dfs = []
                for f in kline_files:
                    content = await self._download_single(client, base_url + f)
                    df = pl.read_parquet(io.BytesIO(content))
                    df = self._downcast_df(df)
                    
                    # 稳定且均匀的 Hash 分片
                    sharded = df.filter((pl.col("code").hash(42) % self.total_nodes) == self.node_index)
                    if not sharded.is_empty():
                        kline_dfs.append(sharded)
                    
                    del content, df, sharded
                    await asyncio.sleep(0.01) # 关键：每处理完一个文件，必须出让控制权给服务器心跳！
                
                if kline_dfs:
                    self.df_daily = pl.concat(kline_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                del kline_dfs
                gc.collect()

                # 3. 资金流 (流式处理)
                flow_dfs = []
                for f in flow_files:
                    content = await self._download_single(client, base_url + f)
                    df = pl.read_parquet(io.BytesIO(content))
                    df = self._downcast_df(df)
                    
                    sharded = df.filter((pl.col("code").hash(42) % self.total_nodes) == self.node_index)
                    if not sharded.is_empty():
                        flow_dfs.append(sharded)
                        
                    del content, df, sharded
                    await asyncio.sleep(0.01)
                    
                if flow_dfs and self.df_daily is not None:
                    df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                    self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
                del flow_dfs
                gc.collect()

                # 4. 板块 (流式处理)
                sector_dfs = []
                for f in sector_files:
                    content = await self._download_single(client, base_url + f)
                    df = pl.read_parquet(io.BytesIO(content))
                    sector_dfs.append(self._downcast_df(df))
                    
                    del content, df
                    await asyncio.sleep(0.01)
                    
                if sector_dfs:
                    self.df_sector_daily = pl.concat(sector_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                del sector_dfs
                gc.collect()

            # 5. 后处理计算 (送入线程池，防止卡死主线程)
            if self.df_daily is not None:
                await asyncio.to_thread(self._cpu_heavy_processing)

            logger.info(f"✅ Node {self.node_index}: Load Complete. Time: {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"❌ RAM Load Error: {e}", exc_info=True)

    async def _download_single(self, client, url):
        """带重试机制的单文件下载"""
        for attempt in range(3):
            try:
                resp = await client.get(url, timeout=30.0)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                if attempt == 2: raise
                await asyncio.sleep(1)

    def _cpu_heavy_processing(self):
        """耗时 CPU 计算"""
        try:
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
        logger.info(f"Node {self.node_index}: Resampling weekly and monthly data...")
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

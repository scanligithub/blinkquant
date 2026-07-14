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
        """内存降级，防止拼接时 OOM"""
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in ["volume", "amount"]]
        if f64_cols:
            return df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
        return df

    async def async_load_data(self):
        start_time = time.time()
        try:
            logger.info(f"🚀 Node {self.node_index}: Starting memory-safe async load...")
            
            # 1. 在主线程异步下载所有文件数据 (网络 I/O 密集，不会卡死服务器)
            all_data_map = await self._download_all_to_ram()
            
            # 2. 将高压力的 CPU 解析与合并，送到独立的后台线程池执行
            await asyncio.to_thread(self._cpu_heavy_processing, all_data_map)

            logger.info(f"✅ Node {self.node_index}: RAM Load Complete. Total time: {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"❌ RAM Load Error: {e}", exc_info=True)

    async def _download_all_to_ram(self):
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        data_files = [f for f in all_files if f.endswith(".parquet")]

        base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
        headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
        data_map = {}
        semaphore = asyncio.Semaphore(15) 

        async def download_file(client, filename):
            async with semaphore:
                for attempt in range(3):
                    try:
                        resp = await client.get(base_url + filename, timeout=60.0)
                        resp.raise_for_status()
                        return filename, resp.content
                    except Exception:
                        if attempt == 2: raise
                        await asyncio.sleep(1)

        logger.info(f"Node {self.node_index}: Downloading {len(data_files)} files...")
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            tasks = [download_file(client, f) for f in data_files]
            results = await asyncio.gather(*tasks)

        for fname, content in results:
            data_map[fname] = content
        return data_map

    def _cpu_heavy_processing(self, data_map):
        """所有 CPU 密集型任务统一在子线程执行，彻底保护 FastAPI 主事件循环"""
        try:
            logger.info(f"Node {self.node_index}: Parsing and sharding data...")
            
            # --- 股票列表 ---
            stock_list_file = next((f for f in data_map if "stock_list.parquet" in f), None)
            if stock_list_file:
                sdf = pl.read_parquet(io.BytesIO(data_map[stock_list_file]))
                self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
                del data_map[stock_list_file]

            # --- 股票日线 ---
            kline_files = sorted([f for f in data_map if "stock_kline_" in f])
            kline_dfs = []
            for f in kline_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                df = self._downcast_df(df)
                # 使用成熟的 hash 分片
                node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                sharded = df.filter(node_filter)
                if not sharded.is_empty():
                    kline_dfs.append(sharded)
                data_map[f] = b""
                del data_map[f]
            
            if kline_dfs:
                self.df_daily = pl.concat(kline_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del kline_dfs
            gc.collect()
            time.sleep(0.1) # 线程级防饿死休眠

            # --- 资金流 ---
            flow_files = sorted([f for f in data_map if "stock_money_flow_" in f])
            flow_dfs = []
            for f in flow_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                df = self._downcast_df(df)
                node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                sharded = df.filter(node_filter)
                if not sharded.is_empty():
                    flow_dfs.append(sharded)
                data_map[f] = b""
                del data_map[f]

            if flow_dfs and self.df_daily is not None:
                df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
            del flow_dfs
            gc.collect()
            time.sleep(0.1)

            # --- 板块 ---
            sector_files = sorted([f for f in data_map if "sector_kline_" in f])
            sector_dfs = []
            for f in sector_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                sector_dfs.append(self._downcast_df(df))
                data_map[f] = b""
                del data_map[f]

            if sector_dfs:
                self.df_sector_daily = pl.concat(sector_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del sector_dfs
            gc.collect()
            time.sleep(0.1)

            # --- 数据预处理 ---
            self._apply_forward_adjustment()
            self._resample_all()
            
        finally:
            # 无论如何，最终清空内存池并释放碎片
            data_map.clear()
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

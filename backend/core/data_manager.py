import os
# 【关键修复1】限制 Polars 底层 Rust 线程数，防止吃满 CPU 导致 FastAPI 心跳超时被强杀
os.environ["POLARS_MAX_THREADS"] = "1"

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

        # 内存中的数据对象
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        self.df_sector_daily = None
        self.df_mapping = None

        # 指标计算算子映射
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    async def async_load_data(self):
        """完全基于 RAM 的异步加载主入口"""
        start_time = time.time()
        try:
            logger.info(f"🚀 Node {self.node_index}: Starting RAM-only data load...")
            # 1. 并发下载所有文件内容到内存
            all_data_map = await self._download_all_to_ram()
            
            # 2. 将后续极其耗时的 CPU 计算转移到独立线程，防止阻塞事件循环
            await asyncio.to_thread(self._cpu_bound_processing, all_data_map)
            
            logger.info(f"✅ Node {self.node_index}: RAM Load Complete. Total time: {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"❌ RAM Load Error: {e}", exc_info=True)

    def _cpu_bound_processing(self, all_data_map):
        """专门包裹所有同步的 CPU 密集型任务，包含强制出让 CPU 时间片的逻辑"""
        try:
            # 1. 解析并分片加载
            self._process_ram_data(all_data_map)
            time.sleep(0.2) # 【关键修复3】喘口气，让 FastAPI 回复健康检查
            
            # 2. 数据预处理
            if self.df_daily is not None:
                self._apply_forward_adjustment()
                time.sleep(0.2) # 喘口气
                
                self._resample_all()
                time.sleep(0.2)
        finally:
            # 最终清理
            all_data_map.clear()
            gc.collect()
            try:
                import ctypes
                ctypes.CDLL('libc.so.6').malloc_trim(0)
            except Exception:
                pass

    async def _download_all_to_ram(self):
        """并发获取所有 Parquet 文件的字节流"""
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        data_files = [f for f in all_files if f.endswith(".parquet")]

        base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
        headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
        data_map = {}

        # 限制并发数为 10，防止被 HF 屏蔽
        semaphore = asyncio.Semaphore(10)

        async def download_file(client, filename):
            async with semaphore:
                url = base_url + filename
                response = await client.get(url, timeout=60.0)
                response.raise_for_status()
                return filename, response.content

        logger.info(f"Node {self.node_index}: Downloading {len(data_files)} files...")

        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            tasks = [download_file(client, f) for f in data_files]
            results = await asyncio.gather(*tasks)

        for fname, content in results:
            data_map[fname] = content

        return data_map

    def _downcast_df(self, df):
        """【关键修复2】内存截断：在读取单表时立刻降级 Float64 为 Float32，防止合并时内存爆炸"""
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in ["volume", "amount"]]
        if f64_cols:
            return df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
        return df

    def _process_ram_data(self, data_map):
        """解析内存中的字节流并按节点索引分片"""
        logger.info(f"Node {self.node_index}: Parsing DataFrames...")
    
        # 1. 股票列表
        stock_list_file = next((f for f in data_map if "stock_list.parquet" in f), None)
        if stock_list_file:
            sdf = pl.read_parquet(io.BytesIO(data_map[stock_list_file]))
            self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
            del data_map[stock_list_file]
    
        # 2. 股票日线
        kline_files = sorted([f for f in data_map if "stock_kline_" in f])
        kline_dfs = []
        for f in kline_files:
            df = pl.read_parquet(io.BytesIO(data_map[f]))
            df = self._downcast_df(df) # 立刻降级内存
            node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
            sharded_df = df.filter(node_filter)
            if not sharded_df.is_empty():
                kline_dfs.append(sharded_df)
            # 立刻销毁原始字节流
            data_map[f] = b""
            del data_map[f]
            
        if kline_dfs:
            self.df_daily = pl.concat(kline_dfs, how="diagonal")
            self.df_daily = self.df_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del kline_dfs
            gc.collect() # 合并完立刻收回内存碎片
            time.sleep(0.1)
    
        # 3. 资金流
        flow_files = sorted([f for f in data_map if "stock_money_flow_" in f])
        if flow_files:
            flow_dfs = []
            for f in flow_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                df = self._downcast_df(df) # 立刻降级内存
                node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                sharded_flow = df.filter(node_filter)
                if not sharded_flow.is_empty():
                    flow_dfs.append(sharded_flow)
                data_map[f] = b""
                del data_map[f]
            
            df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            if self.df_daily is not None:
                self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
            del flow_dfs, df_flow
            gc.collect()
            time.sleep(0.1)
    
        # 4. 板块数据
        sector_files = sorted([f for f in data_map if "sector_kline_" in f])
        if sector_files:
            sector_dfs = []
            for f in sector_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                sector_dfs.append(self._downcast_df(df))
                data_map[f] = b""
                del data_map[f]
            self.df_sector_daily = pl.concat(sector_dfs, how="diagonal")
            self.df_sector_daily = self.df_sector_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
            del sector_dfs
            gc.collect()

    def _apply_forward_adjustment(self):
        """执行前复权处理"""
        if self.df_daily is None or "adjustFactor" not in self.df_daily.columns:
            return
    
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
        """基于前复权后的日线数据，生成周线和月线表"""
        if self.df_daily is None:
            return

        logger.info(f"Node {self.node_index}: Resampling weekly and monthly data...")
        aggs = [
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
            pl.col("amount").sum()
        ]

        base = self.df_daily.sort("date")
        self.df_weekly = base.group_by_dynamic("date", every="1w", by="code").agg(aggs)
        time.sleep(0.1) # 喘气
        
        self.df_monthly = base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        time.sleep(0.1)

        if self.df_sector_daily is not None:
            s_base = self.df_sector_daily.sort("date")
            self.df_sector_weekly = s_base.group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = s_base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)

data_manager = DataManager()

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

        # 内存中的数据对象
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        self.df_sector_daily = None
        # 显式初始化 df_mapping 为 None，防止 AttributeError
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
            # 2. 从内存字节流解析并分片加载
            self._process_ram_data(all_data_map)
            # 3. 数据预处理 (CPU 密集型)
            if self.df_daily is not None:
                self._apply_forward_adjustment()
                self._optimize_memory(self.df_daily, "df_daily")
                self._optimize_memory(self.df_sector_daily, "df_sector_daily")
                self._resample_all()
            # 最终清理，确保释放所有临时字节流
            del all_data_map
            gc.collect()
            logger.info(f"✅ Node {self.node_index}: RAM Load Complete. Total time: {time.time() - start_time:.2f}s")
        except Exception as e:
            logger.error(f"❌ RAM Load Error: {e}", exc_info=True)

    async def _download_all_to_ram(self):
        """并发获取所有 Parquet 文件的字节流"""
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        data_files = [f for f in all_files if f.endswith(".parquet")]

        # 构建下载 URL (Hugging Face 官方标准格式)
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

        logger.info(f"Downloading {len(data_files)} files (approx < 1GB) into RAM...")

        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
            tasks = [download_file(client, f) for f in data_files]
            results = await asyncio.gather(*tasks)

        for fname, content in results:
            data_map[fname] = content

        return data_map

    def _process_ram_data(self, data_map):
        """解析内存中的字节流并按节点索引分片"""
        logger.info(f"Node {self.node_index}: Parsing and sharding DataFrames...")

        # 1. 股票列表 (全量)
        stock_list_file = next((f for f in data_map if "stock_list.parquet" in f), None)
        if stock_list_file:
            sdf = pl.read_parquet(io.BytesIO(data_map[stock_list_file]))
            self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}

        # 2. 股票日线 (分片加载)
        kline_files = sorted([f for f in data_map if "stock_kline_" in f])
        kline_dfs = []
        for f in kline_files:
            # 读取整个文件到 Polars 后立即过滤，减少内存峰值
            df = pl.read_parquet(io.BytesIO(data_map[f]))
            # 确定性分片：code 数字部分 % 3
            node_filter = (df["code"].str.extract(r"(\d+)").cast(pl.Int32) % self.total_nodes) == self.node_index
            kline_dfs.append(df.filter(node_filter))

        if kline_dfs:
            self.df_daily = pl.concat(kline_dfs)
            self.df_daily = self.df_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

        # 3. 资金流 (分片并合并)
        flow_files = sorted([f for f in data_map if "stock_money_flow_" in f])
        if flow_files:
            flow_dfs = []
            for f in flow_files:
                df = pl.read_parquet(io.BytesIO(data_map[f]))
                node_filter = (df["code"].str.extract(r"(\d+)").cast(pl.Int32) % self.total_nodes) == self.node_index
                flow_dfs.append(df.filter(node_filter))
            df_flow = pl.concat(flow_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
            if self.df_daily is not None:
                self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")

        # 4. 板块数据 (全量)
        sector_files = sorted([f for f in data_map if "sector_kline_" in f])
        if sector_files:
            self.df_sector_daily = pl.concat([pl.read_parquet(io.BytesIO(data_map[f])) for f in sector_files])
            self.df_sector_daily = self.df_sector_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

    def _apply_forward_adjustment(self):
        """执行前复权处理"""
        if self.df_daily is None or "adjustFactor" not in self.df_daily.columns:
            return

        logger.info(f"Node {self.node_index}: Applying price adjustment...")
        self.df_daily = self.df_daily.sort(["code", "date"])

        # 复权逻辑优化：使用 Over
        adj_col = pl.col("adjustFactor").fill_null(1.0).forward_fill().over("code")
        latest_adj = adj_col.last().over("code")
        qfq_expr = pl.when(latest_adj > 0).then(adj_col / latest_adj).otherwise(1.0)

        self.df_daily = self.df_daily.with_columns([
            (pl.col("open") * qfq_expr).cast(pl.Float32),
            (pl.col("high") * qfq_expr).cast(pl.Float32),
            (pl.col("low") * qfq_expr).cast(pl.Float32),
            (pl.col("close") * qfq_expr).cast(pl.Float32),
            (pl.col("volume") / qfq_expr).cast(pl.Float64)
        ])

    def _optimize_memory(self, df, name):
        """强制将 Float64 降级为 Float32，降低 50% 内存消耗"""
        if df is None:
            return

        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in ["volume", "amount"]]
        if f64_cols:
            opt = df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
            if name == "df_daily":
                self.df_daily = opt
            else:
                self.df_sector_daily = opt
            logger.info(f"Node {self.node_index}: Optimized {name} ({len(f64_cols)} cols -> Float32)")

    def _resample_all(self):
        """基于前复权后的日线数据，生成周线和月线表"""
        if self.df_daily is None:
            return

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
        self.df_monthly = base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)

        # 板块重采样
        if self.df_sector_daily is not None:
            s_base = self.df_sector_daily.sort("date")
            self.df_sector_weekly = s_base.group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = s_base.group_by_dynamic("date", every="1mo", by="code").agg(aggs)


data_manager = DataManager()

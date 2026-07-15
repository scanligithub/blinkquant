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
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.repo_id = "scanli/stocka-data"

        # 健壮解析 NODE_INDEX，防范 HF Space UI 配置中的空格或非数字字符
        node_idx_env = os.getenv("NODE_INDEX", "0").strip()
        try:
            digits = "".join(filter(str.isdigit, node_idx_env))
            self.node_index = int(digits) if digits else 0
        except Exception:
            self.node_index = 0
            
        if self.node_index >= self.total_nodes or self.node_index < 0:
            logger.warning(f"Invalid NODE_INDEX {self.node_index} (out of bounds), resetting to 0")
            self.node_index = 0

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
        """流式、低内存占用的异步加载主入口（串行下载和解析，规避并发 OOM 与连接死锁）"""
        start_time = time.time()
        try:
            logger.info(f"🚀 Node {self.node_index}: Starting streamlined memory-safe data load...")
            
            # 1. 获取文件列表 (使用线程执行同步网络请求，防止阻塞事件循环)
            all_files = await asyncio.to_thread(
                list_repo_files, repo_id=self.repo_id, repo_type="dataset", token=self.hf_token
            )
            data_files = sorted([f for f in all_files if f.endswith(".parquet")])
            
            base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main/"
            headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}
            
            kline_dfs = []
            flow_dfs = []
            sector_dfs = []
            
            # 2. 串行流式下载和解析，严格控制单次内存开销
            # 引入指数退避重试，防止由于 CDN 闪断导致节点初始化失败
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=60.0) as client:
                for fname in data_files:
                    logger.info(f"Node {self.node_index}: Loading {fname}...")
                    url = base_url + fname
                    
                    content = None
                    # 指数退避重试 3 次
                    for attempt in range(1, 4):
                        try:
                            response = await client.get(url)
                            response.raise_for_status()
                            content = response.content
                            break # 下载成功，跳出重试
                        except Exception as download_err:
                            if attempt == 3:
                                logger.error(f"Node {self.node_index}: Failed to download {fname} after 3 attempts: {download_err}")
                            else:
                                wait_time = attempt * 3 # 分别等待 3s, 6s 重试
                                logger.warning(f"Node {self.node_index}: Temp download error for {fname} ({download_err}). Retrying in {wait_time}s...")
                                await asyncio.sleep(wait_time)
                    
                    # 如果重试 3 次后该文件依然下载失败，为保证数据完整性，不应强行启动（否则可能导致空数据错乱）
                    if content is None:
                        raise ValueError(f"Core data file {fname} failed to load. Aborting initialization to force safer redeploy.")
                    
                    bio = io.BytesIO(content)
                    
                    # 根据文件名类型分类解析，并在处理完成后立即 del 释放内存
                    if "stock_list.parquet" in fname:
                        sdf = pl.read_parquet(bio)
                        self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
                        del sdf
                    
                    elif "stock_kline_" in fname:
                        df = pl.read_parquet(bio)
                        node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                        sharded_df = df.filter(node_filter)
                        if not sharded_df.is_empty():
                            kline_dfs.append(sharded_df)
                        del df
                        
                    elif "stock_money_flow_" in fname:
                        df = pl.read_parquet(bio)
                        node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                        sharded_flow = df.filter(node_filter)
                        if not sharded_flow.is_empty():
                            flow_dfs.append(sharded_flow)
                        del df
                        
                    elif "sector_kline_" in fname:
                        sdf = pl.read_parquet(bio)
                        sector_dfs.append(sdf)
                    
                    # 强力垃圾回收，防止字节流在堆中残留
                    del content
                    del bio
                    gc.collect()

            logger.info(f"Node {self.node_index}: All files downloaded. Integrating DataFrames...")

            # 3. 合并并解析日线数据
            if kline_dfs:
                self.df_daily = pl.concat(kline_dfs, how="diagonal")
                self.df_daily = self.df_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                del kline_dfs
                gc.collect()
                
            # 4. 合并资金流并与日线关联
            if flow_dfs:
                df_flow = pl.concat(flow_dfs, how="diagonal").with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                if self.df_daily is not None:
                    self.df_daily = self.df_daily.join(df_flow, on=["date", "code"], how="left")
                del df_flow
                del flow_dfs
                gc.collect()

            # 5. 合并板块数据
            if sector_dfs:
                self.df_sector_daily = pl.concat(sector_dfs, how="diagonal")
                self.df_sector_daily = self.df_sector_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))
                del sector_dfs
                gc.collect()

            # 6. 数据前复权与重采样
            if self.df_daily is not None:
                self._apply_forward_adjustment()
                self._optimize_memory(self.df_daily, "df_daily")
                self._optimize_memory(self.df_sector_daily, "df_sector_daily")
                self._resample_all()
                
            gc.collect()
            
            # 7. 强制 Linux 归还幽灵内存
            try:
                import ctypes
                ctypes.CDLL('libc.so.6').malloc_trim(0)
                logger.info(f"Node {self.node_index}: Forced libc malloc_trim successfully.")
            except Exception as e:
                logger.warning(f"Node {self.node_index}: malloc_trim failed: {e}")
                
            logger.info(f"✅ Node {self.node_index}: RAM Load Complete. Total time: {time.time() - start_time:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ RAM Load Error: {e}", exc_info=True)

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

    def _optimize_memory(self, df, name):
        """将 Float64 降级为 Float32，降低 50% 内存消耗"""
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

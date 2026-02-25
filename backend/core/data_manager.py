import os
import gc
import polars as pl
import psycopg2
import logging
from .data_types import AShareDataSchema
from huggingface_hub import hf_hub_download, list_repo_files

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self):
        self.node_index = int(os.getenv("NODE_INDEX", "0"))
        self.total_nodes = 3
        self.hf_token = os.getenv("HF_TOKEN")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.repo_id = "scanli/stocka-data"

        # 行情数据表
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        
        # 板块数据表
        self.df_sector_daily = None
        self.df_sector_weekly = None
        self.df_sector_monthly = None
        
        # 关系映射表
        self.df_mapping = None
        
        # 指标计算算子映射
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def load_data(self):
        """启动分布式数据加载与预处理流程"""
        try:
            # 1. 原始数据加载与分片
            self._load_raw_parquet()
            
            # 2. 执行前复权 (必须在重采样和内存优化之前)
            self._apply_forward_adjustment()
            
            # 3. 内存类型优化 (Float64 -> Float32)
            self._optimize_memory(self.df_daily, "df_daily")
            self._optimize_memory(self.df_sector_daily, "df_sector_daily")
            
            # 4. 时间周期重采样 (W/M)
            self._resample_all()
            
            # 5. 指标自进化挂载 (从 DB 加载热点指标)
            self._evolve_from_db()
            
            # 6. 最终内存回收
            gc.collect()
            logger.info(f"Node {self.node_index}: Boot sequence complete.")
            
        except Exception as e:
            logger.error(f"Critical Load Error: {e}", exc_info=True)

    def _load_raw_parquet(self):
        logger.info(f"Node {self.node_index}: Starting _load_raw_parquet...")
        all_files = list_repo_files(repo_id=self.repo_id, repo_type="dataset", token=self.hf_token)
        
        # --- 1. 股票日线加载 ---
        stock_files = sorted([f for f in all_files if "stock_kline_" in f])
        lazy_frames = []
        
        for f in stock_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            # 构建惰性分片逻辑
            lf = pl.scan_parquet(path).filter((pl.col("code").hash() % self.total_nodes) == self.node_index)
            lazy_frames.append(lf)
            
        if lazy_frames:
            logger.info(f"Node {self.node_index}: Streaming {len(lazy_frames)} stock partitions...")
            try:
                self.df_daily = pl.concat(lazy_frames).collect(streaming=True)
                self.df_daily = self.df_daily.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
                logger.info(f"Node {self.node_index}: df_daily collected. Shape: {self.df_daily.shape}")

                if "name" in self.df_daily.columns:
                    unique_stocks = self.df_daily.select(["code", "name"]).unique()
                    self.code_to_name = {row["code"]: row["name"] for row in unique_stocks.iter_rows()}
                    logger.info(f"Node {self.node_index}: Populated code_to_name with {len(self.code_to_name)} entries.")
                else:
                    logger.warning(f"Node {self.node_index}: 'name' column not found in df_daily, cannot populate code_to_name.")
            except Exception as e:
                logger.error(f"Node {self.node_index}: Error collecting df_daily or populating code_to_name: {e}", exc_info=True)
        else:
            logger.warning(f"Node {self.node_index}: No lazy_frames for stock data found.")

        # --- 2. 板块行情加载 ---
        sector_files = sorted([f for f in all_files if "sector_kline_" in f])
        s_dfs = []
        for f in sector_files:
            path = hf_hub_download(repo_id=self.repo_id, filename=f, repo_type="dataset", token=self.hf_token, cache_dir="./data_cache")
            s_dfs.append(pl.read_parquet(path))
            
        if s_dfs:
            self.df_sector_daily = pl.concat(s_dfs).with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

        # --- 3. 股票-板块映射表加载 ---
        map_file = [f for f in all_files if "sector_constituents" in f]
        if map_file:
            path = hf_hub_download(repo_id=self.repo_id, filename=map_file[-1], repo_type="dataset", token=self.hf_token)
            self.df_mapping = pl.read_parquet(path).select([
                pl.col("stock_code").alias("code"), 
                pl.col("sector_code")
            ])

    def _apply_forward_adjustment(self):
        """执行前复权处理：OHLC * (当日复权因子 / 最新复权因子)"""
        if self.df_daily is None: return
        
        if "adjustFactor" not in self.df_daily.columns:
            logger.warning(f"Node {self.node_index}: adjustFactor missing, skipping forward adjustment.")
            return

        logger.info(f"Node {self.node_index}: Applying forward adjustment logic...")
        
        # 计算每只股票最新的复权因子
        # 使用 over("code") 窗口函数定位每只股票时间轴上的最后一个因子
        qfq_expr = pl.col("adjustFactor") / pl.col("adjustFactor").last().over("code")
        
        self.df_daily = self.df_daily.with_columns([
            (pl.col("open") * qfq_expr).alias("open"),
            (pl.col("high") * qfq_expr).alias("high"),
            (pl.col("low") * qfq_expr).alias("low"),
            (pl.col("close") * qfq_expr).alias("close"),
        ])
        
        # 计算完复权后，adjustFactor 任务完成，可以选择保留或后续转换
        gc.collect()

    def _optimize_memory(self, df: pl.DataFrame, name: str):
        """强制将 Float64 降级为 Float32，降低 50% 内存消耗"""
        if df is None: return
        
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64]
        if f64_cols:
            optimized_df = df.with_columns([pl.col(c).cast(pl.Float32) for c in f64_cols])
            
            # 写回实例属性
            if name == "df_daily": self.df_daily = optimized_df
            elif name == "df_sector_daily": self.df_sector_daily = optimized_df
            
            logger.info(f"Node {self.node_index}: Optimized {name} ({len(f64_cols)} cols -> Float32)")

    def _resample_all(self):
        """基于前复权后的日线数据，生成周线和月线表"""
        if self.df_daily is None: return
        
        # 定义 OHLCV 聚合规则
        aggs = [
            pl.col("open").first(), 
            pl.col("high").max(), 
            pl.col("low").min(), 
            pl.col("close").last(), 
            pl.col("volume").sum(), 
            pl.col("amount").sum()
        ]
        
        logger.info(f"Node {self.node_index}: Resampling W/M timeframes...")
        
        # 个股重采样
        base_df = self.df_daily.sort("date")
        self.df_weekly = base_df.group_by_dynamic("date", every="1w", by="code").agg(aggs)
        self.df_monthly = base_df.group_by_dynamic("date", every="1mo", by="code").agg(aggs)
        
        # 板块重采样
        if self.df_sector_daily is not None:
            s_base_df = self.df_sector_daily.sort("date")
            self.df_sector_weekly = s_base_df.group_by_dynamic("date", every="1w", by="code").agg(aggs)
            self.df_sector_monthly = s_base_df.group_by_dynamic("date", every="1mo", by="code").agg(aggs)

    def _evolve_from_db(self):
        """自进化：从数据库加载高频指标并执行分批挂载，避免内存尖峰"""
        if not self.postgres_url: return
        
        try:
            conn = psycopg2.connect(self.postgres_url)
            cur = conn.cursor()
            # 获取最热的 150 个指标
            cur.execute("""
                SELECT metric_key FROM metrics_stats 
                WHERE metric_key NOT LIKE '%_W' AND metric_key NOT LIKE '%_M'
                ORDER BY usage_count DESC LIMIT 251
            """)
            top_keys = [row[0] for row in cur.fetchall()]
            cur.close(); conn.close()

            if not top_keys: return

            # 目标数据集清单
            target_configs = [
                ('df_daily', self.df_daily), 
                ('df_weekly', self.df_weekly), 
                ('df_monthly', self.df_monthly)
            ]
            
            BATCH_SIZE = 25 # 每批计算 25 个指标，降低计算时的临时内存压力
            
            for attr_name, df in target_configs:
                if df is None: continue
                
                total_added = 0
                for i in range(0, len(top_keys), BATCH_SIZE):
                    batch = top_keys[i : i + BATCH_SIZE]
                    exprs = []
                    
                    for key in batch:
                        parts = key.split('_')
                        if len(parts) == 3:
                            func, field, param = parts[0], parts[1].lower(), int(parts[2])
                            # 确保基础列存在且指标尚未计算
                            if field in df.columns and key not in df.columns:
                                try:
                                    exprs.append(self.INDICATOR_MAP[func](pl.col(field), param).alias(key))
                                except: pass
                    
                    if exprs:
                        df = df.with_columns(exprs)
                        total_added += len(exprs)
                        gc.collect() # 批次间显式回收

                setattr(self, attr_name, df)
                logger.info(f"Node {self.node_index}: Evolution complete for {attr_name}, added {total_added} cols.")

        except Exception as e:
            logger.error(f"Evolution failed: {e}")

data_manager = DataManager()

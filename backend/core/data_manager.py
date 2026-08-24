import os
import gc
import time
import asyncio
import io
import datetime
import logging
import httpx
import polars as pl
from huggingface_hub import list_repo_files
from .indicator_registry import INDICATOR_FUNCS

logger = logging.getLogger(__name__)

# 主板 ST 涨跌幅与普通股并轨日：此前主板 ST 为 5%，此后统一 10%（见 security.py 板别注释）
MAIN_BOARD_ST_UNIFY_DATE = datetime.date(2026, 7, 6)
# 创业板注册制改革日：此前创业板限幅为 10%（数据集自 2005 年起，该规则实际生效）
CHI_NEXT_REFORM_DATE = datetime.date(2020, 8, 24)
# 无涨跌幅限制日判定余量：正常封板日涨幅不超过限幅+修约误差(<1pp)，超出即视为无限制日（新股期等）
NO_LIMIT_BAND_MARGIN_PCT = 2.0
# 官方基准昨收与相邻收盘的背离阈值：超过视为除权除息日采用反推基准；以内视为存储噪声沿用昨收
EX_DIV_DETECT_THRESHOLD = 0.011


def limit_pct_expr(date_col: pl.Expr, is_st_col: pl.Expr) -> pl.Expr:
    """按 code 前缀 + 历史规则计算每行涨跌停幅度（百分比数值，10 = 10%）。

    科创(688/689) → 20，北交所(bj.*) → 30；
    创业板(sz.30)：2020-08-24 注册制改革前 → 10，此后 → 20；
    其余(沪深主板) → 10；2026-07-06 前的主板 ST 股为 5%。
    仅在确认含 date/isST 列的日线表上使用。
    """
    base = pl.when(
        pl.col("code").str.starts_with("sh.688")
        | pl.col("code").str.starts_with("sh.689")
    ).then(pl.lit(20.0)).when(
        pl.col("code").str.starts_with("bj.")
    ).then(pl.lit(30.0)).when(
        pl.col("code").str.starts_with("sz.30")
        & (date_col < pl.lit(CHI_NEXT_REFORM_DATE))
    ).then(pl.lit(10.0)).when(
        pl.col("code").str.starts_with("sz.30")
    ).then(pl.lit(20.0)).otherwise(pl.lit(10.0))
    st_main_5 = (
        (base == pl.lit(10.0))
        & (is_st_col.cast(pl.Int8, strict=False) == 1)
        & (date_col < pl.lit(MAIN_BOARD_ST_UNIFY_DATE))
    ).fill_null(False)
    return pl.when(st_main_5).then(pl.lit(5.0)).otherwise(base)


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

        # ---- 核心修改：读取本次容器唯一的 build_id.txt (用于部署版本校验) ----
        self.build_id = "unknown"
        try:
            if os.path.exists("build_id.txt"):
                with open("build_id.txt", "r") as f:
                    self.build_id = f.read().strip()
        except Exception:
            pass

        # 内存中的数据对象
        self.df_daily = None
        self.df_weekly = None
        self.df_monthly = None
        self.code_to_name = {}
        self.df_sector_daily = None
        self.df_mapping = None
        self.df_sector_list = None
        self.stock_sectors = {}

        # 指标计算算子映射（由注册表派生）
        self.INDICATOR_MAP = dict(INDICATOR_FUNCS)

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
            sector_constituents_dfs = []
            
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
                        # 规范化股票代码为带市场前缀标准格式（纯数字补前缀，已带前缀原样保留）
                        sdf = sdf.with_columns(self._normalize_code_expr(pl.col("code")))
                        self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
                        del sdf
                    
                    elif "sector_list.parquet" in fname:
                        # ★ 新增：板块元数据主表（code, name, type）
                        self.df_sector_list = pl.read_parquet(bio)
                    
                    elif "sector_constituents_" in fname:
                        # ★ 新增：自愈板块成分股关系映射表（sector_code, stock_code, sector_name, date）
                        sector_constituents_dfs.append(pl.read_parquet(bio))
                    
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

            # 5.1 ★ 新增：构建 1-to-1 板块映射（行业优先，兜底概念板块）
            if sector_constituents_dfs:
                self._build_sector_mapping(sector_constituents_dfs)
                del sector_constituents_dfs
                gc.collect()

            # 6. 涨停标志（须在前复权前、用未复权价计算）与前复权重采样
            if self.df_daily is not None:
                self._compute_limit_flags()
                self._apply_forward_adjustment()
                self._append_prev_close()
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

    @staticmethod
    def _normalize_code_expr(code_col):
        """
        返回将纯数字股票代码补全为带市场前缀标准码的 Polars 表达式：
        - 6 开头 → sh.
        - 0 / 3 开头 → sz.
        - 4 / 8 / 9 开头 → bj.
        已带前缀（sh./sz./bj.）的代码原样返回。
        使用纯向量化 when/then，避免 map_elements 的版本兼容问题。
        """
        return (
            pl.when(code_col.str.starts_with("sh.") | code_col.str.starts_with("sz.") | code_col.str.starts_with("bj."))
              .then(code_col)
              .when(code_col.str.starts_with("6"))
              .then(pl.lit("sh.") + code_col)
              .when(code_col.str.starts_with("0") | code_col.str.starts_with("3"))
              .then(pl.lit("sz.") + code_col)
              .when(code_col.str.starts_with("4") | code_col.str.starts_with("8") | code_col.str.starts_with("9"))
              .then(pl.lit("bj.") + code_col)
              .otherwise(code_col)
        )

    def _build_sector_mapping(self, sector_constituents_dfs):
        """
        ★ 新增：构建 1-to-1 板块映射 df_mapping
        规则：
        1. 将 sector_constituents 与 sector_list 按 sector_code Inner Join，获取板块 type
        2. 行业优先：仅保留 type == '行业板块' 的映射
        3. 兜底逻辑：无行业板块的股票，取第一个概念板块充当映射
        4. 去重保证：unique(subset=["code"], keep="first") 强制每个股票唯一
        """
        try:
            constituents = pl.concat(sector_constituents_dfs, how="diagonal")
            
            if constituents.is_empty():
                logger.warning(f"Node {self.node_index}: sector_constituents is empty, df_mapping disabled")
                return
                
            # 字段检查与代码规范化
            if "sector_code" not in constituents.columns or "stock_code" not in constituents.columns:
                logger.warning(f"Node {self.node_index}: sector_constituents missing required columns, df_mapping disabled")
                return
            
            # stock_code 补全前缀（600000 → sh.600000）
            constituents = constituents.with_columns(
                self._normalize_code_expr(pl.col("stock_code")).alias("code")
            )
            
            # 关联 sector_list 获取板块 type（若存在）
            if self.df_sector_list is not None and "code" in self.df_sector_list.columns and "type" in self.df_sector_list.columns:
                mapped = constituents.join(
                    self.df_sector_list.select(["code", "type"]),
                    left_on="sector_code", right_on="code", how="inner"
                )
            else:
                # sector_list 缺失时降级：全部按概念板块处理
                logger.warning(f"Node {self.node_index}: sector_list not loaded, falling back to concept sectors")
                mapped = constituents.with_columns(pl.lit("概念板块").alias("type"))

            # 构建 1-to-N 全量股票→板块映射（行业+概念+地域），供 K 线图板块标签使用
            try:
                all_map = mapped.unique(subset=["code", "sector_code"]).select([
                    pl.col("code"),
                    pl.col("sector_code"),
                    pl.col("sector_name"),
                    pl.col("type"),
                ])
                sectors_by_code: dict = {}
                for row in all_map.iter_rows():
                    sectors_by_code.setdefault(row[0], []).append((row[1], row[2], row[3]))
                self.stock_sectors = sectors_by_code
                logger.info(f"Node {self.node_index}: stock_sectors built: {len(self.stock_sectors)} stocks mapped")
            except Exception as e:
                logger.error(f"Node {self.node_index}: Failed to build stock_sectors: {e}", exc_info=True)
                self.stock_sectors = {}

            # 行业优先：1-to-1 主映射
            industry = mapped.filter(pl.col("type") == "行业板块")
            concept = mapped.filter(pl.col("type") == "概念板块")

            if not industry.is_empty():
                # 兜底：对没有行业板块的个股，用其第一个概念板块补位
                industry_codes = set(industry["code"].to_list())
                concept_fallback = concept.filter(~pl.col("code").is_in(industry_codes))
                if not concept_fallback.is_empty():
                    combined = pl.concat([industry, concept_fallback])
                else:
                    combined = industry
            elif not concept.is_empty():
                # 极端兜底：无任何行业板块时，全部用概念板块
                combined = concept
            else:
                combined = None

            # 强制 1-to-1：每个股票代码只保留一行
            if combined is not None:
                self.df_mapping = combined.unique(subset=["code"], keep="first").select([
                    pl.col("code"),
                    pl.col("sector_code")
                ])
                logger.info(f"Node {self.node_index}: df_mapping built: {len(self.df_mapping)} rows (industry-first 1-to-1)")
            else:
                self.df_mapping = None
                logger.warning(f"Node {self.node_index}: no sector mapping available, df_mapping disabled")
        except Exception as e:
            logger.error(f"Node {self.node_index}: Failed to build sector mapping: {e}", exc_info=True)
            self.df_mapping = None

    def _compute_limit_flags(self):
        """在前复权之前，基于未复权价格计算涨停/跌停标志列（复权不变的布尔值）。

        交易所实际涨停价 = ROUND(官方基准昨收 × (1 ± 板别限幅), 2)（0.01 元修约），
        真实涨停的当日涨幅常落在理论限幅之下（如前收 10.54 → 涨停价 11.59，涨幅仅 9.96%），
        因此不能用 pctChg >= LIMIT_UP_PCT 判断涨停。

        官方基准昨收的确定：
        - 除权除息日交易所以除权参考价为基准，相邻收盘不再适用；供应商 pctChg 恰按该官方
          基准计算（已实测验证），故用 close/(1+pctChg/100) 反推；
        - 仅当反推值与相邻收盘背离超过阈值（真实除权）时采用，避免浮点/存储噪声扰动修约；
        - pctChg 缺失或异常时回退相邻收盘。

        无涨跌幅限制日净化：当日涨幅超出限幅带+余量必然是无限制日（新股上市初期等），
        此时不存在涨停价概念，标志强制 False；正常封板日涨幅不可能越过该阈值。
        已知残余：无限制日内冲高回落、收盘落回限幅带内的行无法识别。

        首行无昨收 → False（天然排除上市首日）。
        """
        if self.df_daily is None:
            return
        logger.info(f"Node {self.node_index}: Computing limit up/down flags...")
        df = self.df_daily.sort(["code", "date"])

        prev_raw = pl.col("close").shift(1).over("code")
        pct_band = limit_pct_expr(pl.col("date"), pl.col("isST"))
        eps = pl.lit(1e-4)

        # 全空列时 dtype 为 Null，先显式抬升为 Float64 保证表达式可用
        pct_chg = pl.col("pctChg").cast(pl.Float64, strict=False)
        pct_valid = pct_chg.is_not_null() & (pct_chg.abs() < pl.lit(40.0))
        implied_prev = pl.col("close") / (pl.lit(1.0) + pct_chg / pl.lit(100.0))
        eff_prev = pl.when(
            pct_valid & ((implied_prev - prev_raw).abs() > pl.lit(EX_DIV_DETECT_THRESHOLD))
        ).then(implied_prev).otherwise(prev_raw)

        up_price = (eff_prev * (pl.lit(1.0) + pct_band / pl.lit(100.0))).round(2)
        down_price = (eff_prev * (pl.lit(1.0) - pct_band / pl.lit(100.0))).round(2)

        no_limit_day = (
            (pct_chg > (pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT)))
            | (pct_chg < -(pct_band + pl.lit(NO_LIMIT_BAND_MARGIN_PCT)))
        ).fill_null(False)

        self.df_daily = df.with_columns([
            ((pl.col("close") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_limit_up"),
            ((pl.col("high") >= up_price - eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_up"),
            ((pl.col("close") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_limit_down"),
            ((pl.col("low") <= down_price + eps) & ~no_limit_day).fill_null(False).alias("is_touch_limit_down"),
        ])

    def _append_prev_close(self):
        """追加前复权口径的昨收列（与 CLOSE 同语义，供 DSL 直接引用）。"""
        if self.df_daily is None:
            return
        self.df_daily = self.df_daily.with_columns(
            pl.col("close").shift(1).over("code").alias("prev_close")
        )

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

        # 必须保留 Float64 的大整数字段（防止万亿级市值/股本溢出）
        keep_f64 = {
            "volume", "amount",
            "total_shares", "float_shares", "total_mv", "float_mv", "forecast_yoy"
        }
        f64_cols = [c for c, t in df.schema.items() if t == pl.Float64 and c not in keep_f64]
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

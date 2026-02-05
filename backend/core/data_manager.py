import os, polars as pl, psutil, gc, time, psycopg2
from huggingface_hub import hf_hub_download, list_repo_files
from .data_types import AShareDataSchema

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
        self.df_sector_daily = None
        self.df_mapping = None

        # --- 内存置换策略 ---
        self.RSS_LIMIT_GB = 14.5
        self.SAFE_LEVEL_GB = 12.5
        self.IMMORTAL_COLS = {'date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn', 'sector_code', 's_close'}
        self.column_metadata = {}
        
        self.INDICATOR_MAP = {
            'MA': lambda col, p: col.rolling_mean(window_size=p).over("code"),
            'EMA': lambda col, p: col.ewm_mean(span=p, adjust=False).over("code"),
            'STD': lambda col, p: col.rolling_std(window_size=p).over("code"),
            'ROC': lambda col, p: ((col / col.shift(p).over("code")) - 1) * 100
        }

    def update_col_usage(self, col_name: str):
        """轻量级计数，仅在 security.py 命中缓存时调用"""
        if col_name in self.column_metadata:
            self.column_metadata[col_name]["hits"] += 1
            self.column_metadata[col_name]["last_used"] = time.time()

    def _get_rss(self):
        return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3)

    def ensure_capacity(self):
        """智能置换：如果内存紧绷，清理最冷门的指标"""
        current_rss = self._get_rss()
        if current_rss < self.RSS_LIMIT_GB:
            return True

        # 找出可置换列
        dynamic_cols = [c for c in self.df_daily.columns if c not in self.IMMORTAL_COLS]
        if not dynamic_cols: return False

        # 按点击量升序排列 (LFU策略)
        candidates = sorted(dynamic_cols, key=lambda c: self.column_metadata.get(c, {}).get("hits", 0))
        
        # 每次只删除最冷门的 3 个指标，防止大规模内存抖动
        to_drop = candidates[:3]
        if to_drop:
            self.df_daily = self.df_daily.drop(to_drop)
            for c in to_drop: self.column_metadata.pop(c, None)
            gc.collect() # 仅在 Drop 后执行
            print(f"Evicted Cold Columns: {to_drop}. RSS now: {self._get_rss():.2f}GB")
        
        return self._get_rss() < self.RSS_LIMIT_GB

    def mount_jit_column(self, col_name: str, expression: pl.Expr):
        """后台静默挂载"""
        if self.df_daily is None or col_name in self.df_daily.columns: return
        
        # 检查空间
        self.ensure_capacity()

        try:
            # 挂载新指标
            self.df_daily = self.df_daily.with_columns(expression.alias(col_name))
            self.column_metadata[col_name] = {"hits": 1, "last_used": time.time()}
            print(f"Background JIT Success: {col_name}")
        except Exception as e:
            print(f"Background JIT Error [{col_name}]: {e}")

    def load_data(self):
        # ... (数据加载逻辑保持不变)
        print(f"Node {self.node_index}: Loading data...")
        # (加载代码略过...)

        # 4. 执行重采样与初始进化
        self._resample_all()
        self._evolve_indicators()

        # --- 核心修复：防止启动期 NoneType 报错 ---
        if self.df_daily is not None:
            self.column_metadata = {
                c: {"hits": 0, "last_used": time.time()} 
                for c in self.df_daily.columns if c not in self.IMMORTAL_COLS
            }
            print(f"Node {self.node_index}: Boot sequence complete.")
        else:
            print(f"Node {self.node_index}: Data fail to Load!")

    def _evolve_indicators(self):
        # ... (保持原有的从 DB 初始进化的逻辑不变)
        pass

    def _resample_all(self):
        # ... (保持原有重采样逻辑)
        pass

data_manager = DataManager()

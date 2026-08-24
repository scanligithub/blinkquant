import polars as pl
import re
import logging
from .data_manager import data_manager
from .security import blink_parser
from .indicator_registry import WINDOW_NAMES, FIELDS

logger = logging.getLogger(__name__)


class SelectionEngine:
    def __init__(self):
        _funcs = "|".join(WINDOW_NAMES)
        _fields = "|".join(FIELDS)
        self.metric_pattern = re.compile(
            rf'\b({_funcs})\s*\(\s*({_fields})\s*,\s*(\d+)\s*\)',
            re.IGNORECASE)

    def _prepare_hot_jit(self, formula: str):
        """
        同步热挂载：全周期广播
        当发现新指标时，强制在 日/周/月 表中全部计算一遍
        """
        matches = self.metric_pattern.findall(formula)
        if not matches:
            return

        # 定义需要检查的表
        targets = [('df_daily', data_manager.df_daily),
                   ('df_weekly', data_manager.df_weekly),
                   ('df_monthly', data_manager.df_monthly)]

        for attr_name, df in targets:
            if df is None:
                continue

            new_exprs = []
            for func, field, param in matches:
                func_name, field_name, p_val = func.upper(), field.upper(), int(param)
                col_name = f"{func_name}_{field_name}_{p_val}"

                # 如果该表中没有这一列，则加入计算队列
                if col_name not in df.columns:
                    try:
                        if func_name in data_manager.INDICATOR_MAP:
                            base_expr = data_manager.INDICATOR_MAP[func_name](
                                blink_parser.fields[field_name], p_val
                            )
                            # 校验：先在当前表上试算一行，若全 null 则不挂载（避免快速路径返回全 null）
                            test_val = df.select(base_expr.head(1)).item()
                            if test_val is None:
                                logger.warning(f"Hot-JIT skip {col_name} on {attr_name}: test eval returned None")
                                continue
                            expr = base_expr.alias(col_name)
                            new_exprs.append(expr)
                    except Exception as e:
                        logger.warning(f"Hot-JIT compute {col_name} on {attr_name} failed: {e}")
                        continue

            if new_exprs:
                # 挂载列
                updated_df = df.with_columns(new_exprs)
                setattr(data_manager, attr_name, updated_df)
                logger.info(f"Hot-JIT Broadcast: Mounted {len(new_exprs)} cols to {attr_name}")

    def execute_selector(self, formula: str, timeframe: str, background_tasks, target_date=None):
        """执行选股。target_date（datetime.date）可选：指定时回退到 ≤ 该日的最近交易日，未指定用数据最新日。"""
        # 1. 执行全周期热挂载
        self._prepare_hot_jit(formula)

        # 2. 选择当前执行周期的数据表
        df_attr = {'D': 'df_daily', 'W': 'df_weekly', 'M': 'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)

        s_df_attr = {'D': 'df_sector_daily', 'W': 'df_sector_weekly', 'M': 'df_sector_monthly'}.get(timeframe, 'df_sector_daily')
        s_df = getattr(data_manager, s_df_attr)

        if df is None:
            return {"error": "Data not loaded."}
        lf = df.lazy()

        # 3. 关联板块 (Safe Join)
        # 安全获取 df_mapping，如果不存在则返回 None，避免 AttributeError
        df_mapping = getattr(data_manager, 'df_mapping', None)
        if df_mapping is not None and s_df is not None:
            try:
                sector_exprs = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close")]
                if "pctChg" in s_df.columns:
                    sector_exprs.append(pl.col("pctChg").alias("s_pctChg"))
                s_lazy = s_df.lazy().select(sector_exprs)
                lf = (lf.join(df_mapping.lazy(), on="code", how="left")
                      .join(s_lazy, on=["date", "sector_code"], how="left"))
            except Exception as e:
                logger.warning(f"Sector join failed: {e}")
                pass

        try:
            # 4. 解析与计算 (Parser 内部直接引用统一列名)
            expr = blink_parser.parse_expression(formula, timeframe)

            lf = lf.with_columns(expr.alias("_signal"))

            if df.is_empty():
                return {"codes": [], "date": None}

            # 5. 目标交易日：未指定 → 数据最新日；指定 → 回退至 ≤ 指定日的最近交易日
            if target_date is not None:
                eligible = df.filter(pl.col("date") <= target_date)
                if eligible.is_empty():
                    min_date = df.select(pl.col("date").min()).item()
                    return {"error": f"指定日期 {target_date} 早于数据起点 {min_date}"}
                last_date = eligible.select(pl.col("date").max()).item()
            else:
                last_date = df.select(pl.col("date").max()).item()

            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal").fill_null(False) == True)
                         .select("code")
                         .collect())

            return {"codes": result_df["code"].to_list(), "date": last_date.isoformat()}
        except Exception as e:
            return {"error": str(e)}


selection_engine = SelectionEngine()
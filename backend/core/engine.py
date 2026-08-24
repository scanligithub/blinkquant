import polars as pl
import re
import logging
import datetime
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
        # MTF 指标模式：可选 W./M./D. 前缀
        _tf_prefix = r'(?:[DWM]\.)?'
        self.metric_pattern_mtf = re.compile(
            rf'{_tf_prefix}\b({_funcs})\s*\(\s*{_tf_prefix}({_fields})\s*,\s*(\d+)\s*\)',
            re.IGNORECASE)
        # 指标使用的 canonical set cache: {(tf, canonical_atom): {"code": set, ...}}
        self._set_cache: dict = {}
        self._set_cache_max = 32

    def _canonical_atom_key(self, tf: str, target_date: datetime.date, atom_expr_str: str) -> str:
        """生成 atom 的 canonical key（用于 set cache 去重），含 target_date 隔离不同交易日。"""
        return f"{tf}:{target_date.isoformat()}:{atom_expr_str}"

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
        """执行选股。

        target_date（datetime.date）可选：指定时回退到 ≤ 该日的最近交易日，未指定用数据最新日。

        多周期支持：
        - 检测 W./M. 前缀 → 使用 parse_multi_tf → plan tree
        - 无前缀 → 单周期路径（向后兼容）
        """
        # 0. 统一 target_date：未指定 → df_daily 最新日；指定 → 归一到 ≤ 该日的最近交易日
        if target_date is None:
            if data_manager.df_daily is not None and not data_manager.df_daily.is_empty():
                target_date = data_manager.df_daily.select(pl.col("date").max()).item()
            else:
                return {"error": "Data not loaded."}
        else:
            eff = (data_manager.df_daily
                   .filter(pl.col("date") <= target_date)
                   .select(pl.col("date").max())
                   .item())
            if eff is None:
                return {"error": f"指定日期 {target_date} 早于数据起点，无可用交易日数据"}
            target_date = eff

        # 1. 执行全周期热挂载
        self._prepare_hot_jit(formula)

        # 2. 检测是否有 MTF 前缀
        clean_expr = formula.strip().replace('&&', '&').replace('||', '|')
        has_mtf = bool(re.search(r'\b[WM]\.\s*([A-Z_]+|[A-Z_]+\s*\()', clean_expr))

        if has_mtf:
            return self._execute_mtf(formula, timeframe, target_date)
        else:
            return self._execute_single(formula, timeframe, target_date)

    def _execute_single(self, formula: str, timeframe: str, target_date: datetime.date):
        """单周期执行路径（向后兼容）。"""
        # 选择当前执行周期的数据表
        df_attr = {'D': 'df_daily', 'W': 'df_weekly', 'M': 'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)

        s_df_attr = {'D': 'df_sector_daily', 'W': 'df_sector_weekly', 'M': 'df_sector_monthly'}.get(timeframe, 'df_sector_daily')
        s_df = getattr(data_manager, s_df_attr)

        if df is None:
            return {"error": "Data not loaded."}
        lf = df.lazy()

        # 关联板块 (Safe Join)
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

        try:
            # 解析与计算
            expr = blink_parser.parse_expression(formula, timeframe)

            # 目标交易日校验
            eligible = df.filter(pl.col("date") <= target_date)
            if eligible.is_empty():
                min_date = df.select(pl.col("date").min()).item()
                return {"error": f"指定日期 {target_date} 早于数据起点 {min_date}"}

            # per-code as-of：在 ≤ target_date 的全历史上计算指标，
            # 每只股票取最后一根 bar 的 _signal（与 MTF _eval_atom 语义一致，不再用全局 last_date）
            result_df = (
                lf.filter(pl.col("date") <= target_date)
                  .with_columns(expr.alias("_signal"))
                  .group_by("code")
                  .agg(pl.col("_signal").last().fill_null(False).alias("_signal"))
                  .filter(pl.col("_signal"))
                  .select("code")
                  .collect()
            )

            return {"codes": result_df["code"].to_list(), "date": target_date.isoformat()}
        except Exception as e:
            return {"error": str(e)}

    def _execute_mtf(self, formula: str, base_tf: str, target_date: datetime.date):
        """多周期执行路径。

        流程：
        1. parse_multi_tf → plan tree
        2. 对每个 atom 独立求值 → Set[code]
        3. 布尔折叠（AND→交集，OR→并集）
        """
        try:
            plan = blink_parser.parse_multi_tf(formula, base_tf)
        except Exception as e:
            return {"error": f"Parse error: {e}"}

        # 递归折叠 plan tree
        result_set = self._fold_plan(plan, target_date, base_tf)
        codes = sorted(result_set) if result_set else set()

        return {"codes": list(codes), "date": target_date.isoformat()}

    def _fold_plan(self, plan: dict, target_date: datetime.date, base_tf: str) -> set:
        """递归折叠 plan tree，返回 code set。"""
        if plan["type"] == "atom":
            return self._eval_atom(plan, target_date, base_tf)
        elif plan["type"] == "bool":
            child_sets = [self._fold_plan(c, target_date, base_tf) for c in plan["children"]]
            if plan["op"] == "AND":
                return set.intersection(*child_sets) if child_sets else set()
            else:  # OR
                return set.union(*child_sets) if child_sets else set()
        return set()

    def _eval_atom(self, atom: dict, target_date: datetime.date, base_tf: str) -> set:
        """对单个 atom 求值，返回 code set。

        契约：返回“每一只股票在 target_date 的 as-of bar 上，该 atom 是否成立”的集合。
        1. 统一经 build_asof_frame 取得 ≤ target_date 的 as-of frame（D/W/M 一致）
        2. 在全历史上计算指标（保留历史窗口）
        3. 每只股票取最后一根 bar 的 _signal（per-code as-of）
        4. mount_enabled = (atom_tf == base_tf)
        5. set cache（key 含 target_date）
        """
        tf = atom["tf"]
        expr = atom["expr"]

        # 生成 canonical key（用于 set cache，含 target_date 隔离）
        cache_key = self._canonical_atom_key(tf, target_date, atom.get("source", str(expr)))
        cached = self._set_cache.get(cache_key)
        if cached is not None:
            return cached

        # 统一 as-of：D/W/M 均经 build_asof_frame 取得 ≤ target_date 的历史
        df = data_manager.build_asof_frame(tf, target_date)
        if df is None or df.is_empty():
            return set()

        # 设置 mount_enabled：基础周期原子可挂载，非基础周期原子走 slow-path
        blink_parser.mount_enabled = (tf == base_tf)

        try:
            # 先计算指标（全历史窗口）→ 每只股票取最后一根 → 判 _signal
            signal_df = (
                df.sort(["code", "date"])
                  .with_columns(expr.alias("_signal"))
                  .select(["code", "date", "_signal"])
                  .group_by("code")
                  .agg(pl.col("_signal").last().fill_null(False).alias("_signal"))
                  .filter(pl.col("_signal"))
                  .select("code")
            )
            result_codes = set(signal_df["code"].to_list())
        except Exception as e:
            logger.warning(f"Atom eval failed for {tf}: {e}")
            result_codes = set()

        # 更新 set cache（LRU：超容量删最旧）
        if len(self._set_cache) >= self._set_cache_max:
            oldest_key = next(iter(self._set_cache))
            del self._set_cache[oldest_key]
        self._set_cache[cache_key] = result_codes

        return result_codes


selection_engine = SelectionEngine()
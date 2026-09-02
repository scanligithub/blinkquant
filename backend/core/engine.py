import polars as pl
import re
import logging
import datetime
from .data_manager import data_manager
from .security import blink_parser
from .indicator_registry import WINDOW_NAMES, FIELDS
from .selection_result import SelectionResult
from .signal_trace import AtomTrace, CodeTrace, SignalTraceData

logger = logging.getLogger(__name__)


class UnsupportedInBacktestError(RuntimeError):
    """回测模式下禁止使用板块/行业字段（PIT leakage 风险）。"""
    pass


class BacktestSelectionError(RuntimeError):
    """回测模式下选股失败，立即中止回测（不允许 fail-silent）。"""
    pass


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

    def execute_selector(self, formula: str, timeframe: str, background_tasks, target_date=None,
                         backtest_mode: bool = False, raise_on_error: bool = False,
                         trace: bool = False, qfq_data_provider=None, latest_adj: dict = None):
        """执行选股。

        target_date（datetime.date）可选：指定时回退到 ≤ 该日的最近交易日，未指定用数据最新日。
        backtest_mode: True 时禁止使用板块/行业字段（防止 PIT leakage）。
        raise_on_error: True 时异常直接抛出（回测模式），False 时返回 error dict（API 兼容）。
        trace: True 时生成完整 SignalTraceData（P2-2A lazy trace）。
        qfq_data_provider: 可选，用于回测模式的前复权数据提供者（懒加载）。
        latest_adj: 可选，{code: latest_adjust_factor} 用于前复权计算。

        多周期支持：
        - 检测 W./M. 前缀 → 使用 parse_multi_tf → plan tree
        - 无前缀 → 单周期路径（向后兼容）
        """
        # 0. P0-1: 回测模式下禁止板块/行业字段（在任何数据加载之前拦截）
        if backtest_mode:
            _sector_kw = re.compile(r'\b(?:S_CLOSE|S_PCT_CHG|INDUSTRY_\w+|SECTOR_\w+)\b', re.IGNORECASE)
            if _sector_kw.search(formula):
                raise UnsupportedInBacktestError(
                    f"回测模式禁止使用板块/行业字段: {formula!r}。"
                    f"历史板块映射为静态数据，存在 PIT leakage 风险。"
                    f"实时选股（backtest_mode=False）可正常使用。"
                )

        # 1. 统一 target_date：未指定 → df_daily 最新日；指定 → 归一到 ≤ 该日的最近交易日
        # 记录用户原始输入用于审计
        requested_date = target_date
        if target_date is None:
            if qfq_data_provider is not None and latest_adj is not None:
                # 从 qfq_data_provider 获取最新日期
                end_date = datetime.date.today()
                start_date = end_date - datetime.timedelta(days=365)
                try:
                    df = qfq_data_provider.load_qfq_window(start_date, end_date, latest_adj)

                    if not df.is_empty():
                        target_date = df.select(pl.col("date").max()).item()
                    else:
                        return {"error": "Data not loaded."}
                except Exception:
                    return {"error": "Data not loaded."}
            elif data_manager.df_daily is not None and not data_manager.df_daily.is_empty():
                target_date = data_manager.df_daily.select(pl.col("date").max()).item()
            else:
                return {"error": "Data not loaded."}
        else:
            if qfq_data_provider is not None and latest_adj is not None:
                # 查找 ≤ target_date 的最近交易日
                try:
                    df = qfq_data_provider.load_qfq_window(
                        target_date - datetime.timedelta(days=365), target_date, latest_adj
                    )
                    eff = df.filter(pl.col("date") <= target_date).select(pl.col("date").max()).item()
                    if eff is None:
                        return {"error": f"指定日期 {target_date} 早于数据起点，无可用交易日数据"}
                    target_date = eff
                except Exception:
                    return {"error": f"指定日期 {target_date} 早于数据起点，无可用交易日数据"}
            else:
                eff = (data_manager.df_daily
                       .filter(pl.col("date") <= target_date)
                       .select(pl.col("date").max())
                       .item())
                if eff is None:
                    return {"error": f"指定日期 {target_date} 早于数据起点，无可用交易日数据"}
                target_date = eff

        # 2. 执行全周期热挂载（仅当使用 data_manager 时）
        if qfq_data_provider is None or latest_adj is None:
            self._prepare_hot_jit(formula)

        # 2. 检测是否有 MTF 前缀
        clean_expr = formula.strip().replace('&&', '&').replace('||', '|')
        has_mtf = bool(re.search(r'\b[WM]\.\s*([A-Z_]+|[A-Z_]+\s*\()', clean_expr))

        if has_mtf:
            result_dict = self._execute_mtf(formula, timeframe, target_date,
                                            backtest_mode=backtest_mode,
                                            raise_on_error=raise_on_error,
                                            qfq_data_provider=qfq_data_provider,
                                            latest_adj=latest_adj)
        else:
            result_dict = self._execute_single(formula, timeframe, target_date,
                                               backtest_mode=backtest_mode,
                                               raise_on_error=raise_on_error,
                                               qfq_data_provider=qfq_data_provider,
                                               latest_adj=latest_adj)

        # 返回 SelectionResult（保持错误 dict 兼容）
        if isinstance(result_dict, dict) and "error" in result_dict:
            return result_dict

        return SelectionResult(
            requested_date=requested_date,
            signal_date=target_date,
            codes=result_dict["codes"],
            metadata={
                "formula": formula,
                "timeframe": timeframe,
                "has_mtf": has_mtf,
                "nodes_responding": 1,
                "degraded": False,
            }
        )

    def execute_selector_ranked(self, formula: str, target_date: datetime.date,
                                ranking_fn, top_n: int = 20,
                                eligible_codes: list = None,
                                requested_date: datetime.date = None):
        """Ranking-aware selection：eligibility filter → ranking → Top-N → codes。

        与 execute_selector 的区别：
        1. 加载 ≤ target_date 的 daily 数据（含历史窗口供 ranking 计算指标）
        2. 对所有 eligible codes 应用 ranking_fn 评分
        3. 按 score desc 取前 top_n（相同 score → code asc tie-break）

        Returns:
            SelectionResult with codes 和 metadata['scores'] dict
        """
        import polars as pl
        from core.data_manager import data_manager

        # 历史窗口：信号日往前 60 天（供 MA20 等指标计算）
        lookback_days = 60 if ranking_fn is not None else 0
        history_start = target_date - datetime.timedelta(days=lookback_days + 15)

        # 加载 daily frame（含历史）
        if data_manager.df_daily is None:
            return SelectionResult(
                requested_date=requested_date,
                signal_date=target_date,
                codes=[],
                metadata={"formula": formula, "has_ranking": True, "error": "Data not loaded"}
            )

        df = data_manager.df_daily.filter(
            (pl.col("date") >= history_start) & (pl.col("date") <= target_date)
        )
        if eligible_codes:
            df = df.filter(pl.col("code").is_in(eligible_codes))

        if df.is_empty():
            return SelectionResult(
                requested_date=requested_date,
                signal_date=target_date,
                codes=[],
                metadata={"formula": formula, "has_ranking": True}
            )

        # 评估公式表达式（在历史窗口上）
        from core.blink_parser import blink_parser
        has_mtf = any(tok in formula for tok in ['W.', 'w.', 'M.', 'm.'])
        timeframe = 'W' if has_mtf else 'D'

        try:
            expr = blink_parser.parse_expression(formula, timeframe)
            blink_parser.mount_enabled = True

            # 计算 _signal 和 score 在同一趟扫描
            scored = (
                df.sort(["code", "date"])
                .with_columns(expr.alias("_signal"))
            )

            # 取 signal_date 的信号
            signal_day = scored.filter(pl.col("date") == target_date)
            codes_with_signal = (
                signal_day
                .filter(pl.col("_signal"))
                .select("code")
                .to_series()
                .to_list()
            )

            # 如果没有 ranking_fn，直接返回 code asc
            if ranking_fn is None:
                return SelectionResult(
                    requested_date=requested_date,
                    signal_date=target_date,
                    codes=sorted(codes_with_signal),
                    metadata={"formula": formula, "has_ranking": False}
                )

            # ranking 评分（在历史窗口上）
            ranked = ranking_fn(df, target_date)
            if ranked.is_empty():
                return SelectionResult(
                    requested_date=requested_date,
                    signal_date=target_date,
                    codes=[],
                    metadata={"formula": formula, "has_ranking": True}
                )

            # 只保留通过 eligibility 的 codes
            ranked = ranked.filter(pl.col("code").is_in(codes_with_signal))
            if ranked.is_empty():
                return SelectionResult(
                    requested_date=requested_date,
                    signal_date=target_date,
                    codes=[],
                    metadata={"formula": formula, "has_ranking": True}
                )

            # 取 Top-N
            picked_codes = ranked["code"].to_list()[:top_n]
            scores = dict(zip(ranked["code"].to_list(),
                              [round(s, 6) for s in ranked["score"].to_list()]))

            return SelectionResult(
                requested_date=requested_date,
                signal_date=target_date,
                codes=picked_codes,
                metadata={
                    "formula": formula,
                    "has_ranking": True,
                    "ranking_fn": ranking_fn.__name__ if hasattr(ranking_fn, '__name__') else str(ranking_fn),
                    "scores": {c: scores.get(c, 0.0) for c in picked_codes},
                    "eligible_count": len(codes_with_signal),
                }
            )

        except Exception as e:
            logger.warning(f"Ranked selection failed: {e}")
            return SelectionResult(
                requested_date=requested_date,
                signal_date=target_date,
                codes=[],
                metadata={"formula": formula, "has_ranking": True, "error": str(e)}
            )

    def _execute_single(self, formula: str, timeframe: str, target_date: datetime.date,
                        backtest_mode: bool = False, raise_on_error: bool = False,
                        qfq_data_provider=None, latest_adj: dict = None):
        """单周期执行路径（向后兼容）。"""
        # P0-1: 回测模式下禁止板块/行业字段（PIT leakage）
        if backtest_mode:
            _sector_kw = re.compile(r'\b(?:S_CLOSE|S_PCT_CHG|INDUSTRY_\w+|SECTOR_\w+)\b', re.IGNORECASE)
            if _sector_kw.search(formula):
                raise UnsupportedInBacktestError(
                    f"回测模式禁止使用板块/行业字段: {formula!r}。"
                    f"历史板块映射为静态数据，存在 PIT leakage 风险。"
                    f"实时选股（backtest_mode=False）可正常使用。"
                )

        # 选择当前执行周期的数据表
        if qfq_data_provider is not None and latest_adj is not None:
            # 回测懒加载模式：从 qfq_data_provider 加载目标日期的数据
            # 需要 lookback 窗口来计算指标（如 MA250 需要 250 天）
            lookback_days = 250
            start_date = target_date - datetime.timedelta(days=lookback_days)
            df = qfq_data_provider.load_qfq_window(start_date, target_date, latest_adj)
            if df.is_empty():
                return {"error": f"No data for target_date {target_date}"}
        else:
            df_attr = {'D': 'df_daily', 'W': 'df_weekly', 'M': 'df_monthly'}.get(timeframe, 'df_daily')
            df = getattr(data_manager, df_attr)

        s_df_attr = {'D': 'df_sector_daily', 'W': 'df_sector_weekly', 'M': 'df_sector_monthly'}.get(timeframe, 'df_sector_daily')
        s_df = getattr(data_manager, s_df_attr)

        if df is None:
            return {"error": "Data not loaded."}
        lf = df.lazy()

        # 关联板块 (Safe Join) — backtest_mode 下跳过，避免无用计算
        if not backtest_mode:
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
            if raise_on_error:
                raise BacktestSelectionError(
                    f"Selection failed: formula={formula!r}, "
                    f"target_date={target_date}: {e}") from e
            return {"error": str(e)}

    def _execute_mtf(self, formula: str, base_tf: str, target_date: datetime.date,
                     backtest_mode: bool = False, raise_on_error: bool = False,
                     qfq_data_provider=None, latest_adj: dict = None):
        """多周期执行路径。

        流程：
        1. parse_multi_tf → plan tree
        2. 对每个 atom 独立求值 → Set[code]
        3. 布尔折叠（AND→交集，OR→并集）
        """
        try:
            plan = blink_parser.parse_multi_tf(formula, base_tf)
        except Exception as e:
            if raise_on_error:
                raise BacktestSelectionError(
                    f"MTF parse failed: formula={formula!r}, "
                    f"target_date={target_date}: {e}") from e
            return {"error": f"Parse error: {e}"}

        # 递归折叠 plan tree
        result_set = self._fold_plan(plan, target_date, base_tf,
                                     backtest_mode=backtest_mode,
                                     raise_on_error=raise_on_error,
                                     qfq_data_provider=qfq_data_provider,
                                     latest_adj=latest_adj)
        codes = sorted(result_set) if result_set else set()

        return {"codes": list(codes), "date": target_date.isoformat()}

    def _fold_plan(self, plan: dict, target_date: datetime.date, base_tf: str,
                   backtest_mode: bool = False, raise_on_error: bool = False,
                   qfq_data_provider=None, latest_adj: dict = None) -> set:
        """递归折叠 plan tree，返回 code set。"""
        if plan["type"] == "atom":
            return self._eval_atom(plan, target_date, base_tf,
                                   backtest_mode=backtest_mode,
                                   raise_on_error=raise_on_error)
        elif plan["type"] == "bool":
            child_sets = [self._fold_plan(c, target_date, base_tf,
                                          backtest_mode=backtest_mode,
                                          raise_on_error=raise_on_error,
                                          qfq_data_provider=qfq_data_provider,
                                          latest_adj=latest_adj)
                          for c in plan["children"]]
            if plan["op"] == "AND":
                return set.intersection(*child_sets) if child_sets else set()
            else:  # OR
                return set.union(*child_sets) if child_sets else set()
        return set()

    def _eval_atom(self, atom: dict, target_date: datetime.date, base_tf: str,
                   backtest_mode: bool = False, raise_on_error: bool = False,
                   qfq_data_provider=None, latest_adj: dict = None) -> set:
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

        # P0-1: 回测模式下禁止板块/行业字段（MTF atom 级别检查）
        if backtest_mode:
            atom_src = atom.get("source", "")
            _sector_kw = re.compile(r'\b(?:S_CLOSE|S_PCT_CHG|INDUSTRY_\w+|SECTOR_\w+)\b', re.IGNORECASE)
            if _sector_kw.search(atom_src):
                raise UnsupportedInBacktestError(
                    f"回测模式禁止使用板块/行业字段: {atom_src!r}。"
                    f"历史板块映射为静态数据，存在 PIT leakage 风险。"
                )

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
            if raise_on_error:
                raise BacktestSelectionError(
                    f"MTF atom eval failed: tf={tf}, "
                    f"target_date={target_date}, "
                    f"atom={atom.get('source', '?')}: {e}") from e
            logger.warning(f"Atom eval failed for {tf}: {e}")
            result_codes = set()

        # 更新 set cache（LRU：超容量删最旧）
        if len(self._set_cache) >= self._set_cache_max:
            oldest_key = next(iter(self._set_cache))
            del self._set_cache[oldest_key]
        self._set_cache[cache_key] = result_codes

        return result_codes


    # =========================================================================
    # SignalTrace generation (P1-2)
    # =========================================================================
    def execute_selector_with_trace(self, formula: str, timeframe: str, background_tasks,
                                    target_date=None, backtest_mode: bool = False,
                                    raise_on_error: bool = False):
        """执行选股并生成 SignalTraceData（P1-2）。

        Returns:
            tuple: (SelectionResult, SignalTraceData)
        """
        # Reuse existing execute_selector logic, but also generate trace
        result = self.execute_selector(formula, timeframe, background_tasks,
                                        target_date, backtest_mode, raise_on_error)

        if isinstance(result, dict) and "error" in result:
            # Error case
            if raise_on_error:
                raise BacktestSelectionError(f"Selection failed: {result['error']}")
            return result, None

        # Generate trace for the selected codes
        trace = self._generate_trace(result.codes, formula, timeframe, result.signal_date, backtest_mode)

        return result, trace

    def _generate_trace(self, codes: list, formula: str, timeframe: str,
                        signal_date: datetime.date, backtest_mode: bool) -> SignalTraceData:
        """为选中的 codes 生成完整的 SignalTraceData。"""
        trace = SignalTraceData(
            engine_version="unknown",
            signal_date=signal_date.isoformat(),
            formula=formula,
            traces=[],
        )

        for code in codes:
            code_trace = self._trace_code(code, formula, timeframe, signal_date, backtest_mode)
            if code_trace:
                trace.traces.append(code_trace)

        return trace

    def _trace_code(self, code: str, formula: str, timeframe: str,
                    signal_date: datetime.date, backtest_mode: bool) -> CodeTrace:
        """对单个 code 生成完整的原子级 trace。"""
        # Get the plan tree to extract all atoms
        has_mtf = bool(re.search(r'\b[WM]\.\s*([A-Z_]+|[A-Z_]+\s*\()', formula.strip().replace('&&', '&').replace('||', '|')))

        if has_mtf:
            plan = blink_parser.parse_multi_tf(formula, 'D')
            atoms = self._extract_atoms_from_plan(plan, code, signal_date, backtest_mode)
        else:
            # Single timeframe - parse expression and extract atoms
            expr = blink_parser.parse_expression(formula, 'D')
            atoms = self._extract_atoms_from_expr(expr, formula, 'D', code, signal_date, backtest_mode)

        return CodeTrace(
            code=code,
            passed=True,  # Selected codes are by definition passed
            atoms=atoms,
            execution=None,  # Filled later by BacktestEngine
        )

    def _extract_atoms_from_plan(self, plan: dict, code: str,
                                 signal_date: datetime.date, backtest_mode: bool) -> list:
        """从 MTF plan tree 提取原子。"""
        atoms = []
        if plan["type"] == "atom":
            atom_trace = self._trace_atom(plan, code, signal_date, backtest_mode)
            if atom_trace:
                atoms.append(atom_trace)
        elif plan["type"] == "bool":
            for child in plan["children"]:
                atoms.extend(self._extract_atoms_from_plan(child, code, signal_date, backtest_mode))
        return atoms

    def _extract_atoms_from_expr(self, expr, formula: str, timeframe: str,
                                 code: str, signal_date: datetime.date, backtest_mode: bool) -> list:
        """从单周期表达式提取原子（简化版：直接解析 formula 中的原子）。"""
        # 从 formula 字符串中提取原子信息
        atoms = []
        matches = self.metric_pattern.findall(formula)
        for func, field, param in matches:
            atom_id = f"{func.upper()}_{field.upper()}_{param}"
            atoms.append(self._trace_single_atom(atom_id, field.upper(), int(param), code, signal_date, backtest_mode))
        return atoms

    def _trace_single_atom(self, atom_id: str, field: str, window: int,
                           code: str, signal_date: datetime.date, backtest_mode: bool) -> AtomTrace:
        """Trace 单个原子的评估结果。"""
        # 获取该 code 在 signal_date 的 as-of frame 数据
        frame = data_manager.build_asof_frame('D', signal_date)
        if frame is None or frame.is_empty():
            return AtomTrace(
                atom_id=atom_id, field=field, window=str(window),
                value=float("nan"), operator=None, threshold=None, passed=False
            )

        code_df = frame.filter(pl.col("code") == code).sort("date")
        if code_df.is_empty():
            return AtomTrace(
                atom_id=atom_id, field=field, window=str(window),
                value=float("nan"), operator=None, threshold=None, passed=False
            )

        last_bar = code_df.tail(1)
        if last_bar.is_empty():
            return AtomTrace(
                atom_id=atom_id, field=field, window=str(window),
                value=float("nan"), operator=None, threshold=None, passed=False
            )

        # 获取字段值
        if field not in last_bar.columns:
            return AtomTrace(
                atom_id=atom_id, field=field, window=str(window),
                value=float("nan"), operator=None, threshold=None, passed=False
            )

        value = last_bar[field].item()
        if value is None:
            return AtomTrace(
                atom_id=atom_id, field=field, window=str(window),
                value=float("nan"), operator=None, threshold=None, passed=False
            )

        # 对于比较类原子，我们需要解析完整表达式来获取 operator 和 threshold
        # 这里简化：仅记录原始值和通过状态
        # 实际比较在 blink_parser 内部完成，我们只记录原子值
        return AtomTrace(
            atom_id=atom_id,
            field=field,
            window=str(window),
            value=float(value) if value is not None else float("nan"),
            operator=None,
            threshold=None,
            passed=True,  # Selected codes passed the overall formula
        )


selection_engine = SelectionEngine()
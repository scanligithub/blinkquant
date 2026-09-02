#!/usr/bin/env python3
"""Apply lazy loading changes to engine.py and backtest_engine.py"""
import re

def apply_engine_changes():
    with open('backend/core/engine.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Update execute_selector signature
    content = content.replace(
        'def execute_selector(self, formula: str, timeframe: str, background_tasks, target_date=None,\n                         backtest_mode: bool = False, raise_on_error: bool = False):',
        'def execute_selector(self, formula: str, timeframe: str, background_tasks, target_date=None,\n                         backtest_mode: bool = False, raise_on_error: bool = False,\n                         qfq_data_provider=None, latest_adj: dict = None):'
    )
    
    # 2. Update docstring for execute_selector
    old_docstring = '''        """执行选股。

        target_date（datetime.date）可选：指定时回退到 ≤ 该日的最近交易日，未指定用数据最新日。
        backtest_mode: True 时禁止使用板块/行业字段（防止 PIT leakage）。
        raise_on_error: True 时异常直接抛出（回测模式），False 时返回 error dict（API 兼容）。

        多周期支持：
        - 检测 W./M. 前缀 → 使用 parse_multi_tf → plan tree
        - 无前缀 → 单周期路径（向后兼容）
        """'''
    
    new_docstring = '''        """执行选股。

        target_date（datetime.date）可选：指定时回退到 ≤ 该日的最近交易日，未指定用数据最新日。
        backtest_mode: True 时禁止使用板块/行业字段（防止 PIT leakage）。
        raise_on_error: True 时异常直接抛出（回测模式），False 时返回 error dict（API 兼容）。
        qfq_data_provider: 可选，用于回测模式的前复权数据提供者（懒加载）。
        latest_adj: 可选，{code: latest_adjust_factor} 用于前复权计算。

        多周期支持：
        - 检测 W./M. 前缀 → 使用 parse_multi_tf → plan tree
        - 无前缀 → 单周期路径（向后兼容）
        """'''
    
    content = content.replace(old_docstring, new_docstring)
    
    # 3. Update target_date normalization to support lazy loading
    old_norm = '''        # 1. 统一 target_date：未指定 → df_daily 最新日；指定 → 归一到 ≤ 该日的最近交易日
        # 记录用户原始输入用于审计
        requested_date = target_date
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

        # 2. 执行全周期热挂载
        self._prepare_hot_jit(formula)'''
    
    new_norm = '''        # 1. 统一 target_date：未指定 → df_daily 最新日；指定 → 归一到 ≤ 该日的最近交易日
        # 记录用户原始输入用于审计
        requested_date = target_date
        if target_date is None:
            if qfq_data_provider is not None and latest_adj is not None:
                # 从 qfq_data_provider 获取最新日期
                import datetime as _dt
                end_date = _dt.date.today()
                start_date = end_date - _dt.timedelta(days=365)
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
                        target_date - _dt.timedelta(days=365), target_date, latest_adj
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
            self._prepare_hot_jit(formula)'''
    
    content = content.replace(old_norm, new_norm)
    
    # 4. Update calls to _execute_mtf and _execute_single
    content = content.replace(
        '''            result_dict = self._execute_mtf(formula, timeframe, target_date,
                                            backtest_mode=backtest_mode,
                                            raise_on_error=raise_on_error)''',
        '''            result_dict = self._execute_mtf(formula, timeframe, target_date,
                                            backtest_mode=backtest_mode,
                                            raise_on_error=raise_on_error,
                                            qfq_data_provider=qfq_data_provider,
                                            latest_adj=latest_adj)'''
    )
    
    content = content.replace(
        '''            result_dict = self._execute_single(formula, timeframe, target_date,
                                               backtest_mode=backtest_mode,
                                               raise_on_error=raise_on_error)''',
        '''            # P2-3B: Try pre-computing signal matrix for D-timeframe simple formulas
            self._precompute_signal_matrix(formula, timeframe, backtest_mode=backtest_mode,
                                           qfq_data_provider=qfq_data_provider, latest_adj=latest_adj)
            result_dict = self._execute_single(formula, timeframe, target_date,
                                               backtest_mode=backtest_mode,
                                               raise_on_error=raise_on_error,
                                               qfq_data_provider=qfq_data_provider,
                                               latest_adj=latest_adj)'''
    )
    
    # 5. Update _precompute_signal_matrix signature
    content = content.replace(
        '    def _precompute_signal_matrix(self, formula: str, timeframe: str,\n                                  backtest_mode: bool = False) -> bool:',
        '    def _precompute_signal_matrix(self, formula: str, timeframe: str,\n                                  backtest_mode: bool = False,\n                                  qfq_data_provider=None, latest_adj: dict = None) -> bool:'
    )
    
    # 6. Update _precompute_signal_matrix body to handle lazy mode
    old_precompute = '''    def _precompute_signal_matrix(self, formula: str, timeframe: str,
                                  backtest_mode: bool = False) -> bool:
        """尝试预计算信号矩阵：对 D 周期简单公式，一次计算所有日期的信号。

        成功返回 True，缓存存于 self._signal_matrix_cache[formula_key]。
        失败返回 False，调用方回退到逐日期计算。
        """
        if timeframe != 'D':
            return False
        if backtest_mode:
            return False
        if not self._signal_matrix_enabled:
            return False

        # 只优化简单 D 公式：无 MTF 前缀
        clean = formula.strip().replace('&&', '&').replace('||', '|')
        if re.search(r'\\b[WM]\\.\\s*([A-Z_]+|[A-Z_]+\\s*\\()', clean):
            return False

        cache_key = f"sigmat:{timeframe}:{formula}"
        if cache_key in self._signal_matrix_cache:
            return True

        df = data_manager.df_daily
        if df is None:
            return False

        try:
            # 解析公式得到 Polars 表达式
            expr = blink_parser.parse_expression(formula, timeframe)
            # 一次性计算所有 date × code 的信号
            sig_df = (
                df.lazy()
                  .with_columns(expr.alias("_signal"))
                  .select(["date", "code", "_signal"])
                  .collect()
            )
            self._signal_matrix_cache[cache_key] = sig_df
            return True
        except Exception as e:
            logger.warning(f"Signal matrix precompute failed: {e}")
            return False'''
    
    new_precompute = '''    def _precompute_signal_matrix(self, formula: str, timeframe: str,
                                  backtest_mode: bool = False,
                                  qfq_data_provider=None, latest_adj: dict = None) -> bool:
        """尝试预计算信号矩阵：对 D 周期简单公式，一次计算所有日期的信号。

        成功返回 True，缓存存于 self._signal_matrix_cache[formula_key]。
        失败返回 False，调用方回退到逐日期计算。
        """
        if timeframe != 'D':
            return False
        if backtest_mode:
            return False
        if not self._signal_matrix_enabled:
            return False

        # 只优化简单 D 公式：无 MTF 前缀
        clean = formula.strip().replace('&&', '&').replace('||', '|')
        if re.search(r'\\b[WM]\\.\\s*([A-Z_]+|[A-Z_]+\\s*\\()', clean):
            return False

        cache_key = f"sigmat:{timeframe}:{formula}"
        if cache_key in self._signal_matrix_cache:
            return True

        if qfq_data_provider is not None and latest_adj is not None:
            # For backtest lazy mode, we don't precompute signal matrix (too much data)
            return False

        df = data_manager.df_daily
        if df is None:
            return False

        try:
            # 解析公式得到 Polars 表达式
            expr = blink_parser.parse_expression(formula, timeframe)
            # 一次性计算所有 date × code 的信号
            sig_df = (
                df.lazy()
                  .with_columns(expr.alias("_signal"))
                  .select(["date", "code", "_signal"])
                  .collect()
            )
            self._signal_matrix_cache[cache_key] = sig_df
            return True
        except Exception as e:
            logger.warning(f"Signal matrix precompute failed: {e}")
            return False'''
    
    content = content.replace(old_precompute, new_precompute)
    
    # 7. Update _execute_single signature and body
    old_execute_single = '''    def _execute_single(self, formula: str, timeframe: str, target_date: datetime.date,
                        backtest_mode: bool = False, raise_on_error: bool = False):
        """单周期执行路径（向后兼容）。"""
        # P0-1: 回测模式下禁止板块/行业字段（PIT leakage）
        if backtest_mode:
            _sector_kw = re.compile(r'\\b(?:S_CLOSE|S_PCT_CHG|INDUSTRY_\\w+|SECTOR_\\w+)\\b', re.IGNORECASE)
            if _sector_kw.search(formula):
                raise UnsupportedInBacktestError(
                    f"回测模式禁止使用板块/行业字段: {formula!r}。"
                    f"历史板块映射为静态数据，存在 PIT leakage 风险。"
                    f"实时选股（backtest_mode=False）可正常使用。"
                )

        # 选择当前执行周期的数据表
        df_attr = {'D': 'df_daily', 'W': 'df_weekly', 'M': 'df_monthly'}.get(timeframe, 'df_daily')
        df = getattr(data_manager, df_attr)'''
    
    new_execute_single = '''    def _execute_single(self, formula: str, timeframe: str, target_date: datetime.date,
                        backtest_mode: bool = False, raise_on_error: bool = False,
                        qfq_data_provider=None, latest_adj: dict = None):
        """单周期执行路径（向后兼容）。"""
        # P0-1: 回测模式下禁止板块/行业字段（PIT leakage）
        if backtest_mode:
            _sector_kw = re.compile(r'\\b(?:S_CLOSE|S_PCT_CHG|INDUSTRY_\\w+|SECTOR_\\w+)\\b', re.IGNORECASE)
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
            df = getattr(data_manager, df_attr)'''
    
    content = content.replace(old_execute_single, new_execute_single)
    
    # 8. Update _execute_mtf signature
    content = content.replace(
        '    def _execute_mtf(self, formula: str, base_tf: str, target_date: datetime.date,\n                     backtest_mode: bool = False, raise_on_error: bool = False):',
        '    def _execute_mtf(self, formula: str, base_tf: str, target_date: datetime.date,\n                     backtest_mode: bool = False, raise_on_error: bool = False,\n                     qfq_data_provider=None, latest_adj: dict = None):'
    )
    
    # 9. Update _execute_mtf body to pass parameters
    content = content.replace(
        '''        # 递归折叠 plan tree
        result_set = self._fold_plan(plan, target_date, base_tf,
                                     backtest_mode=backtest_mode,
                                     raise_on_error=raise_on_error)''',
        '''        # 递归折叠 plan tree
        result_set = self._fold_plan(plan, target_date, base_tf,
                                     backtest_mode=backtest_mode,
                                     raise_on_error=raise_on_error,
                                     qfq_data_provider=qfq_data_provider,
                                     latest_adj=latest_adj)'''
    )
    
    # 10. Update _fold_plan signature
    content = content.replace(
        '    def _fold_plan(self, plan: dict, target_date: datetime.date, base_tf: str,\n                   backtest_mode: bool = False, raise_on_error: bool = False) -> set:',
        '    def _fold_plan(self, plan: dict, target_date: datetime.date, base_tf: str,\n                   backtest_mode: bool = False, raise_on_error: bool = False,\n                   qfq_data_provider=None, latest_adj: dict = None) -> set:'
    )
    
    # 11. Update _fold_plan calls to pass parameters
    content = content.replace(
        '''            child_sets = [self._fold_plan(c, target_date, base_tf,
                                          backtest_mode=backtest_mode,
                                          raise_on_error=raise_on_error)
                          for c in plan["children"]]''',
        '''            child_sets = [self._fold_plan(c, target_date, base_tf,
                                          backtest_mode=backtest_mode,
                                          raise_on_error=raise_on_error,
                                          qfq_data_provider=qfq_data_provider,
                                          latest_adj=latest_adj)
                          for c in plan["children"]]'''
    )
    
    # 12. Update _eval_atom signature
    content = content.replace(
        '    def _eval_atom(self, atom: dict, target_date: datetime.date, base_tf: str,\n                   backtest_mode: bool = False, raise_on_error: bool = False) -> set:',
        '    def _eval_atom(self, atom: dict, target_date: datetime.date, base_tf: str,\n                   backtest_mode: bool = False, raise_on_error: bool = False,\n                   qfq_data_provider=None, latest_adj: dict = None) -> set:'
    )
    
    # 13. Update _eval_atom docstring
    old_eval_docstring = '''        """对单个 atom 求值，返回 code set。

        契约：返回"每一只股票在 target_date 的 as-of bar 上，该 atom 是否成立"的集合。
        1. 统一经 build_asof_frame 取得 ≤ target_date 的 as-of frame（D/W/M 一致）
        2. 在全历史上计算指标（保留历史窗口）
        3. 每只股票取最后一根 bar 的 _signal（per-code as-of）
        4. mount_enabled = (atom_tf == base_tf)
        5. set cache（key 含 target_date 隔离）
        """'''
    
    new_eval_docstring = '''        """对单个 atom 求值，返回 code set。

        契约：返回"每一只股票在 target_date 的 as-of bar 上，该 atom 是否成立"的集合。
        1. 统一经 build_asof_frame 取得 ≤ target_date 的 as-of frame（D/W/M 一致）
        2. 在全历史上计算指标（保留历史窗口）
        3. 每只股票取最后一根 bar 的 _signal（per-code as-of）
        4. mount_enabled = (atom_tf == base_tf)
        5. set cache（key 含 target_date 隔离）
        
        Note: qfq_data_provider and latest_adj are accepted for API compatibility
              but MTF path currently falls back to data_manager (full data).
        """'''
    
    content = content.replace(old_eval_docstring, new_eval_docstring)
    
    # 14. Update _eval_atom recursive calls
    content = content.replace(
        '''            return self._eval_atom(plan, target_date, base_tf,
                                    backtest_mode=backtest_mode,
                                    raise_on_error=raise_on_error)''',
        '''            return self._eval_atom(plan, target_date, base_tf,
                                    backtest_mode=backtest_mode,
                                    raise_on_error=raise_on_error,
                                    qfq_data_provider=qfq_data_provider,
                                    latest_adj=latest_adj)'''
    )
    
    content = content.replace(
        '''                           for c in plan["children"]]''',
        '''                           for c in plan["children"]]''')
    
    with open('backend/core/engine.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Applied engine.py changes")

def apply_backtest_engine_changes():
    with open('backend/core/backtest_engine.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Add logger import
    content = content.replace(
        'import datetime\nimport enum',
        'import datetime\nimport enum\nimport logging'
    )
    content = content.replace(
        'logger = logging.getLogger(__name__)\n\n\nclass EventPhase',
        'logger = logging.getLogger(__name__)\n\n\nclass EventPhase'
    )
    
    # 2. Add latest_adj and limit flags pre-computation
    old_init = '''        # Processed corporate actions tracker
        if not is_new_checkpoint:
            self._processed_actions = []
        
        import time as _time'''
    
    new_init = '''        # Processed corporate actions tracker
        if not is_new_checkpoint:
            self._processed_actions = []
        
        # P2-MEM: Precompute latest adjust factors for lazy qfq loading
        logger.info("Precomputing latest adjust factors for lazy qfq loading...")
        self._latest_adj = self.raw_price_store.load_latest_adjust_factors()
        logger.info(f"Loaded latest adjust factors for {len(self._latest_adj)} codes")
        
        import time as _time'''
    
    content = content.replace(old_init, new_init)
    
    # 3. Update _phase_post_close_signal to pass lazy loading params
    old_phase = '''        if ranking_fn is not None:
            sel = self.selection_engine.execute_selector(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True, trace=False)
            if hasattr(sel, 'codes') and sel.codes:
                eligible = sel.codes
                if universe_filter is not None and eligible:
                    eligible = universe_filter.filter(eligible, t)
                import datetime as _dt
                lookback_start = t - _dt.timedelta(days=75)
                frame = data_manager.df_daily.filter(
                    (pl.col("date") >= lookback_start) & (pl.col("date") <= t)
                ).filter(pl.col("code").is_in(eligible)) if data_manager.df_daily is not None and eligible else pl.DataFrame()
                ranked = ranking_fn(frame, t) if not frame.is_empty() else pl.DataFrame()
                if not ranked.is_empty():
                    picked = ranked["code"].to_list()[:top_n]
                    weights = {c: 1.0 / len(picked) for c in picked} if picked else {}
                else:
                    weights = {}
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d
        else:
            sel = self.selection_engine.execute_selector(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True, trace=False)
            if not (isinstance(sel, dict) and "error" in sel):
                codes = sel.codes
                if universe_filter is not None and codes:
                    codes = universe_filter.filter(codes, t)
                weights = self.allocator(codes, t)
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d'''
    
    new_phase = '''        if ranking_fn is not None:
            sel = self.selection_engine.execute_selector(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True, trace=False,
                qfq_data_provider=self.raw_price_store, latest_adj=self._latest_adj)
            if hasattr(sel, 'codes') and sel.codes:
                eligible = sel.codes
                if universe_filter is not None and eligible:
                    eligible = universe_filter.filter(eligible, t)
                import datetime as _dt
                lookback_start = t - _dt.timedelta(days=75)
                frame = data_manager.df_daily.filter(
                    (pl.col("date") >= lookback_start) & (pl.col("date") <= t)
                ).filter(pl.col("code").is_in(eligible)) if data_manager.df_daily is not None and eligible else pl.DataFrame()
                ranked = ranking_fn(frame, t) if not frame.is_empty() else pl.DataFrame()
                if not ranked.is_empty():
                    picked = ranked["code"].to_list()[:top_n]
                    weights = {c: 1.0 / len(picked) for c in picked} if picked else {}
                else:
                    weights = {}
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d
        else:
            sel = self.selection_engine.execute_selector(
                formula, "D", None, target_date=t,
                backtest_mode=True, raise_on_error=True, trace=False,
                qfq_data_provider=self.raw_price_store, latest_adj=self._latest_adj)
            if not (isinstance(sel, dict) and "error" in sel):
                codes = sel.codes
                if universe_filter is not None and codes:
                    codes = universe_filter.filter(codes, t)
                weights = self.allocator(codes, t)
                new_intents = self._generate_intents(weights, new_prices)
                diag["intents_total"] += len(new_intents)
                if weights:
                    diag["target_gross_by_date"][exec_d] = sum(weights.values())
                new_sig, new_exec = t, exec_d'''
    
    content = content.replace(old_phase, new_phase)
    
    # 4. Update _phase_post_execution to use on-demand limit flags
    old_post_exec = '''            intent_codes = [i.code for i in self._pend_intents]
            limit_flags = (data_manager.get_limit_flags(t, intent_codes)
                           if intent_codes else {})'''
    
    new_post_exec = '''            intent_codes = [i.code for i in self._pend_intents]
            # P2-MEM: On-demand limit flags — only load for current execution date and intent codes
            limit_flags = self.raw_price_store.load_limit_flags_for_date(t, intent_codes)'''
    
    content = content.replace(old_post_exec, new_post_exec)
    
    with open('backend/core/backtest_engine.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Applied backtest_engine.py changes")

if __name__ == "__main__":
    apply_engine_changes()
    apply_backtest_engine_changes()
    print("All changes applied")
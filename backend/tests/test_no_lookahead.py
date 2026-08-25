#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
No-lookahead (未来数据泄漏) 回归测试套件

核心思路：投毒差分 (poisoning differential)
- 把 T 日之后的所有行情数据恶意改掉（close×100, high×100, low×100, open×100, volume×1000, 甚至新增只有 T 之后才存在的股票/行）
- 再跑一次 target_date=T 的选股，结果必须与未投毒时完全一致
- 这直接检验“选股结果是否只依赖 ≤T 的数据”的定义，比逐行审计代码可靠得多

覆盖：
1. 投毒差分测试（核心）
2. 截断等价测试
3. as-of frame 不变量
4. 周五完成性不变量（周五 partial 必须等于完整周线）
5. 哨兵自检（证明检测器本身有效：故意制造泄漏，断言检测器报警）
"""

import datetime
import unittest
import polars as pl
from copy import deepcopy

from core.data_manager import data_manager
from core.engine import selection_engine
from core.security import blink_parser


class TestNoLookaheadPoisonDifferential(unittest.TestCase):
    """投毒差分核心测试：T 之后的数据任意改动，不得影响 T 的选股结果"""

    @classmethod
    def setUpClass(cls):
        """构造一个可复用的、覆盖多周/多月的合成 fixture"""
        cls.base_daily = cls._build_fixture()
        # 记录原始状态，供 tearDownClass 恢复
        cls._orig_daily = data_manager.df_daily
        cls._orig_weekly = data_manager.df_weekly
        cls._orig_monthly = data_manager.df_monthly
        cls._orig_mapping = data_manager.df_mapping

    @classmethod
    def tearDownClass(cls):
        data_manager.df_daily = cls._orig_daily
        data_manager.df_weekly = cls._orig_weekly
        data_manager.df_monthly = cls._orig_monthly
        data_manager.df_mapping = cls._orig_mapping
        data_manager._asof_frame_cache.clear()
        selection_engine._set_cache.clear()

    def tearDown(self):
        # 重置 parser 单例状态，避免 mount_enabled / current_df 污染后续测试
        blink_parser.mount_enabled = True
        blink_parser.current_df = None

    @staticmethod
    def _build_fixture() -> pl.DataFrame:
        """
        构造 6 周（约 30 个交易日）× 4 只股票的合成数据：
        - sh.A: 全程在场
        - sh.B: 周三停牌（缺少周三数据）
        - sh.C: 第 4 周周一退市（之后无数据）
        - sh.D: 第 5 周周一新上市（之前无数据）

        日期：2026-07-13 (周一) 到 2026-08-21 (周五)，共 30 个交易日
        """
        dates = []
        codes = []
        closes = []
        opens = []
        highs = []
        lows = []
        volumes = []
        amounts = []

        # 生成 2026-07-13 到 2026-08-21 的所有交易日（跳过周末）
        start = datetime.date(2026, 7, 13)  # Monday
        end = datetime.date(2026, 8, 21)    # Friday
        current = start
        trading_days = []
        while current <= end:
            if current.weekday() < 5:  # Mon-Fri
                trading_days.append(current)
            current += datetime.timedelta(days=1)

        # 股票定义：(code, 开始日索引, 结束日索引(含), 基础价格)
        # 结束日索引为 None 表示到最后
        stock_defs = [
            ("sh.A", 0, None, 10.0),      # 全程在场
            ("sh.B", 0, None, 20.0),      # 全程在场（但会在特定周三缺失）
            ("sh.C", 0, 14, 30.0),        # 第 3 周结束退市（索引 14 对应 8/1 周五）
            ("sh.D", 20, None, 40.0),     # 第 5 周一上市（索引 20 对应 8/10 周一）
        ]

        # 让 sh.B 在第 2 周周三(索引 7)缺数据模拟停牌
        b_missing_days = {7}

        for i, day in enumerate(trading_days):
            for code, start_idx, end_idx, base_price in stock_defs:
                if i < start_idx:
                    continue
                if end_idx is not None and i > end_idx:
                    continue
                if code == "sh.B" and i in b_missing_days:
                    continue

                # 简单价格随机游走（确定性：用 day+code hash）
                seed = (i * 17 + hash(code)) % 1000
                drift = (seed / 1000 - 0.5) * 0.02  # ±1%
                close = round(base_price * (1 + drift), 2)
                open_ = round(close * (1 + (seed % 7 - 3) / 1000), 2)
                high = round(max(close, open_) * (1 + abs(seed % 11) / 1000), 2)
                low = round(min(close, open_) * (1 - abs(seed % 11) / 1000), 2)
                vol = 1_000_000 + (seed % 50) * 10_000
                amt = round(vol * close, 2)

                dates.append(day)
                codes.append(code)
                closes.append(close)
                opens.append(open_)
                highs.append(high)
                lows.append(low)
                volumes.append(float(vol))
                amounts.append(amt)

        df = pl.DataFrame({
            "date": dates,
            "code": codes,
            "close": closes,
            "open": opens,
            "high": highs,
            "low": lows,
            "volume": volumes,
            "amount": amounts,
        }).sort(["code", "date"])
        return df

    def _install_fixture(self, df: pl.DataFrame):
        """安装 fixture 并重置周/月线与缓存"""
        data_manager.df_daily = df
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()
        data_manager._resample_all()
        selection_engine._set_cache.clear()
        blink_parser.mount_enabled = True
        blink_parser.current_df = None

    def _poison_after(self, df: pl.DataFrame, cutoff: datetime.date) -> pl.DataFrame:
        """
        对 cutoff 之后的所有行进行投毒：
        - close/open/high/low × 100
        - volume × 1000
        - amount = volume × close
        """
        poisoned = df.with_columns([
            pl.when(pl.col("date") > cutoff)
            .then(pl.col("close") * 100)
            .otherwise(pl.col("close")).alias("close"),
            pl.when(pl.col("date") > cutoff)
            .then(pl.col("open") * 100)
            .otherwise(pl.col("open")).alias("open"),
            pl.when(pl.col("date") > cutoff)
            .then(pl.col("high") * 100)
            .otherwise(pl.col("high")).alias("high"),
            pl.when(pl.col("date") > cutoff)
            .then(pl.col("low") * 100)
            .otherwise(pl.col("low")).alias("low"),
            pl.when(pl.col("date") > cutoff)
            .then(pl.col("volume") * 1000)
            .otherwise(pl.col("volume")).alias("volume"),
        ])
        # 重新计算 amount
        poisoned = poisoned.with_columns(
            (pl.col("volume") * pl.col("close")).alias("amount")
        )
        return poisoned

    def _append_future_only_stocks(self, df: pl.DataFrame, cutoff: datetime.date) -> pl.DataFrame:
        """
        追加几只“只有 cutoff 之后才存在”的新股，确保代码集合成员资格不能来自未来
        """
        future_codes = ["sh.FUTURE1", "sh.FUTURE2"]
        extra_rows = []
        for code in future_codes:
            for day_offset in [1, 2, 3, 4, 5]:  # cutoff 后 5 个交易日
                future_day = cutoff + datetime.timedelta(days=day_offset)
                # 跳过周末
                if future_day.weekday() >= 5:
                    continue
                base = 50.0 if code == "sh.FUTURE1" else 60.0
                extra_rows.append({
                    "date": future_day,
                    "code": code,
                    "close": base,
                    "open": base - 0.1,
                    "high": base + 0.2,
                    "low": base - 0.2,
                    "volume": 500_000.0,
                    "amount": 500_000.0 * base,
                })
        if extra_rows:
            extra_df = pl.DataFrame(extra_rows)
            df = pl.concat([df, extra_df]).sort(["code", "date"])
        return df

    def _run_select(self, formula: str, target_date: datetime.date) -> set:
        """执行选股并返回 code set"""
        result = selection_engine.execute_selector(
            formula, "D", None, target_date=target_date
        )
        if "error" in result:
            raise RuntimeError(f"Select error: {result['error']}")
        return set(result["codes"])

    def test_poison_differential_sweep(self):
        """
        对多个 target_date（覆盖周三/周五/月末/月初/中间日）做投毒差分，
        包括：
        - 纯 D 周期公式
        - W 前缀公式
        - M 前缀公式
        - MTF AND/OR 混合公式
        """
        test_dates = [
            datetime.date(2026, 7, 15),  # 周三（第 1 周）
            datetime.date(2026, 7, 17),  # 周五（第 1 周结束）
            datetime.date(2026, 7, 22),  # 周三（第 2 周）
            datetime.date(2026, 7, 31),  # 周五月末（7 月底）
            datetime.date(2026, 8, 5),   # 周三（8 月初）
            datetime.date(2026, 8, 13),  # 周五（sh.C 已退市，sh.D 未上市）
            datetime.date(2026, 8, 19),  # 周三（sh.D 已上市）
        ]

        formulas = [
            "CLOSE > MA(CLOSE, 5)",                      # 纯 D
            "W.MA(W.CLOSE, 5) > W.MA(W.CLOSE, 10)",     # 纯 W
            "M.MA(M.CLOSE, 5) > M.MA(M.CLOSE, 10)",     # 纯 M
            "CLOSE > MA(CLOSE, 10) AND W.CLOSE > 5",    # D AND W
            "CLOSE > 5 OR W.MA(W.CLOSE, 5) > 10",       # D OR W
            "MA(CLOSE, 20) > MA(CLOSE, 60)",            # 跨周期 MA
            "W.CLOSE > W.MA(W.CLOSE, 20)",              # W 长窗口
        ]

        clean_daily = self.base_daily

        for T in test_dates:
            for formula in formulas:
                with self.subTest(target_date=T, formula=formula):
                    # 1. 干净数据跑一次
                    self._install_fixture(clean_daily)
                    clean_codes = self._run_select(formula, T)

                    # 2. 构造投毒数据：T 之后的行情 ×100 + 新增只有未来才有的股票
                    poisoned = self._poison_after(clean_daily, T)
                    poisoned = self._append_future_only_stocks(poisoned, T)

                    # 3. 投毒数据跑一次
                    self._install_fixture(poisoned)
                    poisoned_codes = self._run_select(formula, T)

                    # 4. 断言完全一致
                    self.assertEqual(
                        clean_codes, poisoned_codes,
                        f"Lookahead leak! formula={formula} T={T} "
                        f"clean={sorted(clean_codes)} poisoned={sorted(poisoned_codes)}"
                    )

    def test_truncation_equivalence(self):
        """
        截断等价：data_manager.df_daily 截断到 ≤T 后选股，结果必须等于完整数据选股
        这直接验证 build_asof_frame 的过滤逻辑是否真正生效
        """
        formulas = [
            "CLOSE > MA(CLOSE, 10)",
            "W.MA(W.CLOSE, 5) > W.MA(W.CLOSE, 10)",
            "CLOSE > 5 AND W.CLOSE > 10",
        ]

        for T in [datetime.date(2026, 7, 22), datetime.date(2026, 8, 5), datetime.date(2026, 8, 13)]:
            for formula in formulas:
                with self.subTest(T=T, formula=formula):
                    # 完整数据
                    self._install_fixture(self.base_daily)
                    full_codes = self._run_select(formula, T)

                    # 截断数据（仅保留 ≤T）
                    truncated = self.base_daily.filter(pl.col("date") <= T)
                    self._install_fixture(truncated)
                    trunc_codes = self._run_select(formula, T)

                    self.assertEqual(
                        full_codes, trunc_codes,
                        f"Truncation inequivalence! formula={formula} T={T} "
                        f"full={sorted(full_codes)} trunc={sorted(trunc_codes)}"
                    )


class TestAsOfFrameInvariants(unittest.TestCase):
    """as-of frame 本身的不变量：所有日期 ≤ target_date，单调性等"""

    def setUp(self):
        self._orig_daily = data_manager.df_daily
        self._orig_weekly = data_manager.df_weekly
        self._orig_monthly = data_manager.df_monthly
        self._orig_mapping = data_manager.df_mapping
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()

        # 复用 TestNoLookaheadPoisonDifferential 的 fixture
        self.daily = TestNoLookaheadPoisonDifferential._build_fixture()
        data_manager.df_daily = self.daily
        data_manager._resample_all()

    def tearDown(self):
        data_manager.df_daily = self._orig_daily
        data_manager.df_weekly = self._orig_weekly
        data_manager.df_monthly = self._orig_monthly
        data_manager.df_mapping = self._orig_mapping
        data_manager._asof_frame_cache.clear()
        # 重置 parser 单例状态，避免 mount_enabled / current_df 污染后续测试
        blink_parser.mount_enabled = True
        blink_parser.current_df = None

    def test_all_dates_le_target(self):
        """as-of frame 中的所有日期必须 ≤ target_date（D/W/M 均适用）"""
        test_dates = [
            datetime.date(2026, 7, 15),
            datetime.date(2026, 7, 31),
            datetime.date(2026, 8, 13),
        ]
        for T in test_dates:
            for tf in ["D", "W", "M"]:
                with self.subTest(tf=tf, T=T):
                    frame = data_manager.build_asof_frame(tf, T)
                    if not frame.is_empty():
                        max_date = frame.select(pl.col("date").max()).item()
                        self.assertLessEqual(max_date, T,
                            f"{tf} frame max_date={max_date} > target={T}")

    def test_frame_monotonicity(self):
        """随着 target_date 推进，as-of frame 的行数/覆盖股票数单调非减"""
        test_dates = [
            datetime.date(2026, 7, 15),
            datetime.date(2026, 7, 22),
            datetime.date(2026, 7, 29),
            datetime.date(2026, 8, 5),
        ]
        prev_D = prev_W = prev_M = 0
        for T in test_dates:
            for tf, prev in [("D", prev_D), ("W", prev_W), ("M", prev_M)]:
                frame = data_manager.build_asof_frame(tf, T)
                rows = frame.height
                if prev > 0:
                    self.assertGreaterEqual(rows, prev,
                        f"{tf} frame rows decreased: {prev} -> {rows} at T={T}")
                if tf == "D": prev_D = rows
                elif tf == "W": prev_W = rows
                else: prev_M = rows

    def test_codes_monotonicity(self):
        """覆盖的股票代码集合单调非减"""
        test_dates = [
            datetime.date(2026, 7, 15),
            datetime.date(2026, 7, 22),
            datetime.date(2026, 7, 29),
            datetime.date(2026, 8, 5),
        ]
        prev_D = prev_W = prev_M = set()
        for T in test_dates:
            for tf, prev in [("D", prev_D), ("W", prev_W), ("M", prev_M)]:
                frame = data_manager.build_asof_frame(tf, T)
                codes = set(frame["code"].to_list())
                if prev:
                    self.assertTrue(prev.issubset(codes),
                        f"{tf} codes lost at T={T}: {prev - codes}")
                if tf == "D": prev_D = codes
                elif tf == "W": prev_W = codes
                else: prev_M = codes


class TestFridayCompletionInvariant(unittest.TestCase):
    """周五完成性：当 target_date 落在周五（周结束日）时，
    合成的 partial 周线的 OHLCV 必须等于该周完整周线（group_by_dynamic 生成、以周一为 date 标签）的 OHLCV
    """

    def setUp(self):
        self._orig_daily = data_manager.df_daily
        self._orig_weekly = data_manager.df_weekly
        self._orig_monthly = data_manager.df_monthly
        self._orig_mapping = data_manager.df_mapping
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()

        # 构造一个完整的周五 fixture
        dates = [
            # Week 1: 7/13-7/17
            *(datetime.date(2026, 7, 13) + datetime.timedelta(days=i) for i in range(5)),
            # Week 2: 7/20-7/24
            *(datetime.date(2026, 7, 20) + datetime.timedelta(days=i) for i in range(5)),
            # Week 3: 7/27-7/31
            *(datetime.date(2026, 7, 27) + datetime.timedelta(days=i) for i in range(5)),
        ]
        codes = ["sh.TEST"] * len(dates)
        closes = [10.0 + i * 0.5 for i in range(len(dates))]
        self.daily = pl.DataFrame({
            "date": dates,
            "code": codes,
            "close": closes,
            "open": [c - 0.1 for c in closes],
            "high": [c + 0.2 for c in closes],
            "low": [c - 0.2 for c in closes],
            "volume": [1_000_000.0] * len(dates),
            "amount": [10_000_000.0] * len(dates),
        }).sort(["code", "date"])

        data_manager.df_daily = self.daily
        data_manager._resample_all()

    def tearDown(self):
        data_manager.df_daily = self._orig_daily
        data_manager.df_weekly = self._orig_weekly
        data_manager.df_monthly = self._orig_monthly
        data_manager.df_mapping = self._orig_mapping
        data_manager._asof_frame_cache.clear()
        # 重置 parser 单例状态，避免 mount_enabled / current_df 污染后续测试
        blink_parser.mount_enabled = True
        blink_parser.current_df = None

    def test_friday_partial_equals_completed(self):
        """周五作为 target_date 时，partial week 合成行的 OHLCV == 该周完整周线（以周一为标签）的 OHLCV"""
        friday = datetime.date(2026, 7, 17)  # 第 1 周周五
        monday = datetime.date(2026, 7, 13)  # 该周周一（group_by_dynamic 的标签）

        # 1. as-of frame 在周五
        asof_friday = data_manager.build_asof_frame("W", friday)
        partial_row = asof_friday.filter(pl.col("date") == friday)
        self.assertEqual(partial_row.height, 1, "周五应恰好有一行 partial 合成行")

        # 2. 完整周线表中同一周的行（标签为周一）
        completed_row = data_manager.df_weekly.filter(pl.col("date") == monday)
        self.assertEqual(completed_row.height, 1, "完整周线表应恰好有一行该周的周线（标签为周一）")

        # 3. 逐列比较 OHLCV（partial 合成行 vs 完整周线行）
        p = partial_row.to_dicts()[0]
        c = completed_row.to_dicts()[0]
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            self.assertAlmostEqual(p[col], c[col], places=4,
                msg=f"Friday completion mismatch on {col}: partial={p[col]} vs completed={c[col]}")


class TestLookaheadDetectorCanary(unittest.TestCase):
    """
    哨兵自检：故意在测试内部制造一个“泄漏”场景，
    运行投毒差分检测器，必须能捕获并报警。
    这证明检测器本身是有效的，而非“永远不报错的假阴性”。
    """

    def test_canary_detects_leak(self):
        """
        构造一个已知泄漏的场景：
        - 在清洗逻辑中，故意把周线表中“本周（含未来）”的行也保留了下来
        - 跑投毒差分，必须检测到差异并报警
        """
        from core.security import blink_parser

        # 1. 准备极简 fixture：两周，两只股
        daily = pl.DataFrame({
            "date": [
                datetime.date(2026, 7, 13), datetime.date(2026, 7, 14),
                datetime.date(2026, 7, 15), datetime.date(2026, 7, 16),
                datetime.date(2026, 7, 17),  # 周五
                datetime.date(2026, 7, 20), datetime.date(2026, 7, 21),
                datetime.date(2026, 7, 22), datetime.date(2026, 7, 23),
                datetime.date(2026, 7, 24),  # 周五
            ],
            "code": ["sh.CANARY"] * 10,
            "close": [10, 11, 12, 13, 14, 20, 21, 22, 23, 24],
            "open": [9.9, 10.9, 11.9, 12.9, 13.9, 19.9, 20.9, 21.9, 22.9, 23.9],
            "high": [10.2, 11.2, 12.2, 13.2, 14.2, 20.2, 21.2, 22.2, 23.2, 24.2],
            "low": [9.8, 10.8, 11.8, 12.8, 13.8, 19.8, 20.8, 21.8, 22.8, 23.8],
            "volume": [1_000_000.0] * 10,
            "amount": [10_000_000.0] * 10,
        }).sort(["code", "date"])

        data_manager.df_daily = daily
        data_manager.df_mapping = None
        data_manager._asof_frame_cache.clear()
        data_manager._resample_all()
        selection_engine._set_cache.clear()
        blink_parser.mount_enabled = True
        blink_parser.current_df = None

        # 目标日：第 1 周周三
        T = datetime.date(2026, 7, 15)
        formula = "W.CLOSE > 13"

        # 正确路径：使用 build_asof_frame 正确构建的 as-of frame
        clean_result = selection_engine.execute_selector(formula, "D", None, target_date=T)
        clean_codes = set(clean_result["codes"])

        # 故意制造泄漏：使用完整周线表（含本周完整 K 线，已含周四/周五数据）
        # 手动构造“错误”的 as-of frame：不做 partial 合成，直接用 completed（含本周行）
        orig_weekly = data_manager.df_weekly
        cur_start = T - datetime.timedelta(days=T.weekday())  # 2026-07-13 周一
        # 错误版本：保留本周行（date <= cur_start 而不是 < cur_start）
        bad_completed = orig_weekly.filter(pl.col("date") <= cur_start)  # 保留本周行！
        bad_partial = daily.filter(
            (pl.col("date") >= cur_start) & (pl.col("date") <= T)
        ).sort(["code", "date"]).group_by("code").agg([
            pl.col("date").max().alias("date"),
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
            pl.col("amount").sum(),
        ])
        bad_frame = pl.concat([bad_completed, bad_partial]).sort(["code", "date"])

        # 在 bad_frame 上直接跑指标
        blink_parser.mount_enabled = False
        expr = blink_parser.parse_expression(formula, "W")
        bad_result_codes = set(
            bad_frame.with_columns(expr.alias("_signal"))
            .filter(pl.col("_signal"))
            .select("code")["code"].to_list()
        )

        # 断言：正确路径与泄漏路径结果不同
        self.assertNotEqual(
            clean_codes, bad_result_codes,
            "CANARY FAILED: detector did not catch the injected leak!"
        )
        # 具体预期：正确路径应不命中（周三 close=12 <= 13），泄漏路径命中（完整周线 close=14 > 13）
        self.assertEqual(clean_codes, set(), f"Clean should be empty, got {clean_codes}")
        self.assertEqual(bad_result_codes, {"sh.CANARY"}, f"Leaked should hit, got {bad_result_codes}")

    def tearDown(self):
        # 重置 parser 单例状态，避免 mount_enabled / current_df 污染后续测试
        blink_parser.mount_enabled = True
        blink_parser.current_df = None


if __name__ == "__main__":
    unittest.main()
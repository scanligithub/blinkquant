import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
import unittest
import polars as pl
from core.data_manager import data_manager
from core.engine import selection_engine
from core.indicator_registry import WINDOW_NAMES

class TestDerivation(unittest.TestCase):
    def test_indicator_map_derived_from_registry(self):
        self.assertEqual(set(data_manager.INDICATOR_MAP.keys()), set(WINDOW_NAMES))

    def test_indicator_map_funcs_are_callable(self):
        for name, fn in data_manager.INDICATOR_MAP.items():
            self.assertTrue(callable(fn), f"{name} not callable")

    def test_metric_pattern_matches_registered_funcs(self):
        self.assertTrue(selection_engine.metric_pattern.search("MA(CLOSE, 20)"), "MA should match")
        self.assertTrue(selection_engine.metric_pattern.search("ema(close, 12)"), "case-insensitive")
        self.assertFalse(selection_engine.metric_pattern.search("KDJ(CLOSE, 9)"), "unregistered should not match")

    def test_metric_pattern_covers_all_window_indicators(self):
        for name in WINDOW_NAMES:
            self.assertTrue(selection_engine.metric_pattern.search(f"{name}(CLOSE, 10)"), f"{name} missing from pattern")

class TestEnginePattern(unittest.TestCase):
    def test_pattern_matches_non_ohlcv_field(self):
        from core.indicator_registry import FIELDS
        self.assertTrue(selection_engine.metric_pattern.search("MA(PE_TTM, 5)"))

    def test_pattern_matches_all_window_indicators(self):
        from core.indicator_registry import WINDOW_NAMES
        for name in WINDOW_NAMES:
            self.assertTrue(selection_engine.metric_pattern.search(f"{name}(CLOSE, 10)"),
                            f"{name} missing from pattern")

    def test_pattern_does_not_match_non_window(self):
        # 非 window 算子（CROSS_UP 不在 WINDOW_NAMES）直接调用不匹配
        self.assertFalse(selection_engine.metric_pattern.search("CROSS_UP(CLOSE, 10)"))

    def test_pattern_matches_no_space_and_lowercase_field(self):
        self.assertTrue(selection_engine.metric_pattern.search("MA(CLOSE,20)"))
        self.assertTrue(selection_engine.metric_pattern.search("MA(pe_ttm, 5)"))
        self.assertFalse(selection_engine.metric_pattern.search("XEMA(CLOSE, 10)"))

    def test_pattern_extracts_inner_window_calls(self):
        # 金叉/COUNT 公式内层 MA 必须被提取（Hot-JIT 挂载依赖子串匹配）
        golden = selection_engine.metric_pattern.findall("CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))")
        self.assertEqual(golden, [("MA", "CLOSE", "20"), ("MA", "CLOSE", "60")])
        count = selection_engine.metric_pattern.findall("COUNT(CLOSE > MA(CLOSE, 20), 10) >= 7")
        self.assertEqual(count, [("MA", "CLOSE", "20")])

class TestExecuteSelectorDate(unittest.TestCase):
    """date 参数语义：默认最新交易日；指定日回退至 ≤ 指定日的最近交易日。"""

    def setUp(self):
        self._orig_daily = data_manager.df_daily
        self._orig_mapping = data_manager.df_mapping
        data_manager.df_mapping = None
        data_manager.df_daily = pl.DataFrame({
            "date": [datetime.date(2026, 8, 18), datetime.date(2026, 8, 19), datetime.date(2026, 8, 20)] * 2,
            "code": ["sh.600000"] * 3 + ["sz.000001"] * 3,
            "close": [10.0, 11.0, 12.0, 5.0, 5.0, 4.0],
        }).sort(["code", "date"])

    def tearDown(self):
        data_manager.df_daily = self._orig_daily
        data_manager.df_mapping = self._orig_mapping

    def _run(self, target_date=None):
        return selection_engine.execute_selector("CLOSE > 10", "D", None, target_date=target_date)

    def test_default_uses_latest_date(self):
        out = self._run()
        self.assertEqual(out["date"], "2026-08-20")
        self.assertEqual(out["codes"], ["sh.600000"])

    def test_exact_trading_day(self):
        # 2026-08-19 收盘 11.0 > 10 命中
        out = self._run(target_date=datetime.date(2026, 8, 19))
        self.assertEqual(out["date"], "2026-08-19")
        self.assertEqual(out["codes"], ["sh.600000"])

    def test_non_trading_day_falls_back_to_previous(self):
        # 8/22 是周末（数据中不存在）→ 回退到 8/20
        out = self._run(target_date=datetime.date(2026, 8, 22))
        self.assertEqual(out["date"], "2026-08-20")
        self.assertEqual(out["codes"], ["sh.600000"])

    def test_date_before_data_start_returns_error(self):
        out = self._run(target_date=datetime.date(2026, 7, 1))
        self.assertIn("error", out)
        self.assertIn("早于数据起点", out["error"])


class TestMultiTFWednesdayAntiLeak(unittest.TestCase):
    """Wednesday anti-leak regression test for MTF atoms.

    Verifies that W/M atoms reconstruct "completed bars + synthetic partial"
    from daily data up to target_date, never leaking future data.
    """

    def setUp(self):
        self._orig_daily = data_manager.df_daily
        self._orig_weekly = data_manager.df_weekly
        self._orig_monthly = data_manager.df_monthly
        self._orig_mapping = data_manager.df_mapping
        data_manager.df_mapping = None
        # Invalidate asof frame cache
        data_manager._asof_frame_cache.clear()

        # Synthetic daily data: 2026-08-17 (Mon) to 2026-08-21 (Fri)
        # Weekly bar: Mon-Fri, so completed week = 2026-08-10 to 2026-08-14
        # target_date = 2026-08-19 (Wed) => partial week = 2026-08-17 to 2026-08-19
        dates = [
            # Completed week: Mon-Fri (2026-08-10 to 2026-08-14)
            datetime.date(2026, 8, 10), datetime.date(2026, 8, 11),
            datetime.date(2026, 8, 12), datetime.date(2026, 8, 13),
            datetime.date(2026, 8, 14),
            # Current week: Mon-Wed (2026-08-17 to 2026-08-19)
            datetime.date(2026, 8, 17), datetime.date(2026, 8, 18),
            datetime.date(2026, 8, 19),
            # Thu-Fri (should NOT be visible on Wednesday)
            datetime.date(2026, 8, 20), datetime.date(2026, 8, 21),
        ]
        codes = ["sh.600000"] * len(dates)
        # Completed week close: 9.0 (Mon), 9.2, 9.4, 9.6, 9.8 (Fri)
        # Current week: 10.0 (Mon), 10.5 (Tue), 11.0 (Wed)
        # Thu: 15.0, Fri: 20.0 (should NOT leak)
        closes = [9.0, 9.2, 9.4, 9.6, 9.8, 10.0, 10.5, 11.0, 15.0, 20.0]

        self.df_daily = pl.DataFrame({
            "date": dates,
            "code": codes,
            "close": closes,
            "open": [c - 0.1 for c in closes],
            "high": [c + 0.2 for c in closes],
            "low": [c - 0.2 for c in closes],
            "volume": [1000000.0] * len(dates),
            "amount": [10000000.0] * len(dates),
        }).sort(["code", "date"])

        data_manager.df_daily = self.df_daily
        # Build weekly/monthly via resample
        data_manager._resample_all()

    def tearDown(self):
        data_manager.df_daily = self._orig_daily
        data_manager.df_weekly = self._orig_weekly
        data_manager.df_monthly = self._orig_monthly
        data_manager.df_mapping = self._orig_mapping
        data_manager._asof_frame_cache.clear()

    def test_wednesday_no_leak_w_atom(self):
        """W.CLOSE > 10 on Wednesday (2026-08-19) should only see partial week close=11.0, not Thu/Fri."""
        target_date = datetime.date(2026, 8, 19)  # Wednesday

        # Build as-of frame for weekly
        asof_w = data_manager.build_asof_frame("W", target_date)

        # Partial week should have one row per code with close=11.0 (last of Mon-Wed)
        partial_rows = asof_w.filter(pl.col("date") == datetime.date(2026, 8, 19))
        self.assertEqual(len(partial_rows), 1)
        self.assertAlmostEqual(partial_rows["close"][0], 11.0)

        # No Thu/Fri data should appear in the as-of frame
        dates_in_frame = asof_w["date"].to_list()
        for d in dates_in_frame:
            self.assertLessEqual(d, target_date)

    def test_wednesday_w_atom_correct_result(self):
        """W atom with W.CLOSE > 10 on Wednesday should correctly filter."""
        target_date = datetime.date(2026, 8, 19)
        result = selection_engine.execute_selector(
            "W.CLOSE > 10", "D", None, target_date=target_date
        )
        self.assertEqual(result["date"], target_date.isoformat())
        # On Wednesday, partial week close = 11.0 > 10 => sh.600000 should be in results
        self.assertIn("sh.600000", result["codes"])

    def test_wednesday_m_atom_no_leak(self):
        """M.CLOSE > 10 on Wednesday should only see data up to Wed, not Thu/Fri."""
        target_date = datetime.date(2026, 8, 19)
        asof_m = data_manager.build_asof_frame("M", target_date)

        # Monthly as-of should only include dates <= target_date
        dates_in_frame = asof_m["date"].to_list()
        for d in dates_in_frame:
            self.assertLessEqual(d, target_date)

    def test_set_cache_dedup(self):
        """Same atom evaluated twice should hit cache."""
        target_date = datetime.date(2026, 8, 19)
        result1 = selection_engine.execute_selector(
            "W.CLOSE > 10", "D", None, target_date=target_date
        )
        # Check that cache was populated
        self.assertGreater(len(selection_engine._set_cache), 0)
        # Second call should use cache
        result2 = selection_engine.execute_selector(
            "W.CLOSE > 10", "D", None, target_date=target_date
        )
        self.assertEqual(result1["codes"], result2["codes"])


if __name__ == "__main__":
    unittest.main()

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

if __name__ == "__main__":
    unittest.main()

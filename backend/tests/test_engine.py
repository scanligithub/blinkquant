import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
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

    def test_pattern_extracts_inner_window_calls(self):
        # 金叉/COUNT 公式内层 MA 必须被提取（Hot-JIT 挂载依赖子串匹配）
        golden = selection_engine.metric_pattern.findall("CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))")
        self.assertEqual(golden, [("MA", "CLOSE", "20"), ("MA", "CLOSE", "60")])
        count = selection_engine.metric_pattern.findall("COUNT(CLOSE > MA(CLOSE, 20), 10) >= 7")
        self.assertEqual(count, [("MA", "CLOSE", "20")])

if __name__ == "__main__":
    unittest.main()

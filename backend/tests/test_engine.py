import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from core.data_manager import data_manager
from core.engine import selection_engine
from core.indicator_registry import INDICATORS

class TestDerivation(unittest.TestCase):
    def test_indicator_map_derived_from_registry(self):
        self.assertEqual(set(data_manager.INDICATOR_MAP.keys()), set(INDICATORS.keys()))

    def test_indicator_map_funcs_are_callable(self):
        for name, fn in data_manager.INDICATOR_MAP.items():
            self.assertTrue(callable(fn), f"{name} not callable")

    def test_metric_pattern_matches_registered_funcs(self):
        self.assertTrue(selection_engine.metric_pattern.search("MA(CLOSE, 20)"), "MA should match")
        self.assertTrue(selection_engine.metric_pattern.search("ema(close, 12)"), "case-insensitive")
        self.assertFalse(selection_engine.metric_pattern.search("KDJ(CLOSE, 9)"), "unregistered should not match")

    def test_metric_pattern_covers_all_window_indicators(self):
        for name in INDICATORS:
            self.assertTrue(selection_engine.metric_pattern.search(f"{name}(CLOSE, 10)"), f"{name} missing from pattern")

if __name__ == "__main__":
    unittest.main()

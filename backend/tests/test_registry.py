import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from core.indicator_registry import (
    INDICATORS, FIELDS, UNITS, EXAMPLE_QUERIES, TIMEFRAMES,
    INDICATOR_FUNCS, INDICATOR_NAMES, nl_meta,
)

class TestRegistry(unittest.TestCase):
    def test_builtin_indicators_present(self):
        self.assertEqual(sorted(INDICATORS.keys()), ["EMA", "MA", "REF", "ROC", "STD"])

    def test_all_indicators_are_window_funcs(self):
        for name, entry in INDICATORS.items():
            self.assertTrue(entry.get("window"), f"{name} must be window=True")

    def test_fields_whitelist_nonempty_and_upper(self):
        self.assertTrue(FIELDS)
        for f in FIELDS:
            self.assertEqual(f, f.upper())

    def test_units_cover_all_fields(self):
        for f in FIELDS:
            self.assertIn(f, UNITS, f"unit missing for {f}")

    def test_indicator_funcs_derivation(self):
        self.assertEqual(set(INDICATOR_FUNCS.keys()), set(INDICATORS.keys()))
        self.assertEqual(sorted(INDICATOR_NAMES), ["EMA", "MA", "REF", "ROC", "STD"])

    def test_nl_meta_shape(self):
        meta = nl_meta()
        self.assertEqual(set(meta.keys()), {"fields", "indicators", "timeframes", "units", "example_queries"})
        self.assertEqual(meta["timeframes"], ["D", "W", "M"])
        self.assertEqual(meta["indicators"], INDICATOR_NAMES)
        self.assertEqual(meta["fields"], FIELDS)
        self.assertEqual(meta["example_queries"], EXAMPLE_QUERIES)

if __name__ == "__main__":
    unittest.main()

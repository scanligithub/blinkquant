import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
import polars as pl
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

    def test_lambdas_partition_by_code(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01"],
            "code": ["sh.600000", "sh.600000", "sh.600000", "sz.000001"],
            "close": [10.0, 11.0, 12.0, 100.0],
        })
        ma = INDICATORS["MA"]["func"](pl.col("close"), 2)
        got = df.with_columns(ma.alias("m")).select("code", "date", "m").sort(["code", "date"])
        # sh.600000 第2行=10.5、第3行=11.5；sz.000001 第1行 null（窗口不足，跨代码混算会得到 55）
        self.assertEqual(got.row(1)[2], 10.5)
        self.assertEqual(got.row(2)[2], 11.5)
        self.assertIsNone(got.row(3)[2])

if __name__ == "__main__":
    unittest.main()

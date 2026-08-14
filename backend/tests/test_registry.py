import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
import polars as pl
from core.indicator_registry import (
    INDICATORS, FIELDS, UNITS, EXAMPLE_QUERIES, TIMEFRAMES,
    INDICATOR_FUNCS, INDICATOR_NAMES, WINDOW_NAMES, nl_meta,
)


def meta_signatures_set():
    return set(nl_meta()["signatures"].keys())


class TestRegistry(unittest.TestCase):
    def test_builtin_indicators_present(self):
        self.assertEqual(sorted(INDICATORS.keys()), [
            "ABS", "ATR", "BARSLAST", "BOLL_LOWER", "BOLL_UPPER",
            "COUNT", "CROSS_DOWN", "CROSS_UP", "EMA", "HHV", "KDJ_D",
            "KDJ_K", "LLV", "MA", "MAX", "MIN", "REF", "ROC", "RSI",
            "STD", "SUM",
        ])

    def test_window_indicators_are_window_funcs(self):
        for name, entry in INDICATORS.items():
            if entry.get("window"):
                self.assertEqual(entry["signature"], ["field", "pos_int"],
                                 f"{name} window func must have [field,pos_int] signature")
            else:
                self.assertFalse(entry.get("window"), f"{name} must be window=False")

    def test_all_entries_have_signature(self):
        for name, entry in INDICATORS.items():
            self.assertIn("signature", entry, f"{name} missing signature")
            self.assertIn("func", entry, f"{name} missing func")

    def test_windows_matched_between_map_and_registry(self):
        self.assertEqual(set(WINDOW_NAMES), set(INDICATOR_FUNCS.keys()))
        for name in WINDOW_NAMES:
            self.assertTrue(INDICATORS[name]["window"], f"{name} should be window=True")

    def test_fields_whitelist_nonempty_and_upper(self):
        self.assertTrue(FIELDS)
        for f in FIELDS:
            self.assertEqual(f, f.upper())

    def test_units_cover_all_fields(self):
        for f in FIELDS:
            self.assertIn(f, UNITS, f"unit missing for {f}")

    def test_indicator_funcs_derivation(self):
        self.assertEqual(sorted(WINDOW_NAMES), [
            "EMA", "HHV", "LLV", "MA", "REF", "ROC", "STD", "SUM",
        ])
        self.assertEqual(sorted(INDICATOR_NAMES), sorted(INDICATORS.keys()))
        self.assertEqual(meta_signatures_set(), set(INDICATOR_NAMES))

    def test_nl_meta_shape(self):
        meta = nl_meta()
        self.assertEqual(set(meta.keys()), {
            "fields", "indicators", "timeframes", "units", "example_queries",
            "signatures", "descriptions",
        })
        self.assertEqual(meta["timeframes"], ["D", "W", "M"])
        self.assertEqual(meta["indicators"], INDICATOR_NAMES)
        self.assertEqual(meta["fields"], FIELDS)
        self.assertEqual(meta["example_queries"], EXAMPLE_QUERIES)
        self.assertEqual(set(meta["signatures"].keys()), set(INDICATOR_NAMES))
        self.assertEqual(set(meta["descriptions"].keys()), set(INDICATOR_NAMES))

    def test_signatures_match_expected(self):
        expect = {
            "MA": ["field", "pos_int"], "EMA": ["field", "pos_int"],
            "STD": ["field", "pos_int"], "ROC": ["field", "pos_int"],
            "REF": ["field", "pos_int"], "HHV": ["field", "pos_int"],
            "LLV": ["field", "pos_int"], "SUM": ["field", "pos_int"],
            "CROSS_UP": ["series", "series"], "CROSS_DOWN": ["series", "series"],
            "MAX": ["series", "series"], "MIN": ["series", "series"],
            "ABS": ["series"], "COUNT": ["cond", "pos_int"], "BARSLAST": ["cond"],
        }
        got = {name: entry["signature"] for name, entry in INDICATORS.items()}
        self.assertEqual(got, expect)

    def test_new_indicators_signatures(self):
        expect = {
            "ATR": ["pos_int"],
            "RSI": ["series", "pos_int"],
            "BOLL_UPPER": ["series", "pos_int", "pos_int"],
            "BOLL_LOWER": ["series", "pos_int", "pos_int"],
            "KDJ_K": ["pos_int", "pos_int"],
            "KDJ_D": ["pos_int", "pos_int"],
        }
        got = {name: entry["signature"] for name, entry in INDICATORS.items() if name in expect}
        self.assertEqual(got, expect)
        for name in expect:
            self.assertFalse(INDICATORS[name]["window"], f"{name} must be window=False")
            self.assertIn("func", INDICATORS[name])

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

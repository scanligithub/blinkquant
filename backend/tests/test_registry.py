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
            "KDJ_K", "LLV", "MA", "MACD_DEA", "MACD_DIF", "MACD_HIST",
            "MAX", "MIN", "REF", "ROC", "RSI", "STD", "SUM",
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
            "ATR": ["pos_int"], "RSI": ["series", "pos_int"],
            "BOLL_UPPER": ["series", "pos_int", "pos_int"], "BOLL_LOWER": ["series", "pos_int", "pos_int"],
            "KDJ_K": ["pos_int", "pos_int"], "KDJ_D": ["pos_int", "pos_int"],
            "MACD_DIF": ["pos_int", "pos_int"], "MACD_DEA": ["pos_int", "pos_int", "pos_int"],
            "MACD_HIST": ["pos_int", "pos_int", "pos_int"],
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

    def test_macd_math_matches_definition(self):
        # 单代码长序列，验证 MACD 的定义式：DIF=EMA12-EMA26, HIST=2*(DIF-DEA), DEA=EMA(DIF,9)
        closes = [10 + i * 0.3 + ((i * 7) % 5) * 0.1 for i in range(40)]
        df = pl.DataFrame({
            "date": [f"2024-01-{i+1:02d}" for i in range(40)],
            "code": ["sh.600000"] * 40,
            "close": closes,
        })
        c = pl.col("close")
        ema12 = INDICATORS["EMA"]["func"](c, 12)
        ema26 = INDICATORS["EMA"]["func"](c, 26)
        dif = INDICATORS["MACD_DIF"]["func"](12, 26)
        dea = INDICATORS["MACD_DEA"]["func"](12, 26, 9)
        hist = INDICATORS["MACD_HIST"]["func"](12, 26, 9)
        got = df.with_columns(
            ema12.alias("e12"), ema26.alias("e26"),
            dif.alias("dif"), dea.alias("dea"), hist.alias("hist"),
        ).select(["date", "dif", "dea", "hist", "e12", "e26"]).sort(["date"])
        # 只对收敛稳定区（最后 10 行）断言，避开 ewm 起点效应
        rows = list(got.iter_rows())
        for date, dif_v, dea_v, hist_v, e12, e26 in rows[-10:]:
            self.assertIsNotNone(dif_v)
            self.assertAlmostEqual(dif_v, e12 - e26, places=6)
            self.assertAlmostEqual(hist_v, 2.0 * (dif_v - dea_v), places=6)
        # 最后一行 DEA 应等于 DIF 的 9 期 ewm（用 polars 原生 ewm 复算验证定义）
        last = got.tail(1).select(["dea"]).row(0)[0]
        dea_ref = df.with_columns(
            dif.alias("dif")
        ).select(pl.col("dif").ewm_mean(span=9, adjust=False).over("code").alias("dref")).tail(1).row(0)[0]
        self.assertAlmostEqual(last, dea_ref, places=6)

    def test_macd_partitions_by_code(self):
        # 多代码混排：DIF 用 .over("code") 分组。第二支为常数序列时 DIF 应为 0；
        # 若跨代码串算（带入第一支的动态 close）则会得到非零动态值，据此判定未串算。
        df = pl.DataFrame({
            "date": [f"2024-01-{i+1:02d}" for i in range(35)] + [f"2024-01-{i+1:02d}" for i in range(35)],
            "code": ["sh.600000"] * 35 + ["sz.000001"] * 35,
            "close": [10 + i * 0.3 for i in range(35)] + [100.0] * 35,
        })
        dif = INDICATORS["MACD_DIF"]["func"](12, 26)
        got = df.with_columns(dif.alias("dif")).select(["code", "date", "dif"]).sort(["code", "date"])
        rows = got.iter_rows()
        sh = [v for code, _, v in rows if code == "sh.600000"]
        sz = [v for code, _, v in rows if code == "sz.000001"]
        # 第一支：有动态变化，DIF 不应恒为 0（避免空实现假通过）
        self.assertNotEqual(max(sh), min(sh))
        # 第二支：常数序列，DIF 恒为 0（每个 code 有一份自身 ewm，未跨代码串算）
        for v in sz:
            self.assertAlmostEqual(v, 0.0, places=6)

if __name__ == "__main__":
    unittest.main()

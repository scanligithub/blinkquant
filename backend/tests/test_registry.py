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
            "ABS", "AROON_DOWN", "AROON_UP", "ATR", "BARSLAST", "BBI",
            "BIAS", "BOLL_LOWER", "BOLL_MID", "BOLL_UPPER", "CCI",
            "COUNT", "CR", "CROSS_DOWN", "CROSS_UP", "DEMA", "DMI_ADX",
            "DMI_MDI", "DMI_PDI", "EMA", "HHV", "KDJ_D", "KDJ_J",
            "KDJ_K", "LLV", "MA", "MACD_DEA", "MACD_DIF", "MACD_HIST",
            "MAX", "MFI", "MIN", "OBV", "PPO", "PSY", "REF", "ROC",
            "RSI", "SAR", "STD", "SUM", "TEMA", "TRIX", "UO", "VR",
            "VWAP", "WR",
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
            "DMI_PDI": ["pos_int"], "DMI_MDI": ["pos_int"], "DMI_ADX": ["pos_int"],
            "OBV": [], "CCI": ["pos_int"], "WR": ["pos_int"], "MFI": ["pos_int"],
            "SAR": [], "AROON_UP": ["pos_int"], "AROON_DOWN": ["pos_int"],
            "TRIX": ["pos_int"], "BBI": [], "VWAP": ["pos_int"], "BIAS": ["pos_int"],
            "KDJ_J": ["pos_int", "pos_int"], "BOLL_MID": ["series", "pos_int"],
            "PPO": ["pos_int", "pos_int"], "DEMA": ["series", "pos_int"],
            "TEMA": ["series", "pos_int"], "UO": [], "VR": ["pos_int"],
            "PSY": ["pos_int"], "CR": ["pos_int"],
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

    def test_obv_matches_definition(self):
        # OBV = 累计带符号量：收涨 +vol、收跌 -vol、平 0
        closes = [10.0, 11.0, 9.0, 9.0, 12.0]
        vols = [100.0, 200.0, 300.0, 400.0, 500.0]
        df = pl.DataFrame({
            "code": ["sh.600000"] * 5,
            "close": closes,
            "volume": vols,
        })
        obv = INDICATORS["OBV"]["func"]()
        got = df.with_columns(obv.alias("o")).select("o").to_series().to_list()
        # 首行无前值 → 0；随后 +200、-300、+0、+500 累积 → [0, 200, -100, -100, 400]
        expected = [0.0, 200.0, -100.0, -100.0, 400.0]
        for g, e in zip(got, expected):
            self.assertAlmostEqual(g, e, places=6)

    def test_obv_partitions_by_code(self):
        # 两代码混排，常数序列第二支 OBV 应恒为 0（不跨 code 串算）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4 + ["sz.000001"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0] + [100.0, 100.0, 100.0, 100.0],
            "volume": [1.0, 2.0, 3.0, 4.0] + [10.0, 20.0, 30.0, 40.0],
        })
        obv = INDICATORS["OBV"]["func"]()
        got = df.with_columns(obv.alias("o")).select(["code", "o"]).sort(["code"])
        rows = list(got.iter_rows())
        sz = [v for c, v in rows if c == "sz.000001"]
        self.assertEqual(len(sz), 4)
        for v in sz:
            self.assertAlmostEqual(v, 0.0, places=6)

    def test_vwap_definition(self):
        # VWAP(2) = SUM(C*VOL,2)/SUM(VOL,2)
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4,
            "close": [10.0, 12.0, 14.0, 16.0],
            "volume": [100.0, 100.0, 200.0, 100.0],
        })
        vwap = INDICATORS["VWAP"]["func"](2)
        got = df.with_columns(vwap.alias("v")).select("v").to_series().to_list()
        # 第2行: (10*100+12*100)/200=11.0; 第3行: (12*100+14*200)/300=13.333...
        self.assertAlmostEqual(got[1], 11.0, places=6)
        self.assertAlmostEqual(got[2], 40.0 / 3.0, places=6)

    def test_wr_definition(self):
        # WR(2) = (HHV(H,2)-C)/(HHV(H,2)-LLV(L,2))*100
        df = pl.DataFrame({
            "code": ["sh.600000"] * 3,
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 8.0],
            "close": [10.5, 11.5, 9.0],
        })
        wr = INDICATORS["WR"]["func"](2)
        got = df.with_columns(wr.alias("w")).select("w").to_series().to_list()
        # 第3行: HHV=13, LLV=8, C=9 → (13-9)/(13-8)*100 = 80.0
        self.assertAlmostEqual(got[2], 80.0, places=6)

    def test_sar_iterates_partitioned(self):
        # SAR 迭代不跨 code 串算：第二支全部低点在首日，SAR 应恒为该 low
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4 + ["sz.000001"] * 4,
            "open": [10.0, 10.5, 11.0, 11.5] + [50.0, 51.0, 52.0, 53.0],
            "high": [10.5, 11.0, 11.5, 12.0] + [52.0, 53.0, 54.0, 55.0],
            "low": [9.5, 10.0, 10.5, 11.0] + [49.0, 50.0, 51.0, 52.0],
            "close": [10.0, 10.5, 11.0, 11.5] + [50.0, 51.0, 52.0, 53.0],
        })
        sar = INDICATORS["SAR"]["func"]()
        got = df.with_columns(sar.alias("s")).select(["code", "s"]).sort(["code"])
        rows = list(got.iter_rows())
        sz = [v for c, v in rows if c == "sz.000001"]
        self.assertEqual(len(sz), 4)
        # 精确值（独立参照实现实测）：单调上行、L0=49 → [49, 50, 50, 50.08]；
        # 若跨 code 泄漏应为 [10.1, 12.6, 15.9, 19.7]，可判别真
        self.assertEqual([round(v, 2) for v in sz], [49.0, 50.0, 50.0, 50.08])

    def test_dmi_pdi_positive_trend(self):
        # 单调上行趋势下 DMI_PDI 应显著大于 DMI_MDI（PDI>MDI）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 20,
            "high": [10.0 + i * 0.5 for i in range(20)],
            "low": [9.5 + i * 0.5 for i in range(20)],
            "close": [9.8 + i * 0.5 for i in range(20)],
        })
        pdi = INDICATORS["DMI_PDI"]["func"](5)
        mdi = INDICATORS["DMI_MDI"]["func"](5)
        got = df.with_columns(pdi.alias("p"), mdi.alias("m")).select(["p", "m"])
        last = got.tail(1).row(0)
        self.assertGreater(last[0], last[1])
        self.assertGreaterEqual(last[0], 0.0)

    def test_aroon_up_high_at_window(self):
        # 窗口内今天创新高时 AROON_UP 应为 100；max 距今天 1 根时应为 80（判别恒 100 假实现）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 6,
            "high": [10.0, 11.0, 12.0, 13.0, 13.0, 12.0],
            "low": [9.0, 10.0, 11.0, 12.0, 12.0, 11.0],
        })
        up = INDICATORS["AROON_UP"]["func"](5)
        got = df.with_columns(up.alias("u")).select("u").to_series().to_list()
        self.assertAlmostEqual(got[4], 100.0, places=6)
        self.assertAlmostEqual(got[5], 80.0, places=6)

    def test_dmi_adx_not_nan_in_trend(self):
        # 回归：修复前 DMI_ADX 因首行 0/0=NaN 被 ewm 全串传染为 NaN
        df = pl.DataFrame({
            "code": ["sh.600000"] * 20,
            "high": [10.0 + i * 0.5 for i in range(20)],
            "low": [9.5 + i * 0.5 for i in range(20)],
            "close": [9.8 + i * 0.5 for i in range(20)],
        })
        adx = INDICATORS["DMI_ADX"]["func"](5)
        got = df.with_columns(adx.alias("a")).select("a").to_series().to_list()
        for v in got:
            self.assertIsNotNone(v)
            self.assertFalse(v != v, f"ADX must not be NaN, got {v}")
        self.assertGreater(got[-1], 0.0)

    def test_aroon_down_monotonic_rise(self):
        # 回归：修复前单调上行序列 AROON_DOWN 全为 null；窗口低点恒为窗口首根（min_periods=1 保证首根锚点成立）
        # 锚点仅在第 0 根触发 → barslast=期数差，第 5 根适配衰减为 0.0（TDX 语义一致）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 6,
            "high": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "low": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        })
        down = INDICATORS["AROON_DOWN"]["func"](5)
        got = df.with_columns(down.alias("d")).select("d").to_series().to_list()
        # 实测 [100, 80, 60, 40, 20, 0]：第 5 根 0.0（非修复前全 null 的 None）
        expected = [100.0, 80.0, 60.0, 40.0, 20.0, 0.0]
        for g, e in zip(got, expected):
            self.assertAlmostEqual(g, e, places=6)

if __name__ == "__main__":
    unittest.main()

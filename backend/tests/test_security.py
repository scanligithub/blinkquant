import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ast
import unittest
import polars as pl
from core.security import blink_parser, _require_whitelist_field, _require_positive_int, _split_arith_top_level, _strip_outer_parens

def parse_call(src):
    return ast.parse(src, mode="eval").body

class TestRequireHelpers(unittest.TestCase):
    def test_field_ok(self):
        self.assertEqual(_require_whitelist_field(parse_call("CLOSE")), "CLOSE")

    def test_field_rejects_unknown(self):
        with self.assertRaises(ValueError):
            _require_whitelist_field(parse_call("NOPE"))

    def test_field_rejects_non_name(self):
        with self.assertRaises(ValueError):
            _require_whitelist_field(parse_call("123"))

    def test_positive_int_ok(self):
        self.assertEqual(_require_positive_int(parse_call("20")), 20)

    def test_positive_int_rejects_zero(self):
        with self.assertRaises(ValueError):
            _require_positive_int(parse_call("0"))

    def test_positive_int_rejects_negative(self):
        with self.assertRaises(ValueError):
            _require_positive_int(parse_call("-5"))

    def test_positive_int_rejects_non_int(self):
        with self.assertRaises(ValueError):
            _require_positive_int(parse_call("2.5"))

    def test_positive_int_rejects_non_constant(self):
        with self.assertRaises(ValueError):
            _require_positive_int(parse_call("X"))

    def test_bool_window_rejected(self):
        with self.assertRaises(ValueError):
            _require_positive_int(parse_call("True"))

class TestCallBranch(unittest.TestCase):
    def test_unknown_function_rejected(self):
        node = parse_call("KDJ(CLOSE, 9)")
        with self.assertRaises(ValueError):
            blink_parser._visit(node)

    def test_call_non_name_func_rejected(self):
        node = parse_call("foo.bar(CLOSE, 2)")
        with self.assertRaises(ValueError):
            blink_parser._visit(node)

    def test_non_whitelist_field_rejected(self):
        node = parse_call("MA(NOPE, 20)")
        with self.assertRaises(ValueError):
            blink_parser._visit(node)

    def test_negative_window_rejected(self):
        node = parse_call("MA(CLOSE, -5)")
        with self.assertRaises(ValueError):
            blink_parser._visit(node)

    def test_fast_path_returns_mounted_column(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "code": ["sh.600000"] * 3,
            "close": [10.0, 11.0, 12.0],
            "MA_CLOSE_20": [1.0, 1.0, 1.0],
        })
        blink_parser.current_df = df
        node = parse_call("MA(CLOSE, 20)")
        expr = blink_parser._visit(node)
        self.assertEqual(str(expr), str(pl.col("MA_CLOSE_20")))

    def test_slow_path_computes_when_not_mounted(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "code": ["sh.600000"] * 3,
            "close": [10.0, 11.0, 12.0],
        })
        blink_parser.current_df = df
        node = parse_call("MA(CLOSE, 2)")
        expr = blink_parser._visit(node)
        result = df.with_columns(expr.alias("m")).select(pl.col("m")).to_series().to_list()
        self.assertEqual(result, [None, 10.5, 11.5])

    def test_fields_match_registry(self):
        from core.indicator_registry import FIELDS
        self.assertEqual(set(blink_parser.fields.keys()), set(FIELDS))

    def test_uppercase_and_normalized(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "code": ["sh.600000"] * 2,
            "close": [10.0, 12.0],
            "peTTM": [10.0, 25.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CLOSE > 11 AND PE_TTM < 30", "D")
        got = df.with_columns(expr.alias("s")).select(pl.col("s")).to_series().to_list()
        self.assertEqual(got, [False, True])

    def test_uppercase_or_normalized(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "code": ["sh.600000"] * 2,
            "close": [10.0, 12.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CLOSE < 11 OR CLOSE > 11", "D")
        got = df.with_columns(expr.alias("s")).select(pl.col("s")).to_series().to_list()
        self.assertEqual(got, [True, True])

    def test_double_space_and_normalized(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "code": ["sh.600000"] * 2,
            "close": [10.0, 12.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CLOSE < 11  AND  CLOSE > 10", "D")
        got = df.with_columns(expr.alias("s")).select(pl.col("s")).to_series().to_list()
        self.assertEqual(got, [False, False])

class TestSignatureRecursion(unittest.TestCase):
    def setUp(self):
        self.df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "code": ["sh.600000"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0],
            "open": [9.0, 10.5, 11.5, 12.5],
        })
        blink_parser.current_df = self.df

    def eval_expr(self, expr):
        return blink_parser.parse_expression(expr, "D")

    def test_cross_up_detects_golden_cross(self):
        expr = self.eval_expr("CROSS_UP(MA(CLOSE, 2), MA(OPEN, 2))")
        got = self.df.with_columns(expr.alias("s"))["s"].to_list()
        # close 10,11,12,13 → MA2 = [null, 10.5, 11.5, 12.5]
        # open 9,10.5,11.5,12.5 → MA2 = [null, 9.75, 11.0, 12.0]
        # 首行 shift(1) 无昨日值 → None；第3日 11.5>11.0 且 10.5<=9.75? 否 → F；第4日 12.5>12.0 且 11.5<=11.0? 否 → F
        self.assertEqual(got, [None, None, False, False])

    def test_cross_up_true_on_cross(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "code": ["sh.600000"] * 4,
            "close": [10.0, 12.0, 11.0, 14.0],
            "open": [9.0, 11.0, 12.0, 13.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CROSS_UP(MA(CLOSE, 1), MA(OPEN, 1))", "D")
        got = df.with_columns(expr.alias("s"))["s"].to_list()
        # MA1 = 原值。第4日: 14>13 且 11<=12? 是 → T
        self.assertEqual(got, [None, False, False, True])

    def test_cross_down_true(self):
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "code": ["sh.600000"] * 4,
            "close": [10.0, 9.0, 11.0, 8.0],
            "open": [11.0, 10.0, 10.0, 12.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CROSS_DOWN(CLOSE, OPEN)", "D")
        got = df.with_columns(expr.alias("s"))["s"].to_list()
        # 第4日: 8<12 且 11>=10? 是 → T
        self.assertEqual(got, [None, False, False, True])

    def test_count_counts_true_windows(self):
        expr = self.eval_expr("COUNT(CLOSE > 11, 3)")
        got = self.df.with_columns(expr.alias("c"))["c"].to_list()
        # close 10,11,12,13；>11 为 F,F,T,T → rolling_sum(3) = [null, null, 1, 2]
        self.assertEqual(got, [None, None, 1, 2])

    def test_count_and_or_cond(self):
        expr = self.eval_expr("COUNT(CLOSE > 10 AND OPEN < 11, 3)")
        got = self.df.with_columns(expr.alias("c"))["c"].to_list()
        # AND: F,T,F,F → rolling_sum(3) = [null, null, 1, 1]
        self.assertEqual(got, [None, None, 1, 1])

    def test_barslast(self):
        expr = self.eval_expr("BARSLAST(CLOSE > 10)")
        got = self.df.with_columns(expr.alias("b"))["b"].to_list()
        # close 10,11,12,13；>10: F,T,T,T → [null, 0, 0, 0]
        self.assertEqual(got, [None, 0, 0, 0])

    def test_cross_nested_series(self):
        expr = self.eval_expr("CROSS_UP(MA(CLOSE, 2), MA(OPEN, 2))")
        self.assertIsNotNone(expr)

    def test_series_second_level_nesting_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("CROSS_UP(MA(MA(CLOSE, 2), 2), OPEN)")

    def test_unknown_function_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("KDJ(CLOSE, 9) > 50")

    def test_window_upper_limit_500(self):
        with self.assertRaises(ValueError):
            self.eval_expr("MA(CLOSE, 501) > 0")

    def test_count_cond_nested_count_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("COUNT(COUNT(CLOSE > 10, 2) > 1, 3)")

    def test_count_cond_bad_op_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("COUNT(CLOSE == 10, 3)")

    def test_count_cond_constant_both_sides_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("COUNT(5 > 3, 3)")

    def test_count_cond_window_operand(self):
        expr = self.eval_expr("COUNT(CLOSE > MA(CLOSE, 2), 3)")
        got = self.df.with_columns(expr.alias("c"))["c"].to_list()
        # MA2 = [null, 10.5, 11.5, 12.5]；close > MA2 = [null, T, T, T]
        # rolling_sum(3) = [null, null, null, 3]
        self.assertEqual(got, [None, None, None, 3])

    def test_max_min_abs(self):
        expr = self.eval_expr("MAX(CLOSE, OPEN)")
        got = self.df.with_columns(expr.alias("m"))["m"].to_list()
        self.assertEqual(got, [10.0, 11.0, 12.0, 13.0])
        expr = self.eval_expr("MIN(CLOSE, OPEN)")
        got = self.df.with_columns(expr.alias("m"))["m"].to_list()
        self.assertEqual(got, [9.0, 10.5, 11.5, 12.5])
        expr = self.eval_expr("ABS(MA(CLOSE, 2))")
        got = self.df.with_columns(expr.alias("a"))["a"].to_list()
        # MA2 = [null, 10.5, 11.5, 12.5] → 绝对值不变
        self.assertEqual(got, [None, 10.5, 11.5, 12.5])

class TestKDJATRRSIBoll(unittest.TestCase):
    def setUp(self):
        self.df6 = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"],
            "code": ["sh.600000"] * 6,
            "close": [10.0, 10.5, 10.3, 11.0, 11.8, 12.1],
            "open": [9.9, 10.4, 10.6, 10.2, 11.2, 11.9],
            "high": [10.2, 10.8, 10.7, 11.2, 12.0, 12.3],
            "low": [9.8, 10.2, 10.1, 10.8, 11.4, 11.7],
        })
        self.df12 = pl.DataFrame({
            "date": ["2024-0%d-01" % i for i in range(1, 10)] + ["2024-10-01", "2024-11-01", "2024-12-01"],
            "code": ["sh.600000"] * 12,
            "close": [10.0, 10.5, 10.3, 11.0, 11.8, 12.1, 11.5, 11.0, 12.0, 13.0, 13.5, 14.0],
            "open": [9.9, 10.4, 10.6, 10.2, 11.2, 11.9, 11.7, 11.3, 11.8, 12.5, 13.2, 13.8],
            "high": [10.2, 10.8, 10.7, 11.2, 12.0, 12.3, 11.9, 11.4, 12.4, 13.4, 13.9, 14.4],
            "low": [9.8, 10.2, 10.1, 10.8, 11.4, 11.7, 11.0, 10.7, 11.6, 12.6, 13.1, 13.6],
        })

    def eval_df(self, expr, df):
        blink_parser.current_df = df
        return blink_parser.parse_expression(expr, "D")

    def values(self, expr, df):
        return df.with_columns(expr.alias("v")).select("v").to_series().to_list()

    def test_atr(self):
        got = self.values(self.eval_df("ATR(2)", self.df6), self.df6)
        self.assertEqual(len(got), 6)
        self.assertIsNone(got[0])
        self.assertAlmostEqual(got[1], 0.6, places=3)
        self.assertAlmostEqual(got[2], 0.7, places=3)
        self.assertAlmostEqual(got[3], 0.75, places=3)
        self.assertAlmostEqual(got[4], 0.95, places=3)
        self.assertAlmostEqual(got[5], 0.8, places=3)

    def test_rsi(self):
        got = self.values(self.eval_df("RSI(CLOSE, 2)", self.df6), self.df6)
        self.assertEqual(len(got), 6)
        self.assertIsNone(got[0])
        self.assertIsNone(got[1])
        self.assertAlmostEqual(got[2], 71.4286, places=3)
        self.assertAlmostEqual(got[3], 77.7778, places=3)
        self.assertAlmostEqual(got[4], 100, places=3)
        self.assertAlmostEqual(got[5], 100, places=3)

    def test_boll_band(self):
        up = self.values(self.eval_df("BOLL_UPPER(CLOSE, 2, 2)", self.df6), self.df6)
        low = self.values(self.eval_df("BOLL_LOWER(CLOSE, 2, 2)", self.df6), self.df6)
        # MA(2)=10.4, STD=0.1414 → upper=10.6828, lower=10.1172
        self.assertIsNone(up[0])
        self.assertIsNone(low[0])
        self.assertAlmostEqual(up[2], 10.6828, places=3)
        self.assertAlmostEqual(low[2], 10.1172, places=3)

    def test_kdj_k_d(self):
        k = self.values(self.eval_df("KDJ_K(2, 2)", self.df12), self.df12)
        d = self.values(self.eval_df("KDJ_D(2, 2)", self.df12), self.df12)
        self.assertIsNone(k[0])
        self.assertIsNone(k[1])
        self.assertAlmostEqual(k[2], 49.2857, places=3)
        self.assertAlmostEqual(k[11], 69.2308, places=3)
        self.assertIsNone(d[0])
        self.assertIsNone(d[2])
        self.assertAlmostEqual(d[3], 52.2403, places=3)
        self.assertAlmostEqual(d[11], 71.3675, places=3)

    def test_kdj_golden_cross_parses(self):
        expr = self.eval_df("CROSS_UP(KDJ_K(2, 2), KDJ_D(2, 2))", self.df12)
        self.assertIsNotNone(expr)
        rows = self.values(expr, self.df12)
        self.assertEqual(len(rows), 12)

    def test_rsi_cross_nested(self):
        expr = self.eval_df("CROSS_UP(RSI(CLOSE, 2), RSI(CLOSE, 4))", self.df6)
        self.assertIsNotNone(expr)

    def test_count_rsi_cond(self):
        got = self.values(self.eval_df("COUNT(RSI(CLOSE, 2) > 70, 2)", self.df6), self.df6)
        # RSI(2)=[None,None,71.43,77.78,100,100]; >70 掩码 [N,N,T,T,T,T]; rolling_sum(2)=[N,N,N,2,2,2]
        self.assertEqual(got, [None, None, None, 2, 2, 2])

    def test_max_two_level_allowed(self):
        expr = self.eval_df("MAX(MAX(CLOSE, OPEN), OPEN)", self.df6)
        got = self.values(expr, self.df6)
        self.assertEqual(got, [10.0, 10.5, 10.6, 11.0, 11.8, 12.1])

    def test_deep_nesting_still_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_df("CROSS_UP(MA(MA(CLOSE, 2), 2), OPEN)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("COUNT(COUNT(CLOSE > 10, 2) > 1, 3)", self.df6)

    def test_pos_int_bounds(self):
        with self.assertRaises(ValueError):
            self.eval_df("ATR(0)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("ATR(501)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("BOLL_UPPER(CLOSE, 20, 501)", self.df6)

class TestSeriesArithmetic(unittest.TestCase):
    def setUp(self):
        self.df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "code": ["sh.600000"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0],
            "open": [9.0, 10.5, 11.5, 12.5],
            "high": [10.5, 11.5, 12.5, 13.5],
            "low": [9.5, 10.5, 11.5, 12.5],
        })
        blink_parser.current_df = self.df

    def eval_expr(self, expr):
        return blink_parser.parse_expression(expr, "D")

    def values(self, expr):
        return self.df.with_columns(expr.alias("v")).select("v").to_series().to_list()

    def test_abs_ref_diff_passes(self):
        expr = self.eval_expr("ABS(REF(CLOSE, 1) - REF(CLOSE, 2))")
        got = self.values(expr)
        # REF1: [null,10,11,12]  REF2: [null,null,10,11]  差: [null,null,1,1]
        self.assertEqual(got, [None, None, 1.0, 1.0])

    def test_paren_division_cond_passes(self):
        expr = self.eval_expr("(CLOSE - OPEN) / CLOSE > 0.05")
        got = self.values(expr)
        # day1: (10-9)/10=0.10>0.05 T; day2: (11-10.5)/11=0.0455 F; day3: 0.0417 F; day4: (13-12.5)/13=0.0385 F
        self.assertEqual(got, [True, False, False, False])

    def test_constant_mult_cond_passes(self):
        expr = self.eval_expr("CLOSE * 1.1 > REF(CLOSE, 1)")
        got = self.values(expr)
        # day2: 11*1.1=12.1>10 T; day3: 12*1.1=13.2>11 T; day4: 13*1.1=14.3>12 T
        self.assertEqual(got, [None, True, True, True])

    def test_top_level_too_many_ops_rejected(self):
        # 顶层算术前端/后端均不校验，须包进 series 位置触发 _require_series → _require_arith
        with self.assertRaises(ValueError):
            self.eval_expr("ABS(CLOSE / CLOSE / CLOSE / CLOSE / CLOSE)")

    def test_paren_inner_ops_not_counted_in_parent(self):
        # 顶层 1 个运算符(*)，括号内各 1 个；整式共 5 个运算符——若按整树计数会误拒，按顶层计数应放行
        expr = self.eval_expr("ABS(((CLOSE - OPEN) / (CLOSE / CLOSE)) * 2)")
        got = self.values(expr)
        # close/close=1 → (close-open)/1*2 = close-open 的 2 倍: [2,1,1,1]
        self.assertEqual(got, [2.0, 1.0, 1.0, 1.0])

    def test_window_field_param_still_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("MA(CLOSE - OPEN, 20)")

    def test_pow_operator_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("ABS(CLOSE ** 2)")

    def test_count_cond_with_arith_passes(self):
        expr = self.eval_expr("COUNT((CLOSE - OPEN) / CLOSE > 0.05, 2)")
        got = self.values(expr)
        # cond: [T,F,F,F] → rolling_sum(2)=[null,1,0,0]
        self.assertEqual(got, [None, 1, 0, 0])

    def test_bool_operand_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("ABS(CLOSE - True)")

    def test_abs_nested_paren_passes(self):
        expr = self.eval_expr("ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))")
        got = self.values(expr)
        # (REF1-REF2)/REF2: day3 (11-10)/10=0.1; day4 (12-11)/11=0.0909 → abs same
        self.assertAlmostEqual(got[2], 0.1, places=6)
        self.assertAlmostEqual(got[3], 0.090909, places=6)

    def test_unary_minus_top_level_passes(self):
        expr = self.eval_expr("CLOSE / REF(CLOSE, 1) - 1 < -0.05")
        got = self.values(expr)
        # 单日涨幅 (close/ref1-1): [null,0.1,0.0909,0.0769] 均 > -0.05 → 全 False
        self.assertEqual(got, [None, False, False, False])

    def test_unary_minus_cond_operand_passes(self):
        expr = self.eval_expr("COUNT(OPEN - CLOSE < -0.05, 2)")
        got = self.values(expr)
        # open-close: [-1,-0.5,-0.5,-0.5] < -0.05 → [T,T,T,T] → rolling_sum(2)=[null,2,2,2]
        self.assertEqual(got, [None, 2, 2, 2])

    def test_unary_plus_passes(self):
        expr = self.eval_expr("COUNT(CLOSE > +10, 2)")
        got = self.values(expr)
        # close > 10: [F,T,T,T] → rolling_sum(2)=[null,1,2,2]
        self.assertEqual(got, [None, 1, 2, 2])

    def test_unary_bool_operand_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("CLOSE < -True")


class TestArithSplitHelpers(unittest.TestCase):
    def test_split_top_level(self):
        self.assertEqual(_split_arith_top_level("A + B - C * D"), ["A", "B", "C", "D"])

    def test_split_ignores_operators_inside_parens(self):
        self.assertEqual(_split_arith_top_level("(A - B) + C"), ["(A - B)", "C"])

    def test_split_keeps_exponent_minus(self):
        self.assertEqual(_split_arith_top_level("A - 1e-3"), ["A", "1e-3"])
        self.assertEqual(_split_arith_top_level("A * 5e9"), ["A", "5e9"])

    def test_strip_outer_parens(self):
        self.assertEqual(_strip_outer_parens("(A - B) / C"), "(A - B) / C")
        self.assertEqual(_strip_outer_parens("((A))"), "A")


class TestLimitUpPct(unittest.TestCase):
    def _df(self):
        return pl.DataFrame({
            "date": ["2024-01-02"] * 5,
            "code": ["sh.600000", "sh.688001", "sz.300001", "sz.000001", "bj.830001"],
            "close": [10.0] * 5,
            "pctChg": [10.0, 15.0, 20.0, 10.0, 30.0],
        })

    def test_limit_up_pct_derived_from_code_prefix(self):
        df = self._df()
        blink_parser.current_df = df
        node = parse_call("LIMIT_UP_PCT")
        expr = blink_parser._visit(node)
        vals = df.with_columns(expr.alias("lup")).select(pl.col("lup")).to_series().to_list()
        self.assertEqual(vals, [10.0, 20.0, 20.0, 10.0, 30.0])

    def test_limit_up_translation(self):
        df = self._df()
        blink_parser.current_df = df
        # 涨停：主板 pctChg=10 通过（sh.600000/sz.000001）；科创 pctChg=15 拒绝；创业 pctChg=20 通过；北交 30 通过
        expr = blink_parser.parse_expression("PCT_CHG >= LIMIT_UP_PCT")
        out = df.with_columns(expr.alias("hit")).filter(pl.col("hit")).select("code").to_series().to_list()
        self.assertEqual(out, ["sh.600000", "sz.300001", "sz.000001", "bj.830001"])

    def test_limit_down_translation(self):
        df = self._df()
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("PCT_CHG <= 0 - LIMIT_UP_PCT")
        df2 = df.with_columns((pl.col("pctChg") * -1.0).alias("pctChg"))
        out = df2.with_columns(expr.alias("hit")).filter(pl.col("hit")).select("code").to_series().to_list()
        # 所有 pctChg 已取反：-10<=-10(main) / -15<=-20? no / -20<=-20(kc) / -10<=-10 / -30<=-30(bj)
        self.assertEqual(out, ["sh.600000", "sz.300001", "sz.000001", "bj.830001"])


if __name__ == "__main__":
    unittest.main()

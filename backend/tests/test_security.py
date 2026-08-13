import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ast
import unittest
import polars as pl
from core.security import blink_parser, _require_whitelist_field, _require_positive_int

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

if __name__ == "__main__":
    unittest.main()

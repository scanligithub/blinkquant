# DSL 算子扩展（CROSS/HHV/LLV/SUM/ABS/MAX/MIN/COUNT/BARSLAST）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 BlinkQuant 选股 DSL 新增 9 个算子，把安全层从"仅两层节点校验"升级为注册表签名驱动的递归校验器，使「20日均线上穿60日均线」等交叉/条件需求可运行。

**Architecture:** 注册表 `INDICATORS` 条目新增 `signature` 字段声明参数形态（`field`/`pos_int`/`series`/`cond`）；`security.py` 的 `_visit(ast.Call)` 按签名递归校验参数；`engine.py` 的 `metric_pattern` 从 `FIELDS` 全量生成并修复 VOL 列名 bug；前端 `validateFormula`/`buildSystemPrompt` 数据驱动自 `nl-meta` 同步递归校验。

**Tech Stack:** Python 3 / FastAPI / Polars（后端），TypeScript / Next.js（前端），node:test（前端单测），unittest（后端单测）。

**Spec:** `docs/superpowers/specs/2026-08-13-dsl-operator-expansion-design.md`

---

## 关键背景（工程师必读）

- 后端测试惯例：每个 `tests/test_*.py` 顶部 `sys.path.insert(0, dirname(dirname(abspath(__file__))))`，末尾 `unittest.main()`，可 `cd backend && python tests/test_xxx.py` 直接跑。
- 前端测试惯例：`frontend/tests/select-nl.test.mjs` **复制实现**而非 import 真实 `selectNL.ts`（仓库惯例），改真实实现必须同步改测试复制版，并可用 ~20 行守卫测试断言一致性（可选）。
- Hot-JIT：`engine.py:_prepare_hot_jit` 首次遇到 `MA(CLOSE,20)` 把 `MA_CLOSE_20` 挂载到日/周/月表，之后 `_visit` 快路径命中 `pl.col('MA_CLOSE_20')`。**非 window 算子（CROSS 等）不参与挂载/统计**。
- 快路径 pure_key：`f"{func}_{field}_{n}"`，window 型函数命中返回列引用，禁止删改。
- 字段映射：`blink_parser.fields` 是 `{大写字段: pl.col(真实列名)}` 唯一正确映射（`VOL→volume`、`CLOSE→close`）。engine 现硬编码 `pl.col(field_name.lower())` 与 `VOL→vol` 不符，本计划修复。
- 前端 `validateFormula` 当前直接拒绝所有嵌套括号；重写后需放行签名允许的嵌套（CROSS 等），但仍拒绝任意深嵌套。

---

### Task 1: 注册表扩展（indicator_registry.py）

**Files:**
- Modify: `backend/core/indicator_registry.py`
- Test: `backend/tests/test_registry.py`

- [ ] **Step 1: 更新注册表测试——新增算子与签名结构**

修改 `backend/tests/test_registry.py`：

替换 `test_builtin_indicators_present`、`test_all_indicators_are_window_funcs`、`test_indicator_funcs_derivation` 三个测试，新增签名校验测试。完整替换第 11-38 行为：

```python
class TestRegistry(unittest.TestCase):
    def test_builtin_indicators_present(self):
        self.assertEqual(sorted(INDICATORS.keys()), [
            "ABS", "BARSLAST", "COUNT", "CROSS_DOWN", "CROSS_UP",
            "EMA", "HHV", "LLV", "MA", "MAX", "MIN", "REF", "ROC", "STD", "SUM",
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
        # WINDOW_NAMES = 全部 window 型（供 Hot-JIT）；INDICATOR_NAMES = 全部（nl-meta/prompt）
        self.assertEqual(sorted(WINDOW_NAMES), [
            "EMA", "HHV", "LLV", "MA", "REF", "ROC", "STD", "SUM",
        ])
        self.assertEqual(sorted(INDICATOR_NAMES), sorted(INDICATORS.keys()))
        self.assertEqual(meta_signatures_set(), INDICATOR_NAMES)

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
```

同时更新 import 行（第 6-9 行）为：

```python
from core.indicator_registry import (
    INDICATORS, FIELDS, UNITS, EXAMPLE_QUERIES, TIMEFRAMES,
    INDICATOR_FUNCS, INDICATOR_NAMES, WINDOW_NAMES, nl_meta,
)
```

并加辅助函数（第 10 行前）：

```python
def meta_signatures_set():
    return set(nl_meta()["signatures"].keys())
```

保留 `test_lambdas_partition_by_code` 不变。

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && python tests/test_registry.py`
Expected: FAIL —— `ImportError: cannot import name 'WINDOW_NAMES'` 或断言失败（注册表未改）。

- [ ] **Step 3: 重写注册表**

替换 `backend/core/indicator_registry.py` 全文为：

```python
"""指标注册表：BlinkQuant 选股 DSL 的单一事实来源。

约定：注册函数签名由每条目的 "signature" 字段声明，参数形态取值：
- field   = 白名单字段的 ast.Name
- pos_int = 正整数常量（1 ≤ n ≤ 500）
- series  = field 或 一层窗口函数调用（如 MA(CLOSE,20)）
- cond    = 布尔表达式（Compare > >= < <= 或 AND/OR 组合）

"window": True 的条目签名恒为 [field, pos_int]，参与 Hot-JIT 挂载/统计；
其余为慢路径实时计算。开发者新增指标只需在此字典加一项。
"""

import functools
import polars as pl


def cross_up(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a > b) & (prev_a <= prev_b)


def cross_down(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a < b) & (prev_a >= prev_b)


def count(cond, n):
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code")


def barslast(cond):
    row = pl.int_range(pl.len()).over("code")
    anchor = pl.when(cond).then(row).otherwise(None)
    filled = anchor.forward_fill().over("code")
    return (row - filled).cast(pl.Int32)


INDICATORS = {
    # ---- window 型（签名 [field, pos_int]，Hot-JIT 挂载）----
    "MA":  {"func": lambda c, n: c.rolling_mean(window_size=n).over("code"),            "window": True, "signature": ["field", "pos_int"]},
    "EMA": {"func": lambda c, n: c.ewm_mean(span=n, adjust=False).over("code"),          "window": True, "signature": ["field", "pos_int"]},
    "STD": {"func": lambda c, n: c.rolling_std(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "ROC": {"func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True, "signature": ["field", "pos_int"]},
    "REF": {"func": lambda c, n: c.shift(n).over("code"),                               "window": True, "signature": ["field", "pos_int"]},
    "HHV": {"func": lambda c, n: c.rolling_max(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "LLV": {"func": lambda c, n: c.rolling_min(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "SUM": {"func": lambda c, n: c.rolling_sum(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    # ---- 非 window 型（慢路径实时计算）----
    "CROSS_UP":   {"func": cross_up,   "window": False, "signature": ["series", "series"]},
    "CROSS_DOWN": {"func": cross_down, "window": False, "signature": ["series", "series"]},
    "MAX": {"func": lambda a, b: pl.max_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "MIN": {"func": lambda a, b: pl.min_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "ABS": {"func": lambda x: x.abs(), "window": False, "signature": ["series"]},
    "COUNT":    {"func": count,    "window": False, "signature": ["cond", "pos_int"]},
    "BARSLAST": {"func": barslast, "window": False, "signature": ["cond"]},
}

# 字段白名单：必须与 security.py 现有 fields 键集逐项一致（防 drift）
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]

# 单位标注：用于 LLM 提示词与前端展示（覆盖全部白名单字段）
UNITS = {
    "TOTAL_MV": "元", "FLOAT_MV": "元", "TOTAL_SHARES": "股",
    "FLOAT_SHARES": "股", "AMOUNT": "元", "VOL": "股",
    "CLOSE": "元", "OPEN": "元", "HIGH": "元", "LOW": "元",
    "PE_TTM": "无量纲(倍)", "PB_MRQ": "无量纲(倍)", "TURN": "百分比(%)",
    "FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)", "S_CLOSE": "指数点位",
    "IS_FORECAST_GOOD": "布尔标记(0/1)", "IS_FORECAST_BAD": "布尔标记(0/1)",
}

# 算子中文说明：用于 LLM 提示词（value 会拼入 buildSystemPrompt）
DESCRIPTIONS = {
    "MA": "N日简单移动平均", "EMA": "N日指数移动平均", "STD": "N日标准差",
    "ROC": "N日变动率(%)", "REF": "N日前值", "HHV": "N周期内最高值",
    "LLV": "N周期内最低值", "SUM": "N周期内求和",
    "CROSS_UP": "上穿（今日A>B且昨日A<=B）", "CROSS_DOWN": "下穿（今日A<B且昨日A>=B）",
    "MAX": "取两序列较大值", "MIN": "取两序列较小值", "ABS": "绝对值",
    "COUNT": "N周期内条件成立次数", "BARSLAST": "距上次条件成立周期数",
}

EXAMPLE_QUERIES = [
    "CLOSE > MA(CLOSE, 20)",
    "PE_TTM < 20 AND TOTAL_MV > 1e10",
    "CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))",
    "SUM(AMOUNT, 5) > 5e9",
]

TIMEFRAMES = ["D", "W", "M"]

# window 型纯函数子集（供 Hot-JIT 与动态正则）
INDICATOR_FUNCS = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}
WINDOW_NAMES = sorted(INDICATOR_FUNCS.keys())
INDICATOR_NAMES = sorted(INDICATORS.keys())


def nl_meta() -> dict:
    """nl-meta 接口数据（注册表驱动的单一事实来源）"""
    return {
        "fields": FIELDS,
        "indicators": INDICATOR_NAMES,
        "timeframes": TIMEFRAMES,
        "units": UNITS,
        "example_queries": EXAMPLE_QUERIES,
        "signatures": {name: entry["signature"] for name, entry in INDICATORS.items()},
        "descriptions": DESCRIPTIONS,
    }
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && python tests/test_registry.py`
Expected: PASS（全部通过）。

- [ ] **Step 5: 提交**

```bash
git add backend/core/indicator_registry.py backend/tests/test_registry.py
git commit -m "feat(dsl): add 9 operators to registry with signature declarations"
```

---

### Task 2: 安全层递归校验器（security.py）

**Files:**
- Modify: `backend/core/security.py`
- Test: `backend/tests/test_security.py`

- [ ] **Step 1: 写失败测试——签名递归校验**

在 `backend/tests/test_security.py` 末尾、`if __name__` 前追加：

```python
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
        # 第3日：MA2=11.5, MA3=12.5 未穿；第4日：MA2=12.5, MA3=13.0 仍在上方
        # 构造 10,11,12,13：MA2 序列 = [null, 10.5, 11.5, 12.5]
        expr = self.eval_expr("CROSS_UP(MA(CLOSE, 2), MA(OPEN, 2))")
        got = self.df.with_columns(expr.alias("s"))["s"].to_list()
        # open 序列 9,10.5,11.5,12.5 → MA2 = [null, 9.75, 11.0, 12.0]
        # 首行 shift(1) 无昨日值 → None
        # 第3日 11.5>11.0 且第2日 10.5<=9.75? 否 → False
        # 第4日 12.5>12.0 且第3日 11.5<=11.0? 否 → False
        self.assertEqual(got, [None, None, False, False])

    def test_cross_up_true_on_cross(self):
        # close 10,12,11,14；open 9,11,12,13
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "code": ["sh.600000"] * 4,
            "close": [10.0, 12.0, 11.0, 14.0],
            "open": [9.0, 11.0, 12.0, 13.0],
        })
        blink_parser.current_df = df
        expr = blink_parser.parse_expression("CROSS_UP(MA(CLOSE, 1), MA(OPEN, 1))", "D")
        got = df.with_columns(expr.alias("s"))["s"].to_list()
        # MA1 = 原值。close 10,12,11,14 vs open 9,11,12,13
        # 首行 shift(1) 无昨日值 → None
        # 第2日: 12>11 且 10<=9? 否 → F
        # 第3日: 11>12? 否 → F
        # 第4日: 14>13 且 11<=12? 是 → T
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
        # close 10,9,11,8 vs open 11,10,10,12
        # 首行 shift(1) 无昨日值 → None
        # 第2日: 9<10 且 10>=11? 否 → F
        # 第3日: 11<10? 否 → F
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
        # close 10,11,12,13 >10: F,T,T,T；open 9,10.5,11.5,12.5 <11: T,T,F,F
        # AND: F,T,F,F → rolling_sum(3) = [null, null, 1, 1]
        self.assertEqual(got, [None, None, 1, 1])

    def test_barslast(self):
        expr = self.eval_expr("BARSLAST(CLOSE > 10)")
        got = self.df.with_columns(expr.alias("b"))["b"].to_list()
        # close 10,11,12,13；>10: F,T,T,T → anchor=row when true
        # row 0,1,2,3 → anchor null,1,2,3 → filled 0? (lead) → 结果 [null, 0, 0, 0]
        self.assertEqual(got, [None, 0, 0, 0])

    def test_cross_nested_series(self):
        # CROSS_UP 参数是窗口调用
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
        # == / != 不在 cond 白名单（> >= < <=）
        with self.assertRaises(ValueError):
            self.eval_expr("COUNT(CLOSE == 10, 3)")
```

注意：`test_count_and_or_cond` 的期望值需在实现后核对（`barslast` 的 filled 语义用 `forward_fill` 的 polars 行为）。实现后若断言与实际滚动语义不符，修正断言而非放宽实现。

- [ ] **Step 2: 运行测试确认失败**

Run: `cd backend && python tests/test_security.py`
Expected: FAIL（`Unknown function CROSS_UP` 等）。

- [ ] **Step 3: 重写 security.py 的 Call 分支与参数校验器**

替换 `backend/core/security.py` 中 `_require_positive_int`（第 17-23 行）为：

```python
WINDOW_MAX = 500

def _require_positive_int(node: ast.AST) -> int:
    """参数必须是正整数常量，且 1 ≤ n ≤ 500。"""
    if not isinstance(node, ast.Constant) or isinstance(node.value, bool) or not isinstance(node.value, int):
        raise ValueError("Window argument must be an integer constant")
    if node.value <= 0:
        raise ValueError("Window argument must be positive")
    if node.value > WINDOW_MAX:
        raise ValueError(f"Window argument must be at most {WINDOW_MAX}")
    return node.value
```

替换 `_visit` 的 `ast.Call` 分支（第 96-112 行）为：

```python
        elif isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ValueError("Function call target must be a name")
            func = node.func.id.upper()
            entry = INDICATORS.get(func)
            if entry is None:
                raise ValueError(f"Unknown function {func}")
            sig = entry["signature"]
            if len(node.args) != len(sig) or node.keywords:
                raise ValueError(f"Function {func} expects {len(sig)} positional args")
            args = [self._visit_arg(a, s, func) for a, s in zip(node.args, sig)]
            if entry.get("window"):
                field_name, n = args
                pure_key = f"{func}_{field_name}_{n}"
                if self.current_df is not None and pure_key in self.current_df.columns:
                    return pl.col(pure_key)
                return entry["func"](pl.col(field_name.lower()), n)
            return entry["func"](*args)
```

在 `_visit` 方法内新增 `_visit_arg`（放在 `_visit` 之后、类内新增方法）：

```python
    def _visit_arg(self, node: Any, kind: str, func: str) -> Any:
        """按签名声明的形态校验并求值单个参数。"""
        if kind == "field":
            return _require_whitelist_field(node)
        if kind == "pos_int":
            return _require_positive_int(node)
        if kind == "series":
            return self._require_series(node, func)
        if kind == "cond":
            return self._require_cond(node, func)
        raise ValueError(f"Unknown signature kind {kind}")

    def _require_series(self, node: Any, func: str) -> Any:
        """series = 白名单字段 或 一层窗口函数调用（复用 _visit 快路径）。"""
        if isinstance(node, ast.Name):
            name = _require_whitelist_field(node)
            return pl.col(name.lower())
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id.upper() in WINDOW_NAMES):
            return self._visit(node)
        raise ValueError(f"Function {func} arg must be a field or window indicator call")

    def _require_cond(self, node: Any, func: str) -> Any:
        """cond = Compare(> >= < <=) 或 BoolOp(AND/OR)。先结构白名单校验，再委托 _visit。"""
        self._validate_cond_structure(node, func, depth=0)
        return self._visit(node)

    def _validate_cond_structure(self, node: Any, func: str, depth: int) -> None:
        if depth > 2:
            raise ValueError(f"Function {func} cond nesting too deep")
        if isinstance(node, ast.Compare):
            if len(node.ops) != 1 or type(node.ops[0]) not in (ast.Gt, ast.GtE, ast.Lt, ast.LtE):
                raise ValueError(f"Function {func} cond must use > >= < <=")
            self._require_series_operand(node.left, func)
            self._require_series_operand(node.right, func)
            return
        if isinstance(node, ast.BoolOp) and type(node.op) in (ast.And, ast.Or):
            for v in node.values:
                self._validate_cond_structure(v, func, depth + 1)
            return
        raise ValueError(f"Function {func} cond must be a comparison or AND/OR expression")

    def _require_series_operand(self, node: Any, func: str) -> None:
        """cond 的操作数：series（字段/窗口调用）或数值常量。"""
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool):
                raise ValueError(f"Function {func} cond operand must be number or series")
            return
        self._require_series(node, func)
```

`_require_cond` 委托的 `_visit(node)` 对 Compare 的现有分支会处理 `_visit(left)`（Name→字段）与 `_visit(comparator)`（Call→窗口快路径），与 `_visit_arg` 一致。但注意：`_visit` 的 `ast.Compare` 分支对操作数调用 `self._visit`，其中 `ast.Call` 现在走新签名校验——cond 内嵌 `MA(CLOSE,20)` 会正常通过（window 型）。

同时更新 `_require_cond` 前需导入 `WINDOW_NAMES`。修改 `security.py` 顶部 import：

```python
from .indicator_registry import INDICATORS, FIELDS, WINDOW_NAMES
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd backend && python tests/test_security.py`
Expected: PASS。

若 `test_count_and_or_cond` / `test_barslast` 断言与实际 polars 滚动/前向填充语义不符，以实际向量行为为准修正**测试断言**（实现语义不可放宽——`COUNT`=rolling_sum 布尔转 int，`BARSLAST`=锚点 forward_fill 差值）。

- [ ] **Step 5: 提交**

```bash
git add backend/core/security.py backend/tests/test_security.py
git commit -m "feat(dsl): recursive signature-driven arg validation in security layer"
```

---

### Task 3: 引擎与元数据（engine.py / data_manager.py / routes.py）

**Files:**
- Modify: `backend/core/engine.py`
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_engine.py`

- [ ] **Step 1: 写失败测试——metric_pattern 全字段 + 窗口上限**

在 `backend/tests/test_engine.py` 末尾追加：

```python
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
        self.assertFalse(selection_engine.metric_pattern.search("CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))"))
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend && python tests/test_engine.py`
Expected: FAIL（`MA(PE_TTM, 5)` 不匹配，pattern 字段硬编码 OHLCV）。

- [ ] **Step 3: 改 engine.py**

替换 `backend/core/engine.py` 第 10-15 行：

```python
class SelectionEngine:
    def __init__(self):
        _funcs = "|".join(WINDOW_NAMES)
        _fields = "|".join(FIELDS)
        self.metric_pattern = re.compile(
            rf'\b({_funcs})\s*\(\s*({_fields})\s*,\s*(\d+)\s*\)',
            re.IGNORECASE)
```

替换 `_prepare_hot_jit` 中第 41-43 行的列挂载表达式：

```python
                        if func_name in data_manager.INDICATOR_MAP:
                            expr = data_manager.INDICATOR_MAP[func_name](
                                blink_parser.fields[field_name], p_val
                            ).alias(col_name)
                            new_exprs.append(expr)
```

修改 import（第 6 行）：

```python
from .indicator_registry import INDICATOR_NAMES, WINDOW_NAMES, FIELDS
```

（`INDICATOR_NAMES` 保留用于可能的外部引用，若不再使用可一并删除——先保留避免破坏。）

- [ ] **Step 4: 改 routes.py 的 example（可选但推荐）**

`backend/api/routes.py` 无结构改动；`EXAMPLE_QUERIES` 已在注册表扩展（Task 1），`nl_meta` 自动带出，无需改 routes.py。

- [ ] **Step 5: 运行测试确认通过**

Run: `cd backend && python tests/test_engine.py && python tests/test_registry.py`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add backend/core/engine.py backend/tests/test_engine.py
git commit -m "fix(dsl): engine pattern from full FIELDS, fix VOL column mapping"
```

---

### Task 4: 前端 selectNL.ts 递归校验 + prompt

**Files:**
- Modify: `frontend/src/lib/selectNL.ts`
- Test: `frontend/tests/select-nl.test.mjs`

- [ ] **Step 1: 写失败测试**

在 `frontend/tests/select-nl.test.mjs` 顶部 META 增加 signatures/descriptions，并追加新测试。先改 META（第 6-12 行）为：

```js
const META = {
  fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN'],
  indicators: ['ABS', 'BARSLAST', 'COUNT', 'CROSS_DOWN', 'CROSS_UP', 'EMA', 'HHV', 'LLV', 'MA', 'MAX', 'MIN', 'REF', 'ROC', 'STD', 'SUM'],
  timeframes: ['D', 'W', 'M'],
  units: { TOTAL_MV: '元', FLOAT_MV: '元', TOTAL_SHARES: '股', FLOAT_SHARES: '股', AMOUNT: '元', VOL: '股', PE_TTM: '无量纲(倍)', PB_MRQ: '无量纲(倍)', TURN: '百分比(%)', FORECAST_YOY: '百分比(%)', PCT_CHG: '百分比(%)', S_CLOSE: '指数点位' },
  example_queries: ['CLOSE > MA(CLOSE, 20)', 'PE_TTM < 20 AND TOTAL_MV > 1e10', 'CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))', 'SUM(AMOUNT, 5) > 5e9'],
  signatures: { MA: ['field', 'pos_int'], EMA: ['field', 'pos_int'], STD: ['field', 'pos_int'], ROC: ['field', 'pos_int'], REF: ['field', 'pos_int'], HHV: ['field', 'pos_int'], LLV: ['field', 'pos_int'], SUM: ['field', 'pos_int'], CROSS_UP: ['series', 'series'], CROSS_DOWN: ['series', 'series'], MAX: ['series', 'series'], MIN: ['series', 'series'], ABS: ['series'], COUNT: ['cond', 'pos_int'], BARSLAST: ['cond'] },
  descriptions: { MA: 'N日简单移动平均', EMA: 'N日指数移动平均', STD: 'N日标准差', ROC: 'N日变动率(%)', REF: 'N日前值', HHV: 'N周期内最高值', LLV: 'N周期内最低值', SUM: 'N周期内求和', CROSS_UP: '上穿（今日A>B且昨日A<=B）', CROSS_DOWN: '下穿（今日A<B且昨日A>=B）', MAX: '取两序列较大值', MIN: '取两序列较小值', ABS: '绝对值', COUNT: 'N周期内条件成立次数', BARSLAST: '距上次条件成立周期数' },
};
```

删除第 200-204 行旧测试 `validateFormula: 嵌套函数调用拒绝`（其断言被新语义取代），追加：

```js
test('validateFormula: CROSS_UP 嵌套指标通过', () => {
  const r = validateFormula(META, 'CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))');
  assert.equal(r.ok, true);
});

test('validateFormula: CROSS_UP 字段参数通过', () => {
  const r = validateFormula(META, 'CROSS_UP(CLOSE, OPEN)');
  assert.equal(r.ok, true);
});

test('validateFormula: COUNT 条件通过', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > MA(CLOSE, 20), 10) >= 7');
  assert.equal(r.ok, true);
});

test('validateFormula: COUNT AND 条件通过', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > MA(CLOSE, 20) AND VOL > 1e7, 10) > 3');
  assert.equal(r.ok, true);
});

test('validateFormula: BARSLAST 通过', () => {
  const r = validateFormula(META, 'BARSLAST(CLOSE > MA(CLOSE, 20)) <= 5');
  assert.equal(r.ok, true);
});

test('validateFormula: HHV/LLV/SUM 通过', () => {
  assert.equal(validateFormula(META, 'CLOSE > HHV(CLOSE, 20)').ok, true);
  assert.equal(validateFormula(META, 'CLOSE < LLV(CLOSE, 20)').ok, true);
  assert.equal(validateFormula(META, 'SUM(AMOUNT, 5) > 5e9').ok, true);
});

test('validateFormula: 二层嵌套拒绝', () => {
  const r = validateFormula(META, 'CROSS_UP(MA(MA(CLOSE, 20), 20), MA(CLOSE, 60))');
  assert.equal(r.ok, false);
});

test('validateFormula: COUNT 条件嵌套 COUNT 拒绝', () => {
  const r = validateFormula(META, 'COUNT(COUNT(CLOSE > 10, 2) > 1, 3)');
  assert.equal(r.ok, false);
});

test('validateFormula: 窗口超上限拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, 501) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: 未知算子拒绝', () => {
  const r = validateFormula(META, 'KDJ(CLOSE, 9) > 50');
  assert.equal(r.ok, false);
});

test('buildSystemPrompt: 包含新算子说明', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /CROSS_UP/);
  assert.match(p, /上穿/);
  assert.match(p, /COUNT/);
});
```

- [ ] **Step 2: 运行确认失败**

Run: `cd frontend && node --test tests/select-nl.test.mjs`
Expected: FAIL（`validateFormula` 仍拒绝嵌套、`META.signatures` 未定义等）。

- [ ] **Step 3: 重写前端 validateFormula**

替换 `frontend/src/lib/selectNL.ts` 中 `validateFormula`（第 48-108 行）为：

```typescript
const COMPARE_STR = '>=|<=|>|<';
const POS_INT_MAX = 500;

export function validateFormula(
  meta: NLMeta,
  formula: string
): { ok: true } | { ok: false; reason: string } {
  if (typeof formula !== 'string' || formula.trim().length === 0) {
    return { ok: false, reason: '公式为空' };
  }
  if (formula.length > MAX_FORMULA_LENGTH) {
    return { ok: false, reason: `公式过长（上限 ${MAX_FORMULA_LENGTH} 字符）` };
  }
  let depth = 0;
  for (const ch of formula) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return { ok: false, reason: '公式括号不配对' };
  }
  if (depth !== 0) return { ok: false, reason: '公式括号不配对' };
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };

  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);
  const sigs = meta.signatures ?? {};

  // 1. 校验所有函数调用签名（含嵌套，正则全局扫描）
  const callRegex = /([A-Z_][A-Z0-9_]*)\s*\(/g;
  let m: RegExpExecArray | null;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    const sig = sigs[func];
    if (!sig) return { ok: false, reason: `函数 ${func} 未注册` };
    const openIdx = m.index + m[0].length - 1;
    const closeIdx = matchParen(formula, openIdx);
    if (closeIdx === -1) return { ok: false, reason: '公式括号不配对' };
    const argStr = formula.slice(openIdx + 1, closeIdx);
    const args = splitTopLevel(argStr, ',').map((s) => s.trim());
    const v = validateCallArgs(meta, sig, args, func);
    if (v.ok === false) return v;
    callRegex.lastIndex = closeIdx + 1;
  }

  // 2. 其余大写标识符必须 ∈ 白名单
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t: RegExpExecArray | null;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }
  return { ok: true };
}

function matchParen(s: string, openIdx: number): number {
  let d = 0;
  for (let i = openIdx; i < s.length; i++) {
    if (s[i] === '(') d++;
    else if (s[i] === ')') d--;
    if (d === 0) return i;
  }
  return -1;
}

function splitTopLevel(s: string, sep: string): string[] {
  const parts: string[] = [];
  let d = 0, cur = '';
  for (const ch of s) {
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (ch === sep && d === 0) { parts.push(cur); cur = ''; }
    else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur);
  return parts;
}

function splitBoolTopLevel(s: string): string[] {
  const out: string[] = [];
  let d2 = 0, seg = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d2++;
    else if (ch === ')') d2--;
    if (d2 === 0 && (s.slice(i, i + 3) === 'AND' || s.slice(i, i + 2) === 'OR')) {
      const word = s.slice(i, i + 3) === 'AND' ? 'AND' : 'OR';
      const beforeOk = i === 0 || !/[A-Z0-9_]/.test(s[i - 1]);
      const afterIdx = i + word.length;
      const afterOk = afterIdx >= s.length || !/[A-Z0-9_]/.test(s[afterIdx]);
      if (beforeOk && afterOk) {
        out.push(seg.trim());
        seg = '';
        i = afterIdx - 1;
        continue;
      }
    }
    seg += ch;
  }
  if (seg.trim() !== '') out.push(seg.trim());
  return out.filter((x) => x !== '');
}

function validateCallArgs(
  meta: NLMeta,
  sig: string[],
  args: string[],
  func: string
): { ok: true } | { ok: false; reason: string } {
  if (args.length !== sig.length) {
    return { ok: false, reason: `函数 ${func} 必须恰好 ${sig.length} 个参数` };
  }
  for (let i = 0; i < sig.length; i++) {
    const kind = sig[i];
    const arg = args[i];
    if (kind === 'field') {
      if (!meta.fields.includes(arg)) {
        return { ok: false, reason: `函数 ${func} 参数 ${arg} 不在字段白名单` };
      }
    } else if (kind === 'pos_int') {
      if (!/^\d+$/.test(arg) || Number(arg) < 1 || Number(arg) > POS_INT_MAX) {
        return { ok: false, reason: `函数 ${func} 窗口必须是 1-${POS_INT_MAX} 正整数` };
      }
    } else if (kind === 'series') {
      if (!isSeriesExpr(meta, arg)) {
        return { ok: false, reason: `函数 ${func} 参数 ${arg} 必须是字段或窗口指标调用` };
      }
    } else if (kind === 'cond') {
      if (!isCondExpr(meta, arg)) {
        return { ok: false, reason: `函数 ${func} 条件参数不合法` };
      }
    }
  }
  return { ok: true };
}

function isSeriesExpr(meta: NLMeta, tok: string): boolean {
  if (meta.fields.includes(tok)) return true;
  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(([^()]*)\)$/.exec(tok);
  if (!mm) return false;
  const sig = meta.signatures?.[mm[1]];
  if (!sig || sig.length !== 2 || sig[0] !== 'field' || sig[1] !== 'pos_int') return false;
  const args = mm[2].split(',').map((s) => s.trim());
  return validateCallArgs(meta, sig, args, mm[1]).ok === true;
}

function isCondExpr(meta: NLMeta, tok: string): boolean {
  const parts = splitBoolTopLevel(tok);
  if (parts.length === 0) return false;
  return parts.every((p) => isCompareExpr(meta, p));
}

function isCompareExpr(meta: NLMeta, expr: string): boolean {
  const m = new RegExp(`^(.*?)\\s*(${COMPARE_STR})\\s*(.*)$`).exec(expr.trim());
  if (!m) return false;
  const left = m[1].trim();
  const right = m[3].trim();
  if (!isSeriesExpr(meta, left)) return false;
  if (!isSeriesExpr(meta, right) && !isNumber(right)) return false;
  return true;
}

function isNumber(s: string): boolean {
  return /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(s);
}
```

`splitBoolTopLevel` 使用单次线性扫描，按顶层（括号深度 0）的 `AND`/`OR` 分词（带词边界断言，避免误伤字段名）。

注意：`meta.signatures` 与 `meta.descriptions` 需要加入 `NLMeta` 接口（第 4-10 行）。替换 `NLMeta` 接口为：

```typescript
export interface NLMeta {
  fields: string[];
  indicators: string[];
  timeframes: string[];
  units: Record<string, string>;
  example_queries: string[];
  signatures: Record<string, string[]>;
  descriptions: Record<string, string>;
}
```

替换 `buildSystemPrompt` 中指标说明段（第 126 行附近）：

```typescript
    `可选算子（函数名(参数形态)：含义）：`,
    ...Object.entries(meta.descriptions ?? {})
      .map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
    '',
    'CROSS_UP/CROSS_DOWN/MAX/MIN 参数可嵌套指标调用（如 CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))），但不支持更深嵌套。',
    'COUNT/BARSLAST 的条件参数是比较表达式（> >= < <=），可用 AND/OR 组合。',
```

- [ ] **Step 4: 同步测试复制版**

把上述 `validateFormula` + 辅助函数 + `buildSystemPrompt` 更新**完整复制**到 `frontend/tests/select-nl.test.mjs` 的复制区（第 35-105 行区域），保持逐字符一致。

- [ ] **Step 5: 运行测试确认通过**

Run: `cd frontend && node --test tests/select-nl.test.mjs`
Expected: PASS（旧用例 + 新用例全绿）。

- [ ] **Step 6: TypeScript 检查**

Run: `cd frontend && npx tsc --noEmit -p tsconfig.json`
Expected: 无输出（通过）。

- [ ] **Step 7: 提交**

```bash
git add frontend/src/lib/selectNL.ts frontend/tests/select-nl.test.mjs
git commit -m "feat(dsl): recursive signature validation + operator prompt in selectNL"
```

---

### Task 5: 文档与部署说明

**Files:**
- Modify: `docs/CONTEXT.md`

- [ ] **Step 1: 更新 CONTEXT.md 已知对齐**

在 `docs/CONTEXT.md` 的「已知对齐」小节（v2.3 段）替换：

```markdown
- 公式不支持函数嵌套调用：`validateFormula` 的括号配对校验会拒绝嵌套括号。
```

为：

```markdown
- 公式支持签名允许的嵌套调用（CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))、COUNT(CLOSE > MA(CLOSE,20), 10)），但仅限一层：series 参数可嵌套窗口指标、cond 参数可 AND/OR 组合；更深嵌套（MA(MA(...)) 或 cond 内嵌套 COUNT）前后端均拒绝。
- 窗口上限 500：`MA(CLOSE, 501)` 前后端均拒绝。
```

- [ ] **Step 2: 提交**

```bash
git add docs/CONTEXT.md
git commit -m "docs: record DSL operator expansion alignment notes"
```

---

## 自审

### Spec 覆盖核对
- 注册表签名表（spec §1）→ Task 1
- CROSS 判定含等号、COUNT/BARSLAST 实现（spec §2）→ Task 2 测试
- 安全层递归校验（spec §3）→ Task 2
- Hot-JIT metric_pattern 全字段 + VOL bug 修复（spec §4）→ Task 3
- 前端 validateFormula/buildSystemPrompt（spec §6）→ Task 4
- 测试（spec §测试）→ 各任务内嵌
- 非目标（YAGNI：series 一层、cond 内禁 COUNT、NOT 保持排除）→ Task 2/4 测试断言锁定

### 已知实现细节风险（实现时现场核对，放宽测试断言而非实现）
- `test_barslast` 的 forward_fill 语义：若 polars `(row - filled)` 在首真前产生负数/null 行为与断言不符，以实际向量语义调整测试期望。
- `test_count_and_or_cond` 的 rolling_sum 边界：窗口不足处为 null，断言已按此写。

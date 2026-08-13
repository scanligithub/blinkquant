# 自然语言选股 + 声明式指标注册表 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让用户用中文自然语言选股（LLM 翻译成公式 + 预览可编辑），并把后端技术指标改为声明式注册表，实现「开发者一行注册、AST 白名单/Hot-JIT/nl-meta 三处自动派生」。

**Architecture:** 新增后端 `core/indicator_registry.py` 作为字段/指标/单位的单一事实来源；`security.py` 的 `ast.Call` 分支改查注册表 + 参数形态校验器（保留 Hot-JIT 快路径）；`data_manager.py`/`engine.py` 由注册表派生；新增只读接口 `GET /api/v1/nl-meta`。前端新增 Edge API `/api/select-nl`（守卫→限流→拉元数据→调 LLM→JSON 解析→强校验），弹窗 `AISelectModal` 展示可编辑公式预览，确认后复用现有选股管道。

**Tech Stack:** Python 3.10 + Polars + FastAPI（后端）；Next.js 14 + TypeScript + Edge Runtime（前端）；`node --test`（前端测试）；`unittest`（后端测试，仓库无 pytest）。

**Spec:** `docs/superpowers/specs/2026-08-13-nl-stock-select-design.md`

**验证命令（最终全量）：**
- 后端测试：`cd backend; python tests/test_registry.py; python tests/test_security.py; python tests/test_engine.py; python tests/test_meta.py`
- 后端语法：`cd backend; python -m py_compile main.py api/routes.py core/*.py`
- 前端测试：`cd frontend; node --test tests/select-nl.test.mjs`
- 前端类型：`cd frontend; npx tsc --noEmit`

---

## 文件结构

| 路径 | 动作 | 职责 |
|---|---|---|
| `backend/core/indicator_registry.py` | 新增 | INDICATORS / FIELDS / UNITS / EXAMPLE_QUERIES / `nl_meta()`，单一事实来源 |
| `backend/core/security.py` | 修改 | `_visit(ast.Call)` 查注册表；新增 `_require_whitelist_field` / `_require_positive_int` |
| `backend/core/data_manager.py` | 修改 | `INDICATOR_MAP` 由注册表派生 |
| `backend/core/engine.py` | 修改 | `metric_pattern` 由注册表生成 |
| `backend/api/routes.py` | 修改 | 新增 `GET /api/v1/nl-meta` |
| `backend/tests/__init__.py` | 新增 | 空文件，使 tests 为包 |
| `backend/tests/test_registry.py` | 新增 | 注册表与 nl_meta 结构测试 |
| `backend/tests/test_security.py` | 新增 | Call 分支校验/快路径/慢路径/字段一致性 |
| `backend/tests/test_engine.py` | 新增 | metric_pattern 动态生成、INDICATOR_MAP 派生 |
| `frontend/src/lib/selectNL.ts` | 新增 | buildSystemPrompt / parseSelectNLText / validateFormula / 限流纯函数 |
| `frontend/tests/select-nl.test.mjs` | 新增 | 上述纯函数测试（复制实现，仓库惯例） |
| `frontend/src/app/api/select-nl/route.ts` | 新增 | Edge API：守卫→限流→nl-meta→LLM→校验 |
| `frontend/src/components/AISelectModal.tsx` | 新增 | 自然语言输入 + 公式预览弹窗 |
| `frontend/src/app/page.tsx` | 修改 | 公式区加「AI 选股」按钮 + 弹窗接线；`handleSelect` 支持覆盖参数 |

---

## Task 1: 后端注册表 `indicator_registry.py`

**Files:**
- Create: `backend/core/indicator_registry.py`
- Create: `backend/tests/__init__.py`
- Create: `backend/tests/test_registry.py`

- [ ] **Step 1: 写失败测试 `backend/tests/test_registry.py`**

```python
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
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend; python tests/test_registry.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'core.indicator_registry'`

- [ ] **Step 3: 创建 `backend/core/indicator_registry.py`**

```python
"""指标注册表：BlinkQuant 选股 DSL 的单一事实来源。

约定：每个注册函数签名 f(column: pl.Expr, n: int) -> pl.Expr，
第一个参数永远是白名单字段列，第二个永远是正整数窗口常量。
开发者新增指标只需在此字典加一项，AST 白名单 / Hot-JIT / nl-meta 自动派生。
"""

import polars as pl

INDICATORS = {
    "MA":  {"func": lambda c, n: c.rolling_mean(window_size=n).over("code"),            "window": True},
    "EMA": {"func": lambda c, n: c.ewm_mean(span=n, adjust=False).over("code"),          "window": True},
    "STD": {"func": lambda c, n: c.rolling_std(window_size=n).over("code"),             "window": True},
    "ROC": {"func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True},
    "REF": {"func": lambda c, n: c.shift(n).over("code"),                  "window": True},
}

# 字段白名单：必须与 security.py 现有 fields 键集逐项一致（防 drift）
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]

# 单位标注：用于 LLM 提示词与前端展示
UNITS = {
    "TOTAL_MV": "元", "FLOAT_MV": "元", "TOTAL_SHARES": "股",
    "FLOAT_SHARES": "股", "AMOUNT": "元", "VOL": "股",
    "PE_TTM": "无量纲(倍)", "PB_MRQ": "无量纲(倍)", "TURN": "百分比(%)",
    "FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)", "S_CLOSE": "指数点位",
}

EXAMPLE_QUERIES = [
    "CLOSE > MA(CLOSE, 20)",
    "PE_TTM < 20 AND TOTAL_MV > 1e10",
]

TIMEFRAMES = ["D", "W", "M"]

# 供 Hot-JIT 与动态正则使用的纯函数子集（window 型）
INDICATOR_FUNCS = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}
INDICATOR_NAMES = sorted(INDICATOR_FUNCS.keys())


def nl_meta() -> dict:
    """nl-meta 接口数据（注册表驱动的单一事实来源）"""
    return {
        "fields": FIELDS,
        "indicators": INDICATOR_NAMES,
        "timeframes": TIMEFRAMES,
        "units": UNITS,
        "example_queries": EXAMPLE_QUERIES,
    }
```

- [ ] **Step 4: 创建空包文件 `backend/tests/__init__.py`**

```python
# 使 tests 目录成为 Python 包
```

- [ ] **Step 5: 运行确认通过**

Run: `cd backend; python tests/test_registry.py`
Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add backend/core/indicator_registry.py backend/tests/__init__.py backend/tests/test_registry.py
git commit -m "feat(registry): add declarative indicator registry as single source of truth"
```

---

## Task 2: `security.py` 重构 Call 分支查注册表

**Files:**
- Modify: `backend/core/security.py:76-100`（`_visit(ast.Call)`）
- Create: `backend/tests/test_security.py`

- [ ] **Step 1: 写失败测试 `backend/tests/test_security.py`**

```python
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

class TestCallBranch(unittest.TestCase):
    def test_unknown_function_rejected(self):
        node = parse_call("KDJ(CLOSE, 9)")
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

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend; python tests/test_security.py`
Expected: FAIL with `ImportError: cannot import name '_require_whitelist_field'`

- [ ] **Step 3: 修改 `backend/core/security.py`**

在文件顶部 imports 后加注册表导入与校验辅助函数（放在 `class BlinkParser` 之前）：

```python
from .indicator_registry import INDICATORS, FIELDS

def _require_whitelist_field(node: ast.AST) -> str:
    """参数必须是白名单字段名的 ast.Name。返回大写字段名。"""
    if not isinstance(node, ast.Name):
        raise ValueError("Field argument must be a field name")
    name = node.id.upper()
    if name not in FIELDS:
        raise ValueError(f"Unknown field {name}")
    return name

def _require_positive_int(node: ast.AST) -> int:
    """参数必须是正整数常量。"""
    if not isinstance(node, ast.Constant) or isinstance(node.value, bool) or not isinstance(node.value, int):
        raise ValueError("Window argument must be an integer constant")
    if node.value <= 0:
        raise ValueError("Window argument must be positive")
    return node.value
```

把 `_visit` 中的 `ast.Call` 分支（原 `security.py:76-98`）整体替换为：

```python
        elif isinstance(node, ast.Call):
            # 非 Name 函数名（如 foo.bar(1)）统一走 ValueError
            if not isinstance(node.func, ast.Name):
                raise ValueError("Function call target must be a name")
            func = node.func.id.upper()
            if func not in INDICATORS or not INDICATORS[func].get("window"):
                raise ValueError(f"Unknown function {func}")
            if len(node.args) != 2 or node.keywords:
                raise ValueError(f"Function {func} expects exactly 2 positional args")
            field_name = _require_whitelist_field(node.args[0])
            n = _require_positive_int(node.args[1])
            # ★ 快路径必须保留：命中 Hot-JIT 挂载列则直接返回列引用（提速来源，勿删）
            pure_key = f"{func}_{field_name}_{n}"
            if self.current_df is not None and pure_key in self.current_df.columns:
                return pl.col(pure_key)
            # 慢路径：实时向量化计算（首算后 engine 会挂载，下次即命中快路径）
            return INDICATORS[func]["func"](self.fields[field_name], n)
```

- [ ] **Step 4: 运行确认通过**

Run: `cd backend; python tests/test_security.py`
Expected: `OK`

- [ ] **Step 5: 回归旧公式语义（现有合法公式解析不抛错）**

Run:
```bash
cd backend; python -c "from core.security import blink_parser; blink_parser.current_df=None; print(type(blink_parser.parse_expression('CLOSE > MA(CLOSE, 20) and PE_TTM < 30','D')).__name__)"
```
Expected: `Expr`（不抛异常；`parse_expression` 已归一化大写逻辑词，如 `CLOSE > 11 AND PE_TTM < 30` 亦可用）

- [ ] **Step 6: Commit**

```bash
git add backend/core/security.py backend/tests/test_security.py
git commit -m "feat(security): route Call branch through indicator registry with arg validators"
```

---

## Task 3: `data_manager.py` + `engine.py` 由注册表派生

**Files:**
- Modify: `backend/core/data_manager.py:52-58`（INDICATOR_MAP）
- Modify: `backend/core/engine.py:11`（metric_pattern）
- Create: `backend/tests/test_engine.py`

- [ ] **Step 1: 写失败测试 `backend/tests/test_engine.py`**

```python
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
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend; python tests/test_engine.py`
Expected: FAIL（`KDJ` 断言失败或 INDICATOR_MAP 键不一致）

- [ ] **Step 3: 修改 `backend/core/data_manager.py`**

在 `import polars as pl` 之后加：

```python
from .indicator_registry import INDICATOR_FUNCS
```

把 `self.INDICATOR_MAP = {...}` 字典（原 `data_manager.py:53-58`）整体替换为：

```python
        self.INDICATOR_MAP = dict(INDICATOR_FUNCS)
```

- [ ] **Step 4: 修改 `backend/core/engine.py`**

在 `from .data_manager import data_manager` 之后加：

```python
from .indicator_registry import INDICATOR_NAMES
```

把 `__init__` 中的 `self.metric_pattern = re.compile(...)`（原 `engine.py:11`）替换为：

```python
        _funcs = "|".join(INDICATOR_NAMES)
        self.metric_pattern = re.compile(
            rf'({_funcs})\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)',
            re.IGNORECASE)
```

- [ ] **Step 5: 运行确认通过**

Run: `cd backend; python tests/test_engine.py`
Expected: `OK`

- [ ] **Step 6: 语法编译校验全部后端文件**

Run: `cd backend; python -m py_compile main.py api/routes.py core/security.py core/engine.py core/data_manager.py core/indicator_registry.py`
Expected: 无输出，退出码 0

- [ ] **Step 7: Commit**

```bash
git add backend/core/data_manager.py backend/core/engine.py backend/tests/test_engine.py
git commit -m "refactor(engine): derive INDICATOR_MAP and metric_pattern from registry"
```

---

## Task 4: 后端 `GET /api/v1/nl-meta`

**Files:**
- Modify: `backend/api/routes.py`
- Create: `backend/tests/test_meta.py`

- [ ] **Step 1: 写失败测试 `backend/tests/test_meta.py`**

```python
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from core.indicator_registry import nl_meta

class TestNlMeta(unittest.TestCase):
    def test_contains_example_queries(self):
        meta = nl_meta()
        self.assertIn("CLOSE > MA(CLOSE, 20)", meta["example_queries"])
        self.assertIn("PE_TTM < 20 AND TOTAL_MV > 1e10", meta["example_queries"])

    def test_units_include_market_value(self):
        meta = nl_meta()
        self.assertEqual(meta["units"]["TOTAL_MV"], "元")

    def test_route_importable(self):
        # 路由模块可导入（防语法错误），但 fastapi 可能未安装，此处仅编译检查
        import py_compile
        routes_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api", "routes.py")
        py_compile.compile(routes_path, doraise=True)

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行确认失败**

Run: `cd backend; python tests/test_meta.py`
Expected: 当前应 FAIL（`routes.py` 尚未加 nl-meta，但测试只查 compile——先让它过此步，Step 4 用 py_compile 全局验证）。若此步意外 PASS，属预期（本测试主要防回归），继续。

- [ ] **Step 3: 修改 `backend/api/routes.py`**

在文件顶部 `from core.security import blink_parser` 附近加：

```python
from core.indicator_registry import nl_meta as build_nl_meta
```

在 `/status` 路由之前加：

```python
@router.get("/nl-meta")
def get_nl_meta():
    """自然语言选股元数据：字段/指标/单位/示例（公开只读）"""
    return build_nl_meta()
```

- [ ] **Step 4: 全量语法与测试校验**

Run:
```bash
cd backend; python -m py_compile main.py api/routes.py core/*.py; python tests/test_registry.py; python tests/test_security.py; python tests/test_engine.py; python tests/test_meta.py
```
Expected: 全部 `OK`，py_compile 无输出

- [ ] **Step 5: Commit**

```bash
git add backend/api/routes.py backend/tests/test_meta.py
git commit -m "feat(api): add GET /api/v1/nl-meta registry-driven metadata endpoint"
```

---

## Task 5: 前端纯函数 `selectNL.ts` + 测试

**Files:**
- Create: `frontend/src/lib/selectNL.ts`
- Create: `frontend/tests/select-nl.test.mjs`

- [ ] **Step 1: 写失败测试 `frontend/tests/select-nl.test.mjs`**

按仓库惯例（`stock-search.test.mjs` 复制实现绕过 TS import），将 `selectNL.ts` 实现完整复制到测试文件底部，再用 `node --test` 断言。

```js
// frontend/tests/select-nl.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// ---- 复制自 src/lib/selectNL.ts（保持与实现一致）----
const META = {
  fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN'],
  indicators: ['EMA', 'MA', 'REF', 'ROC', 'STD'],
  timeframes: ['D', 'W', 'M'],
  units: { TOTAL_MV: '元', FLOAT_MV: '元', TOTAL_SHARES: '股', FLOAT_SHARES: '股', AMOUNT: '元', VOL: '股', PE_TTM: '无量纲(倍)', PB_MRQ: '无量纲(倍)', TURN: '百分比(%)', FORECAST_YOY: '百分比(%)', PCT_CHG: '百分比(%)', S_CLOSE: '指数点位' },
  example_queries: ['CLOSE > MA(CLOSE, 20)', 'PE_TTM < 20 AND TOTAL_MV > 1e10'],
};
const MAX_FORMULA_LENGTH = 500;
const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

function stripCodeFence(raw) {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

function parseSelectNLText(raw) {
  const cleaned = stripCodeFence(raw);
  let parsed;
  try { parsed = JSON.parse(cleaned); }
  catch { throw new Error('LLM 输出不是合法 JSON'); }
  if (!parsed || typeof parsed !== 'object') throw new Error('LLM 输出不是合法 JSON');
  if (typeof parsed.formula !== 'string' || parsed.formula.trim().length === 0) throw new Error('翻译结果缺少 formula');
  const explanation = typeof parsed.explanation === 'string' ? parsed.explanation.trim() : '';
  const timeframe = typeof parsed.timeframe === 'string' ? parsed.timeframe.toUpperCase() : 'D';
  return { formula: parsed.formula.trim(), timeframe, explanation };
}

function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

function validateFormula(meta, formula) {
  if (typeof formula !== 'string' || formula.trim().length === 0) return { ok: false, reason: '公式为空' };
  if (formula.length > MAX_FORMULA_LENGTH) return { ok: false, reason: `公式过长（上限 ${MAX_FORMULA_LENGTH} 字符）` };
  // 括号配对检查（防嵌套调用/未闭合括号漏到后端 AST）
  let depth = 0;
  for (const ch of formula) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return { ok: false, reason: '公式括号不配对' };
  }
  if (depth !== 0) return { ok: false, reason: '公式括号不配对' };

  // 嵌套调用检查：FUNC( 的参数内不允许再出现括号（防嵌套调用漏到后端 AST）
  const callStartRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(`, 'g');
  let cs;
  while ((cs = callStartRegex.exec(formula)) !== null) {
    const rest = formula.slice(callStartRegex.lastIndex);
    const openIdx = rest.indexOf('(');
    const closeIdx = rest.indexOf(')');
    if (openIdx !== -1 && openIdx < closeIdx) {
      return { ok: false, reason: '公式包含嵌套括号（不支持函数嵌套调用）' };
    }
  }
  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);
  const callRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(([^()]*)\\)`, 'g');
  let m;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    const args = m[2].split(',').map((s) => s.trim());
    if (args.length !== 2) return { ok: false, reason: `函数 ${func} 必须恰好 2 个参数` };
    if (!fields.has(args[0])) return { ok: false, reason: `函数 ${func} 第一参数 ${args[0]} 不在字段白名单` };
    if (!/^\d+$/.test(args[1]) || Number(args[1]) <= 0) return { ok: false, reason: `函数 ${func} 第二参数必须是正整数` };
  }
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue; // NOT 已移除——后端不支持 ast.Not
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };
  return { ok: true };
}

function buildSystemPrompt(meta) {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units).map(([k, v]) => `${k}=${v}`).join('，');
  return [
    '你是一名 A 股量化选股公式翻译助手。请把用户的中文选股需求翻译成 BlinkQuant 公式。',
    '字段白名单（只能使用这些，大小写必须一致）：',
    fieldsLine,
    '',
    '单位：',
    unitsLine,
    '',
    '单位换算规则（重要）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。例如"总市值大于100亿"应表达为 TOTAL_MV > 1e10。',
    '',
    '可选指标函数（只能使用，参数形态 FUNC(字段, 正整数窗口)：' + meta.indicators.join('、') + '。',
    '',
    '周期 timeframe 只能是 ' + meta.timeframes.join('/') + '。',
    '',
    '示例：',
    meta.example_queries.join('\n'),
    '',
    '输出必须是合法 JSON：{"formula":"...","timeframe":"D","explanation":"中文解释"}。',
    '只输出 JSON，不要输出其他文字。',
  ].join('\n');
}

function checkRateLimit(store, key, now, limitPerMinute = 3, limitPerDay = 20) {
  const MINUTE = 60 * 1000;
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  const recent = win.timestamps.filter((ts) => now - ts < DAY);
  const minuteCount = recent.filter((ts) => now - ts < MINUTE).length;
  if (minuteCount >= limitPerMinute) {
    const oldest = recent[recent.length - minuteCount];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, MINUTE - (now - oldest)) };
  }
  if (recent.length >= limitPerDay) {
    const first = recent[0];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, DAY - (now - first)) };
  }
  return { allowed: true, remaining: limitPerDay - recent.length, retryAfterMs: 0 };
}

function recordRequest(store, key, now) {
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  win.timestamps = [...win.timestamps.filter((ts) => now - ts < DAY), now];
  store.set(key, win);
}
// ---- 复制结束 ----

test('parseSelectNLText: 纯 JSON 正常解析', () => {
  const r = parseSelectNLText('{"formula":"CLOSE > MA(CLOSE, 20)","timeframe":"D","explanation":"突破20日均线"}');
  assert.deepEqual(r, { formula: 'CLOSE > MA(CLOSE, 20)', timeframe: 'D', explanation: '突破20日均线' });
});

test('parseSelectNLText: 代码围栏剥离', () => {
  const r = parseSelectNLText('```json\n{"formula":"PE_TTM < 20","timeframe":"d","explanation":"低估值"}\n```');
  assert.equal(r.formula, 'PE_TTM < 20');
  assert.equal(r.timeframe, 'D');
});

test('parseSelectNLText: 缺少 formula 抛错', () => {
  assert.throws(() => parseSelectNLText('{"explanation":"x"}'), /formula/);
});

test('parseSelectNLText: 非法 JSON 抛错', () => {
  assert.throws(() => parseSelectNLText('not json'), /JSON/);
});

test('validateFormula: 合法公式通过', () => {
  const r = validateFormula(META, 'CLOSE > MA(CLOSE, 20) AND PE_TTM < 30');
  assert.equal(r.ok, true);
});

test('validateFormula: 未知函数拒绝', () => {
  const r = validateFormula(META, 'KDJ(CLOSE, 9) > 50');
  assert.equal(r.ok, false);
  assert.match(r.reason, /KDJ/);
});

test('validateFormula: 未知字段拒绝', () => {
  const r = validateFormula(META, 'NOPE > 5');
  assert.equal(r.ok, false);
  assert.match(r.reason, /NOPE/);
});

test('validateFormula: 负窗口拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, -5) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: 非整数窗口拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, 2.5) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: 非法字符拒绝', () => {
  const r = validateFormula(META, "CLOSE > 5; DROP");
  assert.equal(r.ok, false);
});

test('validateFormula: 超长公式拒绝', () => {
  const long = 'CLOSE > 1' + ' AND CLOSE > 1'.repeat(200);
  const r = validateFormula(META, long);
  assert.equal(r.ok, false);
});

test('validateFormula: 空公式拒绝', () => {
  const r = validateFormula(META, '   ');
  assert.equal(r.ok, false);
});

test('validateFormula: 括号不配对拒绝', () => {
  const r = validateFormula(META, 'CLOSE > (5');
  assert.equal(r.ok, false);
  assert.match(r.reason, /括号/);
});

test('validateFormula: 嵌套函数调用拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, MA(CLOSE, 20)) > 10');
  assert.equal(r.ok, false);
  assert.match(r.reason, /括号/);
});

test('validateFormula: NOT 拒绝（后端不支持 ast.Not）', () => {
  const r = validateFormula(META, 'NOT (CLOSE > 11)');
  assert.equal(r.ok, false);
  assert.match(r.reason, /NOT/);
});

test('buildSystemPrompt: 包含字段与单位与示例', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /PE_TTM/);
  assert.match(p, /TOTAL_MV=元/);
  assert.match(p, /CLOSE > MA\(CLOSE, 20\)/);
  assert.match(p, /timeframe/);
});

test('buildSystemPrompt: 包含单位换算规则(亿→1e8)', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /亿/);
  assert.match(p, /1e8/);
  assert.match(p, /1e10/);
});

test('checkRateLimit: 允许窗口内请求', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  const r = checkRateLimit(store, 'k', 1000);
  assert.equal(r.allowed, true);
});

test('checkRateLimit: 每分钟超过阈值拒绝', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  recordRequest(store, 'k', 1000);
  recordRequest(store, 'k', 2000);
  const r = checkRateLimit(store, 'k', 3000); // 已是第 4 个
  assert.equal(r.allowed, false);
});

test('checkRateLimit: 每分钟阈值边界（3次/分内允许）', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  recordRequest(store, 'k', 1000);
  const r = checkRateLimit(store, 'k', 2000); // 第 3 次请求仍允许
  assert.equal(r.allowed, true);
});
```

- [ ] **Step 2: 运行确认可跑**

Run: `cd frontend; node --test tests/select-nl.test.mjs`
Expected: 20 个测试全部 PASS（测试自带复制实现，这一步验证测试本身正确可运行；`selectNL.ts` 尚未创建，语法正确性由此步保证）

- [ ] **Step 3: 创建 `frontend/src/lib/selectNL.ts`**

```ts
// src/lib/selectNL.ts
// 纯函数：自然语言选股 LLM 管道的提示词构建 / JSON 解析 / 公式强校验 / 限流

export interface NLMeta {
  fields: string[];
  indicators: string[];
  timeframes: string[];
  units: Record<string, string>;
  example_queries: string[];
}

export interface SelectNLResult {
  formula: string;
  timeframe: string;
  explanation: string;
}

export const MAX_FORMULA_LENGTH = 500;

const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

function escapeRe(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function stripCodeFence(raw: string): string {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

export function parseSelectNLText(raw: string): SelectNLResult {
  const cleaned = stripCodeFence(raw);
  let parsed: any;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    throw new Error('LLM 输出不是合法 JSON');
  }
  if (!parsed || typeof parsed !== 'object') throw new Error('LLM 输出不是合法 JSON');
  if (typeof parsed.formula !== 'string' || parsed.formula.trim().length === 0) {
    throw new Error('翻译结果缺少 formula');
  }
  const explanation = typeof parsed.explanation === 'string' ? parsed.explanation.trim() : '';
  const timeframe = typeof parsed.timeframe === 'string' ? parsed.timeframe.toUpperCase() : 'D';
  return { formula: parsed.formula.trim(), timeframe, explanation };
}

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
  // 括号配对检查（防嵌套调用/未闭合括号漏到后端 AST）
  let depth = 0;
  for (const ch of formula) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return { ok: false, reason: '公式括号不配对' };
  }
  if (depth !== 0) return { ok: false, reason: '公式括号不配对' };

  // 嵌套调用检查：FUNC( 的参数内不允许再出现括号（防嵌套调用漏到后端 AST）
  const callStartRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(`, 'g');
  let cs: RegExpExecArray | null;
  while ((cs = callStartRegex.exec(formula)) !== null) {
    const rest = formula.slice(callStartRegex.lastIndex);
    const openIdx = rest.indexOf('(');
    const closeIdx = rest.indexOf(')');
    if (openIdx !== -1 && openIdx < closeIdx) {
      return { ok: false, reason: '公式包含嵌套括号（不支持函数嵌套调用）' };
    }
  }
  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);

  // 1. 函数调用形态校验：FUNC(FIELD, N)，参数必须恰好 (字段, 正整数)
  const callRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(([^()]*)\\)`, 'g');
  let m: RegExpExecArray | null;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    const args = m[2].split(',').map((s) => s.trim());
    if (args.length !== 2) return { ok: false, reason: `函数 ${func} 必须恰好 2 个参数` };
    if (!fields.has(args[0])) return { ok: false, reason: `函数 ${func} 第一参数 ${args[0]} 不在字段白名单` };
    if (!/^\d+$/.test(args[1]) || Number(args[1]) <= 0) {
      return { ok: false, reason: `函数 ${func} 第二参数必须是正整数` };
    }
  }

  // 2. 其余大写标识符必须 ∈ 白名单（NOT 已移除——后端不支持 ast.Not）
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t: RegExpExecArray | null;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }

  // 3. 非法字符（防注入到 AST 之外）
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };

  return { ok: true };
}

export function buildSystemPrompt(meta: NLMeta): string {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units)
    .map(([k, v]) => `${k}=${v}`)
    .join('，');
  return [
    '你是一名 A 股量化选股公式翻译助手。请把用户的中文选股需求翻译成 BlinkQuant 公式。',
    '字段白名单（只能使用这些，大小写必须一致）：',
    fieldsLine,
    '',
    '单位：',
    unitsLine,
    '',
    '单位换算规则（重要）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。例如"总市值大于100亿"应表达为 TOTAL_MV > 1e10。',
    '',
    `可选指标函数（只能使用，参数形态 FUNC(字段, 正整数窗口)：${meta.indicators.join('、')}。`,
    '',
    `周期 timeframe 只能是 ${meta.timeframes.join('/')}。`,
    '',
    '示例：',
    meta.example_queries.join('\n'),
    '',
    '输出必须是合法 JSON：{"formula":"...","timeframe":"D","explanation":"中文解释"}。',
    '只输出 JSON，不要输出其他文字。',
  ].join('\n');
}

export interface RateWindow {
  timestamps: number[];
}

export function checkRateLimit(
  store: Map<string, RateWindow>,
  key: string,
  now: number,
  limitPerMinute = 3,
  limitPerDay = 20
): { allowed: boolean; remaining: number; retryAfterMs: number } {
  const MINUTE = 60 * 1000;
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  const recent = win.timestamps.filter((ts) => now - ts < DAY);
  const minuteCount = recent.filter((ts) => now - ts < MINUTE).length;
  if (minuteCount >= limitPerMinute) {
    const oldest = recent[recent.length - minuteCount];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, MINUTE - (now - oldest)) };
  }
  if (recent.length >= limitPerDay) {
    const first = recent[0];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, DAY - (now - first)) };
  }
  return { allowed: true, remaining: limitPerDay - recent.length, retryAfterMs: 0 };
}

export function recordRequest(store: Map<string, RateWindow>, key: string, now: number): void {
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  win.timestamps = [...win.timestamps.filter((ts) => now - ts < DAY), now];
  store.set(key, win);
}
```

- [ ] **Step 4: 确保测试与实现一致后运行确认通过**

Run: `cd frontend; node --test tests/select-nl.test.mjs`
Expected: 20 个测试全部 pass

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/selectNL.ts frontend/tests/select-nl.test.mjs docs/superpowers/plans/2026-08-13-nl-stock-select.md
git commit -m "feat(select-nl): add pure NL prompt/parse/validate/ratelimit functions with tests"
```

---

## Task 6: 前端 Edge API `select-nl/route.ts`

**Files:**
- Create: `frontend/src/app/api/select-nl/route.ts`

- [ ] **Step 1: 创建路由（Edge，无单测——纯胶水层，用类型检查+手动验证覆盖）**

```ts
import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildSystemPrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLText,
  validateFormula,
  type NLMeta,
} from '@/lib/selectNL';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

const LLM_ENDPOINT = process.env.LLM_ENDPOINT;
const LLM_API_KEY = process.env.LLM_API_KEY;
const LLM_MODEL = process.env.LLM_MODEL;
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 15000);

const META_TTL_MS = 24 * 60 * 60 * 1000;
let metaCache: { at: number; data: NLMeta } | null = null;
const rateStore = new Map<string, { timestamps: number[] }>();

async function fetchNlMeta(): Promise<NLMeta> {
  if (metaCache && Date.now() - metaCache.at < META_TTL_MS) return metaCache.data;
  const result = await Promise.any(
    NODES.map(async (nodeUrl) => {
      const res = await fetch(`${nodeUrl}/api/v1/nl-meta`, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`Node responded with ${res.status}`);
      return res.json();
    })
  );
  metaCache = { at: Date.now(), data: result as NLMeta };
  return result as NLMeta;
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  if (!LLM_ENDPOINT || !LLM_API_KEY || !LLM_MODEL) {
    return NextResponse.json({ error: 'AI 选股未配置', code: 'NOT_CONFIGURED' }, { status: 503 });
  }

  const key = `select-nl:${auth.user.userId}`;
  const now = Date.now();
  const limit = checkRateLimit(rateStore, key, now);
  if (!limit.allowed) {
    return NextResponse.json(
      { error: '调用过于频繁，请稍后再试', code: 'RATE_LIMITED', retryAfterMs: limit.retryAfterMs },
      { status: 429 }
    );
  }

  let body: { query?: string };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '请求体非法' }, { status: 400 });
  }
  const query = (body.query || '').trim();
  if (!query) {
    return NextResponse.json({ error: '请输入选股需求' }, { status: 400 });
  }

  try {
    const meta = await fetchNlMeta();
    const systemPrompt = buildSystemPrompt(meta);

    const llmRes = await fetch(LLM_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${LLM_API_KEY}` },
      body: JSON.stringify({
        model: LLM_MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: query },
        ],
        temperature: 0,
      }),
      signal: AbortSignal.timeout(LLM_TIMEOUT_MS),
    });
    if (!llmRes.ok) {
      throw new Error(`LLM HTTP ${llmRes.status}`);
    }
    const llmJson = await llmRes.json();
    const raw = llmJson?.choices?.[0]?.message?.content ?? '';

    const parsed = parseSelectNLText(raw);
    const validation = validateFormula(meta, parsed.formula);
    if (!validation.ok) {
      return NextResponse.json(
        {
          error: `翻译结果不合法：${validation.reason}`,
          code: 'INVALID_FORMULA',
          formula: parsed.formula,
          explanation: parsed.explanation,
        },
        { status: 400 }
      );
    }
    if (!meta.timeframes.includes(parsed.timeframe)) {
      return NextResponse.json(
        { error: `翻译结果周期不合法：${parsed.timeframe}`, code: 'INVALID_FORMULA' },
        { status: 400 }
      );
    }

    recordRequest(rateStore, key, now);

    return NextResponse.json({
      success: true,
      data: { formula: parsed.formula, timeframe: parsed.timeframe, explanation: parsed.explanation },
    });
  } catch (err) {
    console.error('select-nl failed:', err);
    return NextResponse.json(
      { error: 'AI 选股服务暂不可用，请稍后再试或改用公式', code: 'LLM_UNAVAILABLE' },
      { status: 502 }
    );
  }
}
```

- [ ] **Step 2: 类型检查**

Run: `cd frontend; npx tsc --noEmit`
Expected: 无类型错误

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/api/select-nl/route.ts
git commit -m "feat(api): add /api/select-nl edge route (auth, ratelimit, LLM, validation)"
```

---

## Task 7: 前端 `AISelectModal.tsx` 组件

**Files:**
- Create: `frontend/src/components/AISelectModal.tsx`

- [ ] **Step 1: 创建组件（风格对齐现有弹窗，参考 `page.tsx` 保存策略弹窗 + `StockSearch.tsx` loading 态）**

```tsx
'use client';
import { useState } from 'react';

interface AISelectModalProps {
  onClose: () => void;
  onRun: (formula: string, timeframe: string) => void;
}

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

export default function AISelectModal({ onClose, onRun }: AISelectModalProps) {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState<{ formula: string; timeframe: string; explanation: string } | null>(null);
  const [timeframe, setTimeframe] = useState('D');

  const translate = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError('');
    setResult(null);
    try {
      const res = await fetch('/api/select-nl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: query.trim() }),
      });
      const json = await res.json();
      if (!res.ok) {
        setError(json.error || '翻译失败');
        return;
      }
      setResult(json.data);
      setTimeframe(json.data.timeframe);
    } catch (e) {
      setError('AI 选股服务暂不可用');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl w-full max-w-lg shadow-xl p-6" onClick={(e) => e.stopPropagation()}>
        <h2 className="font-bold text-slate-700 mb-4">AI 选股</h2>

        <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">描述你的选股条件</label>
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          rows={2}
          className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
          placeholder="例如：市盈率低于20且总市值大于100亿的股票"
        />
        <div className="mt-3 flex justify-end gap-2">
          <button
            onClick={translate}
            disabled={loading || !query.trim()}
            className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50 flex items-center gap-2"
          >
            {loading && <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>}
            翻译
          </button>
        </div>

        {error && (
          <div className="mt-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>
        )}

        {result && (
          <div className="mt-4">
            <div className="flex items-center justify-between gap-2">
              <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">公式预览（可编辑）</label>
              <div className="flex items-center gap-1">
                {TIMEFRAMES.map((tf) => (
                  <button
                    key={tf.value}
                    onClick={() => setTimeframe(tf.value)}
                    className={`px-2 py-1 text-xs font-bold rounded-md ${timeframe === tf.value ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100'}`}
                  >
                    {tf.label}
                  </button>
                ))}
              </div>
            </div>
            <textarea
              value={result.formula}
              onChange={(e) => setResult({ ...result, formula: e.target.value })}
              rows={2}
              className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
            />
            {result.explanation && (
              <div className="mt-2 text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                {result.explanation}
              </div>
            )}
            <div className="mt-4 flex justify-end gap-2">
              <button onClick={onClose} className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50">
                取消
              </button>
              <button
                onClick={() => onRun(result.formula, timeframe)}
                className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold"
              >
                运行选股
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: 类型检查**

Run: `cd frontend; npx tsc --noEmit`
Expected: 无类型错误

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/AISelectModal.tsx
git commit -m "feat(ui): add AISelectModal with editable formula preview"
```

---

## Task 8: `page.tsx` 接线「AI 选股」入口

**Files:**
- Modify: `frontend/src/app/page.tsx`

- [ ] **Step 1: 加 import**

在 `page.tsx` 顶部静态 import 块（`StockSearch` 附近）加：

```tsx
import AISelectModal from '../components/AISelectModal';
```

- [ ] **Step 2: 加状态**

在 `const [saveStrategyOpen, setSaveStrategyOpen] = useState(false);`（`page.tsx:45`）附近加：

```tsx
  const [showAISelect, setShowAISelect] = useState(false);
```

- [ ] **Step 3: `handleSelect` 支持覆盖参数（避免 setState 异步竞态）**

把 `page.tsx:281-293` 的 `handleSelect` 函数签名与 body 改为：

```tsx
  const handleSelect = async (overrides?: { formula?: string; timeframe?: string }) => {
    setLoading(true); setResults([]); setSelectedStock(null);
    const f = overrides?.formula ?? formula;
    const t = overrides?.timeframe ?? timeframe;
    try {
      const res = await fetch('/api/select', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula: f, timeframe: t })
      });
      const json = await res.json();
      if (json.success) setResults(json.data);
      else alert(`Selection failed: ${json.error}`);
    } catch (err) { alert('Gateway connection failed'); }
    setLoading(false);
  };
```

- [ ] **Step 4: 加「AI 选股」按钮**

在公式输入区按钮组（`page.tsx:493-502`，`运行选股` 与 `保存为策略` 按钮之间）插入：

```tsx
              <button
                onClick={() => setShowAISelect(true)}
                disabled={loading}
                className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-xl font-bold"
              >
                AI 选股
              </button>
```

- [ ] **Step 5: 渲染弹窗**

在 `{saveStrategyOpen && (...)}` 弹窗之后（`page.tsx:803` 附近）加：

```tsx
      {showAISelect && (
        <AISelectModal
          onClose={() => setShowAISelect(false)}
          onRun={(formula, timeframe) => {
            setShowAISelect(false);
            handleSelect({ formula, timeframe });
          }}
        />
      )}
```

- [ ] **Step 6: 类型检查 + lint**

Run: `cd frontend; npx tsc --noEmit`
Expected: 无类型错误

- [ ] **Step 7: Commit**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat(ui): wire AI select modal into main page"
```

---

## Task 9: 全量验证

- [ ] **Step 1: 后端全量**

Run:
```bash
cd backend; python -m py_compile main.py api/routes.py core/*.py; python tests/test_registry.py; python tests/test_security.py; python tests/test_engine.py; python tests/test_meta.py
```
Expected: 全部 `OK`

- [ ] **Step 2: 前端全量**

Run:
```bash
cd frontend; node --test tests/select-nl.test.mjs; npx tsc --noEmit
```
Expected: 测试 pass + 无类型错误

- [ ] **Step 3: 更新 `docs/CONTEXT.md`**

追加一段「自然语言选股 + 指标注册表 (v2.3)」说明，记录：
- 注册表为字段/指标/单位单一事实来源；新增指标三处自动派生的位置
- `/api/v1/nl-meta`、`/api/select-nl` 与 `AISelectModal` 的职责
- 新增 Vercel env：`LLM_ENDPOINT`/`LLM_API_KEY`/`LLM_MODEL`（未配则 AI 入口 503）
- 后端需推 main 重新部署（`backend/**` 触发 3 节点）

- [ ] **Step 4: Commit**

```bash
git add docs/CONTEXT.md
git commit -m "docs: record NL stock select + indicator registry integration"
```

---

## Self-Review 结论

- **Spec 覆盖**：注册表（T1）、security.py Call 分支 + 校验器（T2）、data_manager/engine 派生（T3）、nl-meta（T4）、selectNL 纯函数（T5）、select-nl 路由（T6）、弹窗（T7）、接线（T8）、env/部署/验收/扩展指南（T9 文档 + 各任务）。快路径保留在 T2 Step 3 显式实现并测试。
- **Placeholder 检查**：全部步骤含完整代码与命令，无 TBD/TODO。
- **类型一致性**：`validateFormula`/`parseSelectNLText`/`buildSystemPrompt`/`checkRateLimit`/`recordRequest` 在 selectNL.ts 与 .mjs 测试中签名一致；`INDICATOR_FUNCS`/`INDICATOR_NAMES`/`nl_meta` 在注册表与测试中一致；`handleSelect(overrides?)` 与弹窗 `onRun` 参数类型一致。

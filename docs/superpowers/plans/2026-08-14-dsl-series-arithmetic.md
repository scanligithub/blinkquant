# DSL Series 算术表达式支持实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 DSL 的 series 位置接受 `+ - * /` 算术表达式（顶层运算符 ≤3，括号内递归独立适用），前后端校验语义一致。

**Architecture:** 后端 `_require_series` 新增 BinOp 分支，用 `ast.get_source_segment` 取源码片段做顶层切分计数；前端 `isSeriesExpr` 重构分支顺序 + 新增 `splitArithTopLevel`/`stripOuterParens`/`countTopLevelOps`/`isArithExpr`。两侧用相同切分规则保证结论一致。求值均沿用既有 `_visit`（后端）与后端运行时，无新增求值路径。

**Tech Stack:** Python 3 + polars（后端）、TypeScript + Node test（前端）、`ast.get_source_segment`（Python 3.8+）。

---

## File Structure

| 文件 | 职责 | 动作 |
|---|---|---|
| `backend/core/security.py` | 后端 DSL 安全解析 | Modify：`parse_expression` 保存源文本、`_require_series` 加 BinOp 分支、新增 5 个辅助函数 |
| `backend/tests/test_security.py` | 后端单测 | Modify：新增 TestSeriesArithmetic |
| `frontend/src/lib/selectNL.ts` | 前端公式强校验 | Modify：`isSeriesExpr` 重构 + 新增 4 个辅助函数 |
| `frontend/tests/select-nl.test.mjs` | 前端单测（复制版） | Modify：复制同步 + 新增测试用例 |
| `docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md` | 设计文档 | 参考（已提交 `412e3ac`） |

任务顺序：后端（T1-T3）→ 前端（T4-T6）→ 全量回归 + 文档收尾（T7）。

---

### Task 1: 后端新增算术校验辅助函数（TDD）

**Files:**
- Modify: `backend/core/security.py`
- Test: `backend/tests/test_security.py`

- [ ] **Step 1: 写正式失败测试 TestSeriesArithmetic**

在 `backend/tests/test_security.py` 末尾追加测试类（覆盖 spec 全部验收点；注意顶层比较/顶层算术前端与后端均不做操作数校验，故超限用例**包进 series 位置 `ABS(...)`** 才能触发校验）：

```python
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
```

- [ ] **Step 2: 运行确认失败（红）**

Run: `python -m unittest tests.test_security.TestSeriesArithmetic -v`（在 `backend/` 目录）
Expected: 多数用例 FAIL（`ValueError: Function ABS arg must be a field or single-value indicator call`）；`test_window_field_param_still_rejected` 单独 PASS（本就拒绝）。

- [ ] **Step 3: 实现后端算术校验**

在 `backend/core/security.py` 修改/新增：

1. `parse_expression`（L63-73）在 `tree = ast.parse(...)` 前保存源文本：

```python
clean_expr = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), expr_str.strip())
clean_expr = clean_expr.replace('&&', '&').replace('||', '|')
self.current_source = clean_expr
tree = ast.parse(clean_expr, mode='eval')
```

2. `__init__`（L60-61 附近）初始化 `current_source`：

```python
self.current_df = None
self.current_source = None
```

3. 模块常量（`WINDOW_MAX` 下方）加：

```python
ARITH_MAX_OPS = 3
```

4. 模块级工具函数（`_require_positive_int` 与 `class BlinkParser` 之间插入）：

```python
def _split_arith_top_level(text: str) -> list:
    """按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符。与前端 splitArithTopLevel 语义一致。"""
    parts = []
    depth = 0
    cur = ''
    for i, ch in enumerate(text):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth == 0 and ch in '+-*/':
            if (ch in '+-') and i > 0 and text[i - 1] in 'eE':
                cur += ch
                continue
            parts.append(cur.strip())
            cur = ''
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return [p for p in parts if p] if parts or text.strip() else []


def _is_outer_paren_balanced(t: str) -> bool:
    """首括号是否配对并闭合于末位。"""
    if not t.startswith('('):
        return False
    depth = 0
    for i, ch in enumerate(t):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth == 0:
            return i == len(t) - 1
    return False


def _strip_outer_parens(text: str) -> str:
    t = text.strip()
    while t.startswith('(') and _is_outer_paren_balanced(t):
        t = t[1:-1].strip()
    return t
```

5. `blink_parser` 单例内的 `BlinkParser` 方法，`_require_series`（L133-142）增加 BinOp 分支并调用 `_require_arith`：

```python
def _require_series(self, node: Any, func: str) -> Any:
    """series = 白名单字段 或 签名不含 cond 形态的算子调用（含窗口/非窗口） 或 +-*/ 算术表达式。"""
    if isinstance(node, ast.Name):
        name = _require_whitelist_field(node)
        return self.fields[name]
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id.upper() in INDICATORS
            and "cond" not in INDICATORS[node.func.id.upper()]["signature"]):
        return self._visit(node)
    if isinstance(node, ast.BinOp) and type(node.op) in (ast.Add, ast.Sub, ast.Mult, ast.Div):
        self._require_arith(node, func)
        return self._visit(node)
    raise ValueError(f"Function {func} arg must be a field, single-value indicator call, or arithmetic expression")
```

新增方法（放在 `_require_series` 后面、`_require_cond` 前）：

```python
def _top_level_ops(self, node) -> int:
    """按源码文本统计顶层算术运算符数（括号内不计入）。get_source_segment 失败时退化为整树计数兜底。"""
    seg = ast.get_source_segment(self.current_source, node) if isinstance(self.current_source, str) else None
    if seg is not None:
        t = _strip_outer_parens(seg)
        parts = _split_arith_top_level(t)
        return max(0, len(parts) - 1)
    return self._arith_ops_tree(node)

def _arith_ops_tree(self, node) -> int:
    """兜底：整棵 BinOp 子树运算符总数。"""
    if not isinstance(node, ast.BinOp) or type(node.op) not in (ast.Add, ast.Sub, ast.Mult, ast.Div):
        return 0
    return 1 + self._arith_ops_tree(node.left) + self._arith_ops_tree(node.right)

def _require_arith(self, node: Any, func: str) -> Any:
    """算术表达式结构校验：操作数 = 数值常量 / series / 更浅的算术；顶层运算符数 ≤ ARITH_MAX_OPS。"""
    if self._top_level_ops(node) > ARITH_MAX_OPS:
        raise ValueError(f"Function {func} arithmetic too many top-level operators (max {ARITH_MAX_OPS})")
    for child in (node.left, node.right):
        if isinstance(child, ast.Constant):
            if isinstance(child.value, bool):
                raise ValueError(f"Function {func} arithmetic operand must be number or series")
            continue
        if isinstance(child, ast.BinOp) and type(child.op) in (ast.Add, ast.Sub, ast.Mult, ast.Div):
            self._require_arith(child, func)
        else:
            self._require_series(child, func)
```

- [ ] **Step 4: 运行测试确认通过（绿）**

Run: `python -m unittest tests.test_security -v`（在 `backend/` 目录）
Expected: 既有 44 tests + 新增 10 tests 全 PASS。

- [ ] **Step 5: 运行全量后端测试确认无回归**

Run: `python -m unittest discover -s tests -v`（在 `backend/` 目录）
Expected: 所有测试 PASS（registry 11 + security 44 + 新增 10 + 其他既有套件）。

- [ ] **Step 6: Commit**

```bash
git add backend/core/security.py backend/tests/test_security.py
git commit -m "feat: support +-*/ arithmetic in DSL series positions (backend)"
```

---

### Task 2: 后端边界——顶层切分工具单测（可选但推荐）

**Files:**
- Modify: `backend/tests/test_security.py`
- (无实现改动)

- [ ] **Step 1: 写顶层切分工具的直接单测**

在 `backend/tests/test_security.py` 顶部 imports 追加 `from core.security import _split_arith_top_level, _strip_outer_parens`，末尾追加：

```python
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
```

- [ ] **Step 2: 运行确认通过**

Run: `python -m unittest tests.test_security.TestArithSplitHelpers -v`（在 `backend/` 目录）
Expected: 4 tests PASS。

- [ ] **Step 3: Commit**

```bash
git add backend/tests/test_security.py
git commit -m "test: splitArithTopLevel/stripOuterParens backend helpers"
```

---

### Task 3: 前端 selectNL.ts 算术校验（TDD）

**Files:**
- Modify: `frontend/src/lib/selectNL.ts`
- Test: `frontend/tests/select-nl.test.mjs`（复制版）

- [ ] **Step 1: 写失败测试（复制版函数暂保持旧实现，只新增测试用例）**

在 `frontend/tests/select-nl.test.mjs` 的 `// ---- 复制结束 ----` 之后追加测试用例。**注意**：`validateFormula` 只校验函数调用签名与标识符白名单，不校验顶层比较/顶层算术的操作数，因此所有算术用例必须包进 series 或 cond 位置（`ABS(...)`、`COUNT(...,5)`）才能触发 `isSeriesExpr`/`isArithExpr`：

```javascript
test('validateFormula: ABS 算术参数通过', () => {
  assert.equal(validateFormula(META, 'ABS(REF(CLOSE, 1) - REF(CLOSE, 2))').ok, true);
});

test('validateFormula: cond 括号算术比较通过', () => {
  assert.equal(validateFormula(META, 'COUNT((CLOSE - OPEN) / CLOSE > 0.05, 5)').ok, true);
});

test('validateFormula: cond 常量乘法通过', () => {
  assert.equal(validateFormula(META, 'COUNT(CLOSE * 1.1 > REF(CLOSE, 1), 5)').ok, true);
});

test('validateFormula: 顶层运算符超上限拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE / CLOSE / CLOSE / CLOSE / CLOSE)');
  assert.equal(r.ok, false);
});

test('validateFormula: 括号内运算符不计入父级顶层', () => {
  assert.equal(validateFormula(META, 'ABS(((CLOSE - OPEN) / (CLOSE / CLOSE)) * 2)').ok, true);
});

test('validateFormula: 窗口 field 参数算术拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE - OPEN, 20)');
  assert.equal(r.ok, false);
});

test('validateFormula: 幂运算符拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE ** 2)');
  assert.equal(r.ok, false);
});

test('validateFormula: 布尔操作数拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE - True)');
  assert.equal(r.ok, false);
});

test('validateFormula: 嵌套括号算术通过', () => {
  assert.equal(
    validateFormula(META, 'ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))').ok,
    true
  );
});
```

- [ ] **Step 2: 运行确认失败（红）**

Run: `node --test tests/select-nl.test.mjs`（在 `frontend/` 目录）
Expected: 新增 9 个用例 FAIL（旧复制版 `isSeriesExpr` 无算术分支：`ABS(REF...)` 作为 series 参数时 `closeIdx !== length-1` 提前 return false；`MA(CLOSE-OPEN,20)`、`ABS(CLOSE ** 2)` 因 field/pos_int 校验本 PASS 除外——确认至少通过类用例集体红）。

- [ ] **Step 3: 同步新增复制版函数 + 修改实现 selectNL.ts**

3a. 在 `frontend/tests/select-nl.test.mjs` 的镜像复制区（`isNumber` 函数后）新增 4 个函数，并把 `isSeriesExpr` 替换为重构版：

```javascript
const ARITH_MAX_OPS = 3;

function stripOuterParens(tok) {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s) {
  // 按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符
  const parts = [];
  let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (d === 0 && '+-*/'.includes(ch)) {
      if ((ch === '-' || ch === '+') && /[eE]/.test(s[i - 1] ?? '')) { cur += ch; continue; }
      parts.push(cur.trim());
      cur = '';
    } else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur.trim());
  return parts.filter((x) => x !== '');
}

function countTopLevelOps(tok) {
  return splitArithTopLevel(stripOuterParens(tok)).length - 1;
}

function isArithExpr(meta, tok) {
  const parts = splitArithTopLevel(stripOuterParens(tok));
  if (parts.length < 2) return false;
  if (countTopLevelOps(tok) > ARITH_MAX_OPS) return false;
  return parts.every((p) => isSeriesExpr(meta, p) || isNumber(p) || isArithExpr(meta, p));
}

function isSeriesExpr(meta, tok) {
  if (meta.fields.includes(tok)) return true;
  // 1. 平衡外括号剥离
  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 函数调用路径：仅当 call 的闭合括号恰在末尾
  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(/.exec(tok);
  if (mm) {
    const sig = meta.signatures?.[mm[1]];
    const openIdx = mm.index + mm[0].length - 1;
    if (sig && !sig.includes('cond')) {
      const closeIdx = matchParen(tok, openIdx);
      if (closeIdx === tok.length - 1) {
        const argStr = tok.slice(openIdx + 1, closeIdx);
        const args = splitTopLevel(argStr, ',').map((s) => s.trim());
        if (validateCallArgs(meta, sig, args, mm[1]).ok === true) return true;
      }
    }
  }
  // 3. 算术表达式路径
  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}
```

3b. 在 `frontend/src/lib/selectNL.ts` 中执行与上述复制版逐字一致的改动：
- 顶部常量区加 `const ARITH_MAX_OPS = 3;`
- 新增 `stripOuterParens` / `splitArithTopLevel` / `countTopLevelOps` / `isArithExpr`
- 将 `isSeriesExpr`（L182-194）替换为重构版本（含类型注解 `NLMeta`）

```typescript
const ARITH_MAX_OPS = 3;

// 剥离平衡外括号：'(...)' 且首括号闭合于末位 → 去外括号后返回
function stripOuterParens(tok: string): string {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s: string): string[] {
  // 按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符
  const parts: string[] = [];
  let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (d === 0 && '+-*/'.includes(ch)) {
      // 处理 1e-3：'-' 前是 e/E 时不作为操作符（不切分）
      if ((ch === '-' || ch === '+') && /[eE]/.test(s[i - 1] ?? '')) { cur += ch; continue; }
      parts.push(cur.trim());
      cur = '';
    } else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur.trim());
  return parts.filter((x) => x !== '');
}

function countTopLevelOps(tok: string): number {
  return splitArithTopLevel(stripOuterParens(tok)).length - 1;
}

function isArithExpr(meta: NLMeta, tok: string): boolean {
  const parts = splitArithTopLevel(stripOuterParens(tok));
  if (parts.length < 2) return false;
  if (countTopLevelOps(tok) > ARITH_MAX_OPS) return false;
  return parts.every((p) => isSeriesExpr(meta, p) || isNumber(p) || isArithExpr(meta, p));
}

function isSeriesExpr(meta: NLMeta, tok: string): boolean {
  if (meta.fields.includes(tok)) return true;
  // 1. 平衡外括号剥离：'(CLOSE-OPEN)' → 'CLOSE-OPEN'（后端 ast 对括号透明，前端需显式剥）
  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 函数调用路径：仅当 call 的闭合括号恰在末尾（纯调用，尾部无残留）
  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(/.exec(tok);
  if (mm) {
    const sig = meta.signatures?.[mm[1]];
    const openIdx = mm.index + mm[0].length - 1;
    if (sig && !sig.includes('cond')) {
      const closeIdx = matchParen(tok, openIdx);
      if (closeIdx === tok.length - 1) {
        const argStr = tok.slice(openIdx + 1, closeIdx);
        const args = splitTopLevel(argStr, ',').map((s) => s.trim());
        if (validateCallArgs(meta, sig, args, mm[1]).ok === true) return true;
      }
    }
  }
  // 3. 算术表达式路径：函数调用路径不匹配（非纯调用/尾部有残留/非调用）时尝试
  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}
```

注意：新增的 `splitArithTopLevel` 等函数放在 `isNumber`（L213-215）之后、`buildSystemPrompt`（L217）之前，或紧跟 `isSeriesExpr` 附近均可；函数声明提升使调用顺序无关。`isArithExpr` 递归引用 `isSeriesExpr`/`isNumber`，须确保两者在模块作用域可见（都是同文件函数，满足）。

- [ ] **Step 4: 运行测试确认通过（绿）**

Run: `node --test tests/select-nl.test.mjs`（在 `frontend/` 目录）
Expected: 既有 42 tests + 新增 9 tests 全 PASS。

- [ ] **Step 5: 类型检查**

Run: `npx tsc --noEmit`（在 `frontend/` 目录）
Expected: 无类型错误。

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/selectNL.ts frontend/tests/select-nl.test.mjs
git commit -m "feat: support +-*/ arithmetic in DSL series validation (frontend)"
```

---

### Task 4: 前端防-drift 守卫测试（可选加固）

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`

- [ ] **Step 1: 确认复制版与实现逐字一致**

先验证两个文件中 `splitArithTopLevel` / `isArithExpr`／`isSeriesExpr` 的函数体文本一致（防 drift 的守卫测试当前仓库未启用，此项仅做人工核对）：

Run（PowerShell，比对两个文件的函数体，输出应显示一致）:
```powershell
$impl = Get-Content "frontend/src/lib/selectNL.ts" -Raw
$test = Get-Content "frontend/tests/select-nl.test.mjs" -Raw
($impl -match 'function splitArithTopLevel\(s: string\): string\[\]') -and ($test -match 'function splitArithTopLevel\(s\)')
```
Expected: `True`

> 仓库既有惯例（CONTEXT.md 已注明）是复制实现而非导入。守卫测试列为"后续工作"，本次不做自动化守卫，仅人工核对该新增块一致。

- [ ] **Step 2: Commit（无代码变更则跳过）**

```bash
git status
```
Expected: 无未提交变更（上一步仅为只读核对）。若有意外差异，修正后提交。

---

### Task 5: 全量回归 + 文档收尾

**Files:**
- Modify: `docs/superpowers/plans/2026-08-14-dsl-series-arithmetic.md`（本计划）与 `docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md`（勾选/标记完成）

- [ ] **Step 1: 后端全量回归**

Run: `python -m unittest discover -s tests -v`（在 `backend/` 目录）
Expected: 全部 PASS（registry 11 + security 44+10+4 + 其他套件）。

- [ ] **Step 2: 前端全量回归**

Run: `node --test tests/select-nl.test.mjs`（在 `frontend/` 目录）然后 `npx tsc --noEmit`
Expected: 51 tests PASS + tsc 无错误。

- [ ] **Step 3: 更新 spec 状态与计划勾选**

将 `docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md` 的 `状态：待实施` 改为 `状态：已实施`；本计划所有 checkbox 勾选。

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md
git commit -m "doc: mark DSL series arithmetic plan complete"
```

- [ ] **Step 5: 推送（如需）**

Run: `git push origin main`
Expected: 推送成功（走全局代理）。

---

## 验证命令汇总

- 后端单测：`python -m unittest discover -s tests -v`（`backend/`）
- 前端单测：`node --test tests/select-nl.test.mjs`（`frontend/`）
- 前端类型：`npx tsc --noEmit`（`frontend/`）

## 风险与已知限制

- `ast.get_source_segment` 依赖 `self.current_source` 与节点源自同一源码串；节点若来自缓存 AST（当前无），会退化到整树计数（更保守）。
- 前端 `splitArithTopLevel` 对无空格 `5e9`/`1e-3` 依赖 e/E 前缀保护；若未来出现字段名含 e/E 且相邻 `-`，需复核。当前字段白名单无此情况。
- 除零运行时产良 polars null；前端仅结构校验，不计算数值。
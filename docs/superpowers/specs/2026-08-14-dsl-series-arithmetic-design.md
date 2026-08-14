# Spec: DSL series 位置支持算术表达式（+ - * /）

日期：2026-08-14
状态：待实施

## 背景与问题

select-nl 切到 Nemotron 3.5 Lightning 后，模型常输出 `ABS(REF(CLOSE,1) - REF(CLOSE,2))`、`(CLOSE-OPEN)/CLOSE > 0.05` 这类含算术的公式，但 DSL 的 `series` 形态校验只接受「字段名」或「签名不含 cond 的单值算子调用」，BinOp 一律被拒：

- 后端 `backend/core/security.py::_require_series`（L133-142）抛 `Function {func} arg must be a field or single-value indicator call`
- 前端 `frontend/src/lib/selectNL.ts::isSeriesExpr`（L182-194）返回 false，进而 `validateFormula` 报「函数 X 参数 … 必须是字段或窗口指标调用」

导致大量本可表达的选股需求被 `INVALID_FORMULA` 拒绝。本 spec 扩展 series 形态以接受 `+ - * /` 算术表达式，前后端语义一致。

## 现状要点

- 后端 `BlinkParser._visit`（security.py L86-87）**已支持 BinOp 求值**（`operators` 字典含 Add/Sub/Mult/Div），卡点仅在 `_require_series` 的结构校验。
- `_require_cond` / `_require_series_operand`（L167-173）对比较操作数也是委托 `_require_series`，故 cond 比较两侧的算术会随 series 放开自动生效。
- 前端 `isCompareExpr`（L202-211）的 left 走 `isSeriesExpr`，同样自动受益。
- 前端实现与测试文件（`frontend/tests/select-nl.test.mjs`）各有一份函数副本，必须同步修改，防止 drift。

## 设计决策

- 方案：在 series 校验函数中新增 BinOp 分支（方案 A），而非新增独立签名类型。
- 允许的操作符：`+ - * /`（`ast.Add/Sub/Mult/Div`）。不支持 `// % **` 位运算等。
- 算术规模上限：**一个 series 算术表达式的顶层算术运算符数 ≤ 3（即顶层最多 4 个操作数）**。
  - 「顶层」= 括号外一次切分命中的运算符；括号内子表达式在递归时**独立**适用同一规则（括号内运算符不计入父级上限）。
  - 例：`ABS((REF(CLOSE,1)-REF(CLOSE,2))/REF(CLOSE,2))` 顶层 1 个运算符（`/`），合法；`CLOSE/CLOSE/CLOSE/CLOSE/CLOSE` 顶层 4 个，拒绝。
  - 工程要点：后端 AST 中括号不产生节点（`(A-B)/C` 与 `A-B/C` 的 AST 形状不同），无法从 AST 区分顶层。故**后端也基于源码文本做顶层切分计数**（`ast.get_source_segment` 提取节点源码片段），与前端字符串切分规则完全一致，保证前后端结论一致。
- 出现在所有 series 位置：非 window 算子（ABS/MAX/MIN/CROSS_UP/CROSS_DOWN/RSI/BOLL_UPPER/BOLL_LOWER 的 series 参数）、cond 比较两侧。
- **窗口算子（MA/REF/HHV/LLV/SUM/EMA/STD/ROC）的 field 参数保持不变**：仍只接受字段白名单，不允许 `MA(CLOSE-OPEN, 20)`。窗口算子求值依赖 Hot-JIT 挂载的整列语义，放开会破坏现有优化，本轮不做。
- 数值常量作为 BinOp 操作数：允许（如 `CLOSE * 1.1`）。
- 除零/单位换算：不特殊处理。除零运行时产出 Polars null；单位纪律由提示词负责，本轮不改提示词。

## 后端改动（backend/core/security.py）

`parse_expression` 记录源文本（供 `ast.get_source_segment` 取节点源码片段）：

```python
def parse_expression(self, expr_str: str, timeframe: str = 'D') -> pl.Expr:
    ...
    clean_expr = re.sub(...)
    self.current_source = clean_expr          # 新增：保存清洗后源码
    tree = ast.parse(clean_expr, mode='eval')
    return self._visit(tree.body)
```

`_require_series`（L133-142）增加 BinOp 分支：

```python
def _require_series(self, node, func):
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

新增顶层切分工具与 `_require_arith`：

```python
ARITH_MAX_OPS = 3

def _split_arith_top_level(text: str) -> list[str]:
    """按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 - 不算操作符。与前端 splitArithTopLevel 语义一致。"""
    parts: list[str] = []
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
    return [p for p in parts if p]

def _strip_outer_parens(text: str) -> str:
    t = text.strip()
    while t.startswith('(') and t.endswith(')') and _balanced_outer(t):
        t = t[1:-1].strip()
    return t

def _balanced_outer(t: str) -> bool:
    depth = 0
    for i, ch in enumerate(t):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth == 0:
            return i == len(t) - 1
    return False

def _count_top_level_ops(self, node) -> int:
    """按源码文本统计顶层算术运算符数（括号内不计入）。"""
    seg = ast.get_source_segment(self.current_source, node)
    if seg is None:
        return self._count_arith_ops_tree(node)  # 兜底：退化到整树计数
    t = _strip_outer_parens(seg)
    parts = _split_arith_top_level(t)
    return max(0, len(parts) - 1)

def _require_arith(self, node, func):
    """算术表达式结构校验：操作数 = 数值常量 / series / 更浅的算术；顶层运算符数 ≤ ARITH_MAX_OPS。"""
    if self._count_top_level_ops(node) > ARITH_MAX_OPS:
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

说明：
- 操作符白名单在 BinOp 分支与 `_require_arith` 内各校验一次（外层分支只放行白名单操作符；内层递归同）。
- 非白名单 BinOp（如 `**`、`//`）在 `_require_series` 外层分支不匹配，落入最后的 raise。
- 求值仍由既有 `_visit` 的 BinOp 分支完成，`_visit` 不感知算术规模（规模由结构校验保证）。
- `_count_arith_ops_tree`（整树计数）仅作兜底，正常路径都用源码文本计数，与前端一致。

## 前端改动（frontend/src/lib/selectNL.ts）

新增算术切分与校验，并**重构 `isSeriesExpr` 的分支顺序**（算术分支必须放在函数调用路径之后但能到达的位置，否则 `REF(...)-REF(...)` 会在 `closeIdx !== len-1` 处提前 return false）：

```ts
const ARITH_MAX_OPS = 3;

// 剥离平衡外括号：'(...)' 且首括号闭合于末位 → 去外括号后返回
function stripOuterParens(tok: string): string {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s: string): string[] {
  // 按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 - 不算操作符
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
```

`isSeriesExpr` 重构（L182-194）：

```ts
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

注意：`isNumber`（L213-215）已覆盖 `-?\d+(\.\d+)?([eE][-+]?\d+)?`，含指数记号，与切分逻辑配套。

## 测试

### 后端（backend/tests/test_security.py）

新增 TestSeriesArithmetic：
1. `ABS(REF(CLOSE, 1) - REF(CLOSE, 2))` 通过且求值正确（用测试 DataFrame 核对列值）
2. `(CLOSE - OPEN) / CLOSE > 0.05` 通过（cond 比较两侧算术，顶层 1 个运算符）
3. `CLOSE * 1.1 > REF(CLOSE, 1)` 通过（常量参与，顶层 1 个运算符）
4. 顶层运算符超上限拒绝：`CLOSE / CLOSE / CLOSE / CLOSE / CLOSE`（顶层 4 个运算符）抛错
5. 括号内运算符不计入父级：`(CLOSE - OPEN) / (CLOSE / CLOSE)` 顶层 1 个运算符 `(/)`，括号内各 1 个——通过
6. 窗口 field 参数仍拒：`MA(CLOSE - OPEN, 20)` 抛错
7. `ABS(CLOSE ** 2)` 抛错（非白名单操作符）
8. `COUNT((CLOSE - OPEN) / CLOSE > 0.05, 5)` 通过（cond 参数内算术）
9. 布尔常量操作数拒绝：`ABS(CLOSE - True)` 抛错
10. `ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))` 通过（顶层 1 个运算符）

回归：既有 11（registry）+ 44（security）tests 全绿。

### 前端（frontend/tests/select-nl.test.mjs）

镜像复制版同步新增 `stripOuterParens` / `splitArithTopLevel` / `countTopLevelOps` / `isArithExpr` / `isSeriesExpr` 算术分支，然后新增：
1. `ABS(REF(CLOSE, 1) - REF(CLOSE, 2))` ok
2. `(CLOSE - OPEN) / CLOSE > 0.05` ok
3. `CLOSE * 1.1 > REF(CLOSE, 1)` ok
4. `CLOSE / CLOSE / CLOSE / CLOSE / CLOSE` 拒（顶层运算符超上限）
5. `(CLOSE - OPEN) / (CLOSE / CLOSE)` ok（括号内运算符不计入父级顶层）
6. `MA(CLOSE - OPEN, 20)` 拒（field 参数）
7. `ABS(CLOSE ** 2)` 拒
8. `ABS(CLOSE - True)` 拒（布尔操作数）
9. `ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))` ok
10. 回归：既有 42 tests 全绿。

## 涉及文件

- `backend/core/security.py`（实现）
- `backend/tests/test_security.py`（测试）
- `frontend/src/lib/selectNL.ts`（实现）
- `frontend/tests/select-nl.test.mjs`（镜像 + 测试）

## 验证命令

- 后端：`python -m unittest discover -s tests`（项目根 backend/ 下）
- 前端：`node --test tests/select-nl.test.mjs` + `npx tsc --noEmit`（frontend/ 下）

## 范围外（本轮不做）

- 窗口算子 field 参数支持算术
- 提示词强化（模型误写算术的策略留待后续，优先让 DSL 能表达）
- 新签名类型 arith

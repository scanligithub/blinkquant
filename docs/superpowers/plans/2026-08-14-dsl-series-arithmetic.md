# DSL Series 绠楁湳琛ㄨ揪寮忔敮鎸佸疄鏂借鍒?
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 璁?DSL 鐨?series 浣嶇疆鎺ュ彈 `+ - * /` 绠楁湳琛ㄨ揪寮忥紙椤跺眰杩愮畻绗?鈮?锛屾嫭鍙峰唴閫掑綊鐙珛閫傜敤锛夛紝鍓嶅悗绔牎楠岃涔変竴鑷淬€?
**Architecture:** 鍚庣 `_require_series` 鏂板 BinOp 鍒嗘敮锛岀敤 `ast.get_source_segment` 鍙栨簮鐮佺墖娈靛仛椤跺眰鍒囧垎璁℃暟锛涘墠绔?`isSeriesExpr` 閲嶆瀯鍒嗘敮椤哄簭 + 鏂板 `splitArithTopLevel`/`stripOuterParens`/`countTopLevelOps`/`isArithExpr`銆備袱渚х敤鐩稿悓鍒囧垎瑙勫垯淇濊瘉缁撹涓€鑷淬€傛眰鍊煎潎娌跨敤鏃㈡湁 `_visit`锛堝悗绔級涓庡悗绔繍琛屾椂锛屾棤鏂板姹傚€艰矾寰勩€?
**Tech Stack:** Python 3 + polars锛堝悗绔級銆乀ypeScript + Node test锛堝墠绔級銆乣ast.get_source_segment`锛圥ython 3.8+锛夈€?
---

## File Structure

| 鏂囦欢 | 鑱岃矗 | 鍔ㄤ綔 |
|---|---|---|
| `backend/core/security.py` | 鍚庣 DSL 瀹夊叏瑙ｆ瀽 | Modify锛歚parse_expression` 淇濆瓨婧愭枃鏈€乣_require_series` 鍔?BinOp 鍒嗘敮銆佹柊澧?5 涓緟鍔╁嚱鏁?|
| `backend/tests/test_security.py` | 鍚庣鍗曟祴 | Modify锛氭柊澧?TestSeriesArithmetic |
| `frontend/src/lib/selectNL.ts` | 鍓嶇鍏紡寮烘牎楠?| Modify锛歚isSeriesExpr` 閲嶆瀯 + 鏂板 4 涓緟鍔╁嚱鏁?|
| `frontend/tests/select-nl.test.mjs` | 鍓嶇鍗曟祴锛堝鍒剁増锛?| Modify锛氬鍒跺悓姝?+ 鏂板娴嬭瘯鐢ㄤ緥 |
| `docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md` | 璁捐鏂囨。 | 鍙傝€冿紙宸叉彁浜?`412e3ac`锛?|

浠诲姟椤哄簭锛氬悗绔紙T1-T3锛夆啋 鍓嶇锛圱4-T6锛夆啋 鍏ㄩ噺鍥炲綊 + 鏂囨。鏀跺熬锛圱7锛夈€?
---

### Task 1: 鍚庣鏂板绠楁湳鏍￠獙杈呭姪鍑芥暟锛圱DD锛?
**Files:**
- Modify: `backend/core/security.py`
- Test: `backend/tests/test_security.py`

- [x] **Step 1: 鍐欐寮忓け璐ユ祴璇?TestSeriesArithmetic**

鍦?`backend/tests/test_security.py` 鏈熬杩藉姞娴嬭瘯绫伙紙瑕嗙洊 spec 鍏ㄩ儴楠屾敹鐐癸紱娉ㄦ剰椤跺眰姣旇緝/椤跺眰绠楁湳鍓嶇涓庡悗绔潎涓嶅仛鎿嶄綔鏁版牎楠岋紝鏁呰秴闄愮敤渚?*鍖呰繘 series 浣嶇疆 `ABS(...)`** 鎵嶈兘瑙﹀彂鏍￠獙锛夛細

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
        # REF1: [null,10,11,12]  REF2: [null,null,10,11]  宸? [null,null,1,1]
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
        # 椤跺眰绠楁湳鍓嶇/鍚庣鍧囦笉鏍￠獙锛岄』鍖呰繘 series 浣嶇疆瑙﹀彂 _require_series 鈫?_require_arith
        with self.assertRaises(ValueError):
            self.eval_expr("ABS(CLOSE / CLOSE / CLOSE / CLOSE / CLOSE)")

    def test_paren_inner_ops_not_counted_in_parent(self):
        # 椤跺眰 1 涓繍绠楃(*)锛屾嫭鍙峰唴鍚?1 涓紱鏁村紡鍏?5 涓繍绠楃鈥斺€旇嫢鎸夋暣鏍戣鏁颁細璇嫆锛屾寜椤跺眰璁℃暟搴旀斁琛?        expr = self.eval_expr("ABS(((CLOSE - OPEN) / (CLOSE / CLOSE)) * 2)")
        got = self.values(expr)
        # close/close=1 鈫?(close-open)/1*2 = close-open 鐨?2 鍊? [2,1,1,1]
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
        # cond: [T,F,F,F] 鈫?rolling_sum(2)=[null,1,0,0]
        self.assertEqual(got, [None, 1, 0, 0])

    def test_bool_operand_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_expr("ABS(CLOSE - True)")

    def test_abs_nested_paren_passes(self):
        expr = self.eval_expr("ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))")
        got = self.values(expr)
        # (REF1-REF2)/REF2: day3 (11-10)/10=0.1; day4 (12-11)/11=0.0909 鈫?abs same
        self.assertAlmostEqual(got[2], 0.1, places=6)
        self.assertAlmostEqual(got[3], 0.090909, places=6)
```

- [x] **Step 2: 杩愯纭澶辫触锛堢孩锛?*

Run: `python -m unittest tests.test_security.TestSeriesArithmetic -v`锛堝湪 `backend/` 鐩綍锛?Expected: 澶氭暟鐢ㄤ緥 FAIL锛坄ValueError: Function ABS arg must be a field or single-value indicator call`锛夛紱`test_window_field_param_still_rejected` 鍗曠嫭 PASS锛堟湰灏辨嫆缁濓級銆?
- [x] **Step 3: 瀹炵幇鍚庣绠楁湳鏍￠獙**

鍦?`backend/core/security.py` 淇敼/鏂板锛?
1. `parse_expression`锛圠63-73锛夊湪 `tree = ast.parse(...)` 鍓嶄繚瀛樻簮鏂囨湰锛?
```python
clean_expr = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), expr_str.strip())
clean_expr = clean_expr.replace('&&', '&').replace('||', '|')
self.current_source = clean_expr
tree = ast.parse(clean_expr, mode='eval')
```

2. `__init__`锛圠60-61 闄勮繎锛夊垵濮嬪寲 `current_source`锛?
```python
self.current_df = None
self.current_source = None
```

3. 妯″潡甯搁噺锛坄WINDOW_MAX` 涓嬫柟锛夊姞锛?
```python
ARITH_MAX_OPS = 3
```

4. 妯″潡绾у伐鍏峰嚱鏁帮紙`_require_positive_int` 涓?`class BlinkParser` 涔嬮棿鎻掑叆锛夛細

```python
def _split_arith_top_level(text: str) -> list:
    """鎸?+ - * / 鍦ㄦ嫭鍙峰鎷嗗垎锛沞/E 鎸囨暟璁板彿锛?e9銆?e-3锛夌殑 -/+ 涓嶇畻鎿嶄綔绗︺€備笌鍓嶇 splitArithTopLevel 璇箟涓€鑷淬€?""
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
    """棣栨嫭鍙锋槸鍚﹂厤瀵瑰苟闂悎浜庢湯浣嶃€?""
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

5. `blink_parser` 鍗曚緥鍐呯殑 `BlinkParser` 鏂规硶锛宍_require_series`锛圠133-142锛夊鍔?BinOp 鍒嗘敮骞惰皟鐢?`_require_arith`锛?
```python
def _require_series(self, node: Any, func: str) -> Any:
    """series = 鐧藉悕鍗曞瓧娈?鎴?绛惧悕涓嶅惈 cond 褰㈡€佺殑绠楀瓙璋冪敤锛堝惈绐楀彛/闈炵獥鍙ｏ級 鎴?+-*/ 绠楁湳琛ㄨ揪寮忋€?""
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

鏂板鏂规硶锛堟斁鍦?`_require_series` 鍚庨潰銆乣_require_cond` 鍓嶏級锛?
```python
def _top_level_ops(self, node) -> int:
    """鎸夋簮鐮佹枃鏈粺璁￠《灞傜畻鏈繍绠楃鏁帮紙鎷彿鍐呬笉璁″叆锛夈€俫et_source_segment 澶辫触鏃堕€€鍖栦负鏁存爲璁℃暟鍏滃簳銆?""
    seg = ast.get_source_segment(self.current_source, node) if isinstance(self.current_source, str) else None
    if seg is not None:
        t = _strip_outer_parens(seg)
        parts = _split_arith_top_level(t)
        return max(0, len(parts) - 1)
    return self._arith_ops_tree(node)

def _arith_ops_tree(self, node) -> int:
    """鍏滃簳锛氭暣妫?BinOp 瀛愭爲杩愮畻绗︽€绘暟銆?""
    if not isinstance(node, ast.BinOp) or type(node.op) not in (ast.Add, ast.Sub, ast.Mult, ast.Div):
        return 0
    return 1 + self._arith_ops_tree(node.left) + self._arith_ops_tree(node.right)

def _require_arith(self, node: Any, func: str) -> Any:
    """绠楁湳琛ㄨ揪寮忕粨鏋勬牎楠岋細鎿嶄綔鏁?= 鏁板€煎父閲?/ series / 鏇存祬鐨勭畻鏈紱椤跺眰杩愮畻绗︽暟 鈮?ARITH_MAX_OPS銆?""
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

- [x] **Step 4: 杩愯娴嬭瘯纭閫氳繃锛堢豢锛?*

Run: `python -m unittest tests.test_security -v`锛堝湪 `backend/` 鐩綍锛?Expected: 鏃㈡湁 44 tests + 鏂板 10 tests 鍏?PASS銆?
- [x] **Step 5: 杩愯鍏ㄩ噺鍚庣娴嬭瘯纭鏃犲洖褰?*

Run: `python -m unittest discover -s tests -v`锛堝湪 `backend/` 鐩綍锛?Expected: 鎵€鏈夋祴璇?PASS锛坮egistry 11 + security 44 + 鏂板 10 + 鍏朵粬鏃㈡湁濂椾欢锛夈€?
- [x] **Step 6: Commit**

```bash
git add backend/core/security.py backend/tests/test_security.py
git commit -m "feat: support +-*/ arithmetic in DSL series positions (backend)"
```

---

### Task 2: 鍚庣杈圭晫鈥斺€旈《灞傚垏鍒嗗伐鍏峰崟娴嬶紙鍙€変絾鎺ㄨ崘锛?
**Files:**
- Modify: `backend/tests/test_security.py`
- (鏃犲疄鐜版敼鍔?

- [x] **Step 1: 鍐欓《灞傚垏鍒嗗伐鍏风殑鐩存帴鍗曟祴**

鍦?`backend/tests/test_security.py` 椤堕儴 imports 杩藉姞 `from core.security import _split_arith_top_level, _strip_outer_parens`锛屾湯灏捐拷鍔狅細

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

- [x] **Step 2: 杩愯纭閫氳繃**

Run: `python -m unittest tests.test_security.TestArithSplitHelpers -v`锛堝湪 `backend/` 鐩綍锛?Expected: 4 tests PASS銆?
- [x] **Step 3: Commit**

```bash
git add backend/tests/test_security.py
git commit -m "test: splitArithTopLevel/stripOuterParens backend helpers"
```

---

### Task 3: 鍓嶇 selectNL.ts 绠楁湳鏍￠獙锛圱DD锛?
**Files:**
- Modify: `frontend/src/lib/selectNL.ts`
- Test: `frontend/tests/select-nl.test.mjs`锛堝鍒剁増锛?
- [x] **Step 1: 鍐欏け璐ユ祴璇曪紙澶嶅埗鐗堝嚱鏁版殏淇濇寔鏃у疄鐜帮紝鍙柊澧炴祴璇曠敤渚嬶級**

鍦?`frontend/tests/select-nl.test.mjs` 鐨?`// ---- 澶嶅埗缁撴潫 ----` 涔嬪悗杩藉姞娴嬭瘯鐢ㄤ緥銆?*娉ㄦ剰**锛歚validateFormula` 鍙牎楠屽嚱鏁拌皟鐢ㄧ鍚嶄笌鏍囪瘑绗︾櫧鍚嶅崟锛屼笉鏍￠獙椤跺眰姣旇緝/椤跺眰绠楁湳鐨勬搷浣滄暟锛屽洜姝ゆ墍鏈夌畻鏈敤渚嬪繀椤诲寘杩?series 鎴?cond 浣嶇疆锛坄ABS(...)`銆乣COUNT(...,5)`锛夋墠鑳借Е鍙?`isSeriesExpr`/`isArithExpr`锛?
```javascript
test('validateFormula: ABS 绠楁湳鍙傛暟閫氳繃', () => {
  assert.equal(validateFormula(META, 'ABS(REF(CLOSE, 1) - REF(CLOSE, 2))').ok, true);
});

test('validateFormula: cond 鎷彿绠楁湳姣旇緝閫氳繃', () => {
  assert.equal(validateFormula(META, 'COUNT((CLOSE - OPEN) / CLOSE > 0.05, 5)').ok, true);
});

test('validateFormula: cond 甯搁噺涔樻硶閫氳繃', () => {
  assert.equal(validateFormula(META, 'COUNT(CLOSE * 1.1 > REF(CLOSE, 1), 5)').ok, true);
});

test('validateFormula: 椤跺眰杩愮畻绗﹁秴涓婇檺鎷掔粷', () => {
  const r = validateFormula(META, 'ABS(CLOSE / CLOSE / CLOSE / CLOSE / CLOSE)');
  assert.equal(r.ok, false);
});

test('validateFormula: 鎷彿鍐呰繍绠楃涓嶈鍏ョ埗绾ч《灞?, () => {
  assert.equal(validateFormula(META, 'ABS(((CLOSE - OPEN) / (CLOSE / CLOSE)) * 2)').ok, true);
});

test('validateFormula: 绐楀彛 field 鍙傛暟绠楁湳鎷掔粷', () => {
  const r = validateFormula(META, 'MA(CLOSE - OPEN, 20)');
  assert.equal(r.ok, false);
});

test('validateFormula: 骞傝繍绠楃鎷掔粷', () => {
  const r = validateFormula(META, 'ABS(CLOSE ** 2)');
  assert.equal(r.ok, false);
});

test('validateFormula: 甯冨皵鎿嶄綔鏁版嫆缁?, () => {
  const r = validateFormula(META, 'ABS(CLOSE - True)');
  assert.equal(r.ok, false);
});

test('validateFormula: 宓屽鎷彿绠楁湳閫氳繃', () => {
  assert.equal(
    validateFormula(META, 'ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))').ok,
    true
  );
});
```

- [x] **Step 2: 杩愯纭澶辫触锛堢孩锛?*

Run: `node --test tests/select-nl.test.mjs`锛堝湪 `frontend/` 鐩綍锛?Expected: 鏂板 9 涓敤渚?FAIL锛堟棫澶嶅埗鐗?`isSeriesExpr` 鏃犵畻鏈垎鏀細`ABS(REF...)` 浣滀负 series 鍙傛暟鏃?`closeIdx !== length-1` 鎻愬墠 return false锛沗MA(CLOSE-OPEN,20)`銆乣ABS(CLOSE ** 2)` 鍥?field/pos_int 鏍￠獙鏈?PASS 闄ゅ鈥斺€旂‘璁よ嚦灏戦€氳繃绫荤敤渚嬮泦浣撶孩锛夈€?
- [x] **Step 3: 鍚屾鏂板澶嶅埗鐗堝嚱鏁?+ 淇敼瀹炵幇 selectNL.ts**

3a. 鍦?`frontend/tests/select-nl.test.mjs` 鐨勯暅鍍忓鍒跺尯锛坄isNumber` 鍑芥暟鍚庯級鏂板 4 涓嚱鏁帮紝骞舵妸 `isSeriesExpr` 鏇挎崲涓洪噸鏋勭増锛?
```javascript
const ARITH_MAX_OPS = 3;

function stripOuterParens(tok) {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s) {
  // 鎸?+ - * / 鍦ㄦ嫭鍙峰鎷嗗垎锛沞/E 鎸囨暟璁板彿锛?e9銆?e-3锛夌殑 -/+ 涓嶇畻鎿嶄綔绗?  const parts = [];
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
  // 1. 骞宠　澶栨嫭鍙峰墺绂?  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 鍑芥暟璋冪敤璺緞锛氫粎褰?call 鐨勯棴鍚堟嫭鍙锋伆鍦ㄦ湯灏?  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(/.exec(tok);
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
  // 3. 绠楁湳琛ㄨ揪寮忚矾寰?  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}
```

3b. 鍦?`frontend/src/lib/selectNL.ts` 涓墽琛屼笌涓婅堪澶嶅埗鐗堥€愬瓧涓€鑷寸殑鏀瑰姩锛?- 椤堕儴甯搁噺鍖哄姞 `const ARITH_MAX_OPS = 3;`
- 鏂板 `stripOuterParens` / `splitArithTopLevel` / `countTopLevelOps` / `isArithExpr`
- 灏?`isSeriesExpr`锛圠182-194锛夋浛鎹负閲嶆瀯鐗堟湰锛堝惈绫诲瀷娉ㄨВ `NLMeta`锛?
```typescript
const ARITH_MAX_OPS = 3;

// 鍓ョ骞宠　澶栨嫭鍙凤細'(...)' 涓旈鎷彿闂悎浜庢湯浣?鈫?鍘诲鎷彿鍚庤繑鍥?function stripOuterParens(tok: string): string {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s: string): string[] {
  // 鎸?+ - * / 鍦ㄦ嫭鍙峰鎷嗗垎锛沞/E 鎸囨暟璁板彿锛?e9銆?e-3锛夌殑 -/+ 涓嶇畻鎿嶄綔绗?  const parts: string[] = [];
  let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (d === 0 && '+-*/'.includes(ch)) {
      // 澶勭悊 1e-3锛?-' 鍓嶆槸 e/E 鏃朵笉浣滀负鎿嶄綔绗︼紙涓嶅垏鍒嗭級
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
  // 1. 骞宠　澶栨嫭鍙峰墺绂伙細'(CLOSE-OPEN)' 鈫?'CLOSE-OPEN'锛堝悗绔?ast 瀵规嫭鍙烽€忔槑锛屽墠绔渶鏄惧紡鍓ワ級
  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 鍑芥暟璋冪敤璺緞锛氫粎褰?call 鐨勯棴鍚堟嫭鍙锋伆鍦ㄦ湯灏撅紙绾皟鐢紝灏鹃儴鏃犳畫鐣欙級
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
  // 3. 绠楁湳琛ㄨ揪寮忚矾寰勶細鍑芥暟璋冪敤璺緞涓嶅尮閰嶏紙闈炵函璋冪敤/灏鹃儴鏈夋畫鐣?闈炶皟鐢級鏃跺皾璇?  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}
```

娉ㄦ剰锛氭柊澧炵殑 `splitArithTopLevel` 绛夊嚱鏁版斁鍦?`isNumber`锛圠213-215锛変箣鍚庛€乣buildSystemPrompt`锛圠217锛変箣鍓嶏紝鎴栫揣璺?`isSeriesExpr` 闄勮繎鍧囧彲锛涘嚱鏁板０鏄庢彁鍗囦娇璋冪敤椤哄簭鏃犲叧銆俙isArithExpr` 閫掑綊寮曠敤 `isSeriesExpr`/`isNumber`锛岄』纭繚涓よ€呭湪妯″潡浣滅敤鍩熷彲瑙侊紙閮芥槸鍚屾枃浠跺嚱鏁帮紝婊¤冻锛夈€?
- [x] **Step 4: 杩愯娴嬭瘯纭閫氳繃锛堢豢锛?*

Run: `node --test tests/select-nl.test.mjs`锛堝湪 `frontend/` 鐩綍锛?Expected: 鏃㈡湁 42 tests + 鏂板 9 tests 鍏?PASS銆?
- [x] **Step 5: 绫诲瀷妫€鏌?*

Run: `npx tsc --noEmit`锛堝湪 `frontend/` 鐩綍锛?Expected: 鏃犵被鍨嬮敊璇€?
- [x] **Step 6: Commit**

```bash
git add frontend/src/lib/selectNL.ts frontend/tests/select-nl.test.mjs
git commit -m "feat: support +-*/ arithmetic in DSL series validation (frontend)"
```

---

### Task 4: 鍓嶇闃?drift 瀹堝崼娴嬭瘯锛堝彲閫夊姞鍥猴級

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`

- [x] **Step 1: 纭澶嶅埗鐗堜笌瀹炵幇閫愬瓧涓€鑷?*

鍏堥獙璇佷袱涓枃浠朵腑 `splitArithTopLevel` / `isArithExpr`锛廯isSeriesExpr` 鐨勫嚱鏁颁綋鏂囨湰涓€鑷达紙闃?drift 鐨勫畧鍗祴璇曞綋鍓嶄粨搴撴湭鍚敤锛屾椤逛粎鍋氫汉宸ユ牳瀵癸級锛?
Run锛圥owerShell锛屾瘮瀵逛袱涓枃浠剁殑鍑芥暟浣擄紝杈撳嚭搴旀樉绀轰竴鑷达級:
```powershell
$impl = Get-Content "frontend/src/lib/selectNL.ts" -Raw
$test = Get-Content "frontend/tests/select-nl.test.mjs" -Raw
($impl -match 'function splitArithTopLevel\(s: string\): string\[\]') -and ($test -match 'function splitArithTopLevel\(s\)')
```
Expected: `True`

> 浠撳簱鏃㈡湁鎯緥锛圕ONTEXT.md 宸叉敞鏄庯級鏄鍒跺疄鐜拌€岄潪瀵煎叆銆傚畧鍗祴璇曞垪涓?鍚庣画宸ヤ綔"锛屾湰娆′笉鍋氳嚜鍔ㄥ寲瀹堝崼锛屼粎浜哄伐鏍稿璇ユ柊澧炲潡涓€鑷淬€?
- [x] **Step 2: Commit锛堟棤浠ｇ爜鍙樻洿鍒欒烦杩囷級**

```bash
git status
```
Expected: 鏃犳湭鎻愪氦鍙樻洿锛堜笂涓€姝ヤ粎涓哄彧璇绘牳瀵癸級銆傝嫢鏈夋剰澶栧樊寮傦紝淇鍚庢彁浜ゃ€?
---

### Task 5: 鍏ㄩ噺鍥炲綊 + 鏂囨。鏀跺熬

**Files:**
- Modify: `docs/superpowers/plans/2026-08-14-dsl-series-arithmetic.md`锛堟湰璁″垝锛変笌 `docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md`锛堝嬀閫?鏍囪瀹屾垚锛?
- [x] **Step 1: 鍚庣鍏ㄩ噺鍥炲綊**

Run: `python -m unittest discover -s tests -v`锛堝湪 `backend/` 鐩綍锛?Expected: 鍏ㄩ儴 PASS锛坮egistry 11 + security 44+10+4 + 鍏朵粬濂椾欢锛夈€?
- [x] **Step 2: 鍓嶇鍏ㄩ噺鍥炲綊**

Run: `node --test tests/select-nl.test.mjs`锛堝湪 `frontend/` 鐩綍锛夌劧鍚?`npx tsc --noEmit`
Expected: 51 tests PASS + tsc 鏃犻敊璇€?
- [x] **Step 3: 鏇存柊 spec 鐘舵€佷笌璁″垝鍕鹃€?*

灏?`docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md` 鐨?`鐘舵€侊細寰呭疄鏂絗 鏀逛负 `鐘舵€侊細宸插疄鏂絗锛涙湰璁″垝鎵€鏈?checkbox 鍕鹃€夈€?
- [x] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-08-14-dsl-series-arithmetic-design.md
git commit -m "doc: mark DSL series arithmetic plan complete"
```

- [x] **Step 5: 鎺ㄩ€侊紙濡傞渶锛?*

Run: `git push origin main`
Expected: 鎺ㄩ€佹垚鍔燂紙璧板叏灞€浠ｇ悊锛夈€?
---

## 楠岃瘉鍛戒护姹囨€?
- 鍚庣鍗曟祴锛歚python -m unittest discover -s tests -v`锛坄backend/`锛?- 鍓嶇鍗曟祴锛歚node --test tests/select-nl.test.mjs`锛坄frontend/`锛?- 鍓嶇绫诲瀷锛歚npx tsc --noEmit`锛坄frontend/`锛?
## 椋庨櫓涓庡凡鐭ラ檺鍒?
- `ast.get_source_segment` 渚濊禆 `self.current_source` 涓庤妭鐐规簮鑷悓涓€婧愮爜涓诧紱鑺傜偣鑻ユ潵鑷紦瀛?AST锛堝綋鍓嶆棤锛夛紝浼氶€€鍖栧埌鏁存爲璁℃暟锛堟洿淇濆畧锛夈€?- 鍓嶇 `splitArithTopLevel` 瀵规棤绌烘牸 `5e9`/`1e-3` 渚濊禆 e/E 鍓嶇紑淇濇姢锛涜嫢鏈潵鍑虹幇瀛楁鍚嶅惈 e/E 涓旂浉閭?`-`锛岄渶澶嶆牳銆傚綋鍓嶅瓧娈电櫧鍚嶅崟鏃犳鎯呭喌銆?- 闄ら浂杩愯鏃朵骇鑹?polars null锛涘墠绔粎缁撴瀯鏍￠獙锛屼笉璁＄畻鏁板€笺€

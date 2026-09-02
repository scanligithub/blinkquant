# 涨停/跌停按板别确定性翻译 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让「涨停/跌停」口语按股票所属板别正确翻译（主板10% / 科创创业20% / 北交30%），通过新增派生字段 `LIMIT_UP_PCT` + 提示词硬约束 + 确定性改写实现。

**Architecture:** 后端注册表新增白名单字段 `LIMIT_UP_PCT`，`security.py` 按 `code` 前缀实时算出每行限幅；前端提示词教导用 `PCT_CHG >= LIMIT_UP_PCT`（涨停）/`PCT_CHG <= 0 - LIMIT_UP_PCT`（跌停），新增 `trySafeLimitUpDownRewrite` 兜住 LLM 写死 10/20/30 的非法形态。

**Tech Stack:** Python 3 + polars 1.39 + unittest（后端，工作目录 backend）；Node 20 + node:test + TypeScript（前端，工作目录 frontend）。

**测试命令**：
- 后端：`python -m unittest discover -s tests`（工作目录 `backend`，**无 pytest**）
- 前端：`node --test tests/**/*.mjs`（工作目录 `frontend`，**勿用 `node --test tests`**）
- 类型检查：`npx --yes -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop src/lib/selectNL.ts`（工作目录 `frontend`）
- 在线跑批：`node scripts/nl-test.mjs "https://blinkquant.de5.net" "1@1.com" "22222222"`（需要 `NL_TEST_MODE=true` 豁免限流）

**基准**：本计划前全量后端 98 测试、前端 177 测试全绿；HEAD `5387711`。

---

### Task 1: 后端注册表新增 LIMIT_UP_PCT 字段

**Files:**
- Modify: `backend/core/indicator_registry.py:397-401`（FIELDS）
- Modify: `backend/core/indicator_registry.py:404-411`（UNITS）
- Modify: `backend/core/indicator_registry.py:452-465`（EXAMPLE_QUERIES）

- [ ] **Step 1: 修改 FIELDS list 追加 LIMIT_UP_PCT**

将 `backend/core/indicator_registry.py:397-401`：

```python
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]
```

改为（TURN 后追加）：

```python
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
    "LIMIT_UP_PCT",
]
```

- [ ] **Step 2: UNITS 追加 LIMIT_UP_PCT**

在 `backend/core/indicator_registry.py:404-411` 的 UNITS dict 里，`"FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)"` 后追加一项：

```python
    "LIMIT_UP_PCT": "百分比(%)",
```

- [ ] **Step 3: EXAMPLE_QUERIES 追加涨停示例**

在 `backend/core/indicator_registry.py:464` 的 `"CLOSE > SAR()",` 后追加：

```python
    "PCT_CHG >= LIMIT_UP_PCT",
```

- [ ] **Step 4: 运行后端测试确认红**

Run (workdir `backend`): `python -m unittest discover -s tests -v`
Expected: `test_fields_match_registry`（test_security.py:92）FAIL —— FIELDS 多了 `LIMIT_UP_PCT`，但 `blink_parser.fields` 还没加 → 键集不一致。

- [ ] **Step 5: 提交**

```bash
git add backend/core/indicator_registry.py
git commit -m "test(backend): 注册表预留 LIMIT_UP_PCT 字段触发失败测试"
```

> 注：正常实现流程应测试先行全红再实现（见 Task 2）。此处 Step 4 的红是 Task 2 实现的前置状态。

---

### Task 2: security.py 实现 LIMIT_UP_PCT 派生 + 通过测试

**Files:**
- Modify: `backend/core/security.py:6`（import 区）
- Modify: `backend/core/security.py:76-106`（BlinkParser.fields）
- Test: `backend/tests/test_security.py`

- [ ] **Step 1: 新建 module 级派生函数**

在 `backend/core/security.py:7`（`from .indicator_registry import ...` 之后）插入：

```python
def _limit_up_pct_expr():
    """按 code 前缀计算每行涨停幅度：科创(688/689)/创业(30*) → 20，北交所(bj.*) → 30，其余(沪深主板) → 10。

    2026-07-06 起主板 ST 亦 10%（与普通股一致），故仅按代码判板别即可。
    """
    return pl.when(
        pl.col("code").str.starts_with("sh.688")
        | pl.col("code").str.starts_with("sh.689")
        | pl.col("code").str.starts_with("sz.30")
    ).then(pl.lit(20.0)).when(
        pl.col("code").str.starts_with("bj.")
    ).then(pl.lit(30.0)).otherwise(pl.lit(10.0))
```

- [ ] **Step 2: fields dict 追加映射**

在 `backend/core/security.py:105`（`'TURN': pl.col('turn'),`）后追加：

```python
            'LIMIT_UP_PCT': _limit_up_pct_expr(),
```

- [ ] **Step 3: 写失败测试**

在 `backend/tests/test_security.py` 末尾、`if __name__ == "__main__":` 之前追加：

```python
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
```

> 注：`test_limit_down_translation` 把每行 `pctChg` 取负构造跌停场景，验证 `0 - LIMIT_UP_PCT` 语法合法且语义正确。

- [ ] **Step 4: 运行测试确认红**

Run (workdir `backend`): `python -m unittest discover -s tests -v`
Expected: `TestLimitUpPct` 三例 FAIL（`LIMIT_UP_PCT` 未在 fields → 解析为 `pl.col("limit_up_pct")` 但列不存在 → 空/报错）。

- [ ] **Step 5: 运行测试确认绿**

Run (workdir `backend`): `python -m unittest discover -s tests -v`
Expected: 全部 PASS，含 `test_fields_match_registry`（键集一致）与 `test_units_cover_all_fields`（UNITS 已加）。

- [ ] **Step 6: 提交**

```bash
git add backend/core/security.py backend/tests/test_security.py
git commit -m "feat(backend): LIMIT_UP_PCT 派生字段（按 code 前缀判板别 10/20/30）
```

---

### Task 3: 前端 selectNL.ts 提示词 + 确定性改写

**Files:**
- Modify: `frontend/src/lib/selectNL.ts:327-365`（buildSystemPrompt 易错模式）
- Modify: `frontend/src/lib/selectNL.ts:440-459`（buildHardConstraintSuffix）
- Modify: `frontend/src/lib/selectNL.ts:629-634`（buildAnalyzePrompt 歧义术语）
- Modify: `frontend/src/lib/selectNL.ts:590`（trySafeNumericCrossRewrite 之后追加新函数）
- Modify: `frontend/src/app/api/select-nl/route.ts:91-94`（改写链）

- [ ] **Step 1: buildSystemPrompt 易错模式追加第 12 条**

在 `frontend/src/lib/selectNL.ts:356`（`'11) CCI 突破用 CCI(N) > 100...'`）后追加：

```ts
    '12) 涨停/跌停：涨停 = PCT_CHG >= LIMIT_UP_PCT；跌停 = PCT_CHG <= 0 - LIMIT_UP_PCT。',
    '   禁止写死 10/20/30（各板限幅不同：主板10 科创/创业20 北交30，必须用 LIMIT_UP_PCT 字段）。',
```

（两个 `'` 引号闭合字符串，保持现有数组元素风格。）

- [ ] **Step 2: buildHardConstraintSuffix 追加涨停/跌停约束**

在 `frontend/src/lib/selectNL.ts:455-457`（`if (/较高者|.../` 块）之后追加：

```ts
  if (/涨停|跌停|封板|一字板/.test(text)) {
    lines.push('硬约束：本需求含涨停/跌停语义，必须使用 PCT_CHG 与 LIMIT_UP_PCT 比较（涨停 PCT_CHG >= LIMIT_UP_PCT；跌停 PCT_CHG <= 0 - LIMIT_UP_PCT），禁止写死 10/20/30 数值。');
  }
```

- [ ] **Step 3: buildAnalyzePrompt 歧义术语追加涨停/跌停**

在 `frontend/src/lib/selectNL.ts:634`（`'   - 「距上次…不超过N日」...` 行）后追加：

```ts
    '   - 「涨停」→「当日收盘涨幅达到或超过该股涨停幅度(LIMIT_UP_PCT)」；「跌停」→「当日收盘跌幅达到或超过该股跌停幅度」',
```

- [ ] **Step 4: 新增 trySafeLimitUpDownRewrite**

在 `frontend/src/lib/selectNL.ts:590`（`trySafeNumericCrossRewrite` 函数结束 `}` 之后、`numericLevelFromAnalysis` 之前）插入：

```ts
// 确定性还原：弱模型把「涨停/跌停」写死数值（如 PCT_CHG >= 10，对创业/科创/北交不对），
// 收敛回 LIMIT_UP_PCT 比较（涨停 PCT_CHG >= LIMIT_UP_PCT；跌停 PCT_CHG <= 0 - LIMIT_UP_PCT）。
// 仅当 analysis 文本含涨停/跌停关键词 且 公式整体精确匹配 写死数值幅度时才改写，否则返回 null（不误改「涨幅大于5%」）。
export function trySafeLimitUpDownRewrite(formula: string, analysis?: AnalyzeResult): string | null {
  if (!analysis) return null;
  const text = [...(analysis.conditions || []), analysis.restatement || ''].join(' ');
  if (!/涨停|跌停|封板|一字板/.test(text)) return null;
  const up = /^PCT_CHG\s*(>=|>)\s*(?:10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (up) return `PCT_CHG ${up[1]} LIMIT_UP_PCT`;
  const down = /^PCT_CHG\s*(<=|<)\s*(?:-|0\s*-\s*)(10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (down) return `PCT_CHG ${down[1]} 0 - LIMIT_UP_PCT`;
  const downInvert = /^0\s*-\s*PCT_CHG\s*(>=|>)\s*(?:10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (downInvert) return 'PCT_CHG <= 0 - LIMIT_UP_PCT';
  return null;
}
```

- [ ] **Step 5: route.ts 改写链接入**

将 `frontend/src/app/api/select-nl/route.ts:91-94`：

```ts
      let rewritten = trySafeBollRefRewrite(parsed.formula);
      if (!rewritten) rewritten = trySafeAbsAbsRewrite(parsed.formula);
      if (!rewritten) rewritten = trySafeNumericCrossRewrite(parsed.formula, analysis);
      if (rewritten) parsed = { ...parsed, formula: rewritten };
```

改为（追加第 4 级）：

```ts
      let rewritten = trySafeBollRefRewrite(parsed.formula);
      if (!rewritten) rewritten = trySafeAbsAbsRewrite(parsed.formula);
      if (!rewritten) rewritten = trySafeNumericCrossRewrite(parsed.formula, analysis);
      if (!rewritten) rewritten = trySafeLimitUpDownRewrite(parsed.formula, analysis);
      if (rewritten) parsed = { ...parsed, formula: rewritten };
```

同时 import 区（`frontend/src/app/api/select-nl/route.ts:14-16`）追加：

```ts
  trySafeNumericCrossRewrite,
  trySafeLimitUpDownRewrite,
```

- [ ] **Step 6: 提交**

```bash
git add frontend/src/lib/selectNL.ts frontend/src/app/api/select-nl/route.ts
git commit -m "feat(frontend): 涨停/跌停按板别翻译（提示词+硬约束+trySafeLimitUpDownRewrite）
```

---

### Task 4: 前端测试副本同步 + 新增测试（红色）

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`

- [ ] **Step 1: META 副本 fields 追加 LIMIT_UP_PCT**

将 `frontend/tests/select-nl.test.mjs:17` 的 fields 数组追加 `'LIMIT_UP_PCT'`（末尾）：

```js
fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN', 'LIMIT_UP_PCT'],
```

- [ ] **Step 2: 同步 buildHardConstraintSuffix 新分支**

在 `frontend/tests/select-nl.test.mjs:338-340`（`if (/较高者|.../` 块）之后追加（与 Task 3 Step 2 完全一致）：

```js
  if (/涨停|跌停|封板|一字板/.test(text)) {
    lines.push('硬约束：本需求含涨停/跌停语义，必须使用 PCT_CHG 与 LIMIT_UP_PCT 比较（涨停 PCT_CHG >= LIMIT_UP_PCT；跌停 PCT_CHG <= 0 - LIMIT_UP_PCT），禁止写死 10/20/30 数值。');
  }
```

- [ ] **Step 3: 同步 buildSystemPrompt 易错模式第 12 条**

在 `frontend/tests/select-nl.test.mjs:519`（`'11) CCI 突破...'`）后追加（与 Task 3 Step 1 完全一致）：

```js
    '12) 涨停/跌停：涨停 = PCT_CHG >= LIMIT_UP_PCT；跌停 = PCT_CHG <= 0 - LIMIT_UP_PCT。',
    '   禁止写死 10/20/30（各板限幅不同：主板10 科创/创业20 北交30，必须用 LIMIT_UP_PCT 字段）。',
```

- [ ] **Step 4: 同步 trySafeLimitUpDownRewrite**

在 `frontend/tests/select-nl.test.mjs:465`（`trySafeNumericCrossRewrite` 结束）后、`numericLevelFromAnalysis` 前插入（与 Task 3 Step 4 完全一致的 JS 副本）：

```js
function trySafeLimitUpDownRewrite(formula, analysis) {
  if (!analysis) return null;
  const text = [...(analysis.conditions || []), analysis.restatement || ''].join(' ');
  if (!/涨停|跌停|封板|一字板/.test(text)) return null;
  const up = /^PCT_CHG\s*(>=|>)\s*(?:10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (up) return `PCT_CHG ${up[1]} LIMIT_UP_PCT`;
  const down = /^PCT_CHG\s*(<=|<)\s*(?:-|0\s*-\s*)(10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (down) return `PCT_CHG ${down[1]} 0 - LIMIT_UP_PCT`;
  const downInvert = /^0\s*-\s*PCT_CHG\s*(>=|>)\s*(?:10|20|30)(?:\.0+)?$/.exec(formula.trim());
  if (downInvert) return 'PCT_CHG <= 0 - LIMIT_UP_PCT';
  return null;
}
```

- [ ] **Step 5: guard 断言追加**

在 `frontend/tests/select-nl.test.mjs:1229`（`assert.match(src, /export function trySafeNumericCrossRewrite/);`）后追加：

```js
  assert.match(src, /export function trySafeLimitUpDownRewrite/);
```

- [ ] **Step 6: 写新增测试**

在 `frontend/tests/select-nl.test.mjs:966`（`trySafeNumericCrossRewrite: 无分析或阈值缺失不改写` 测试之后）追加一组测试：

```js
test('trySafeLimitUpDownRewrite: 涨停写死数值改写为 LIMIT_UP_PCT', () => {
  const a = { restatement: '筛选涨停的股票', conditions: ['当日涨停'], logic: '1', timeframe: 'D' };
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 10', a), 'PCT_CHG >= LIMIT_UP_PCT');
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG > 20', a), 'PCT_CHG > LIMIT_UP_PCT');
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 30', a), 'PCT_CHG >= LIMIT_UP_PCT');
});

test('trySafeLimitUpDownRewrite: 跌停写死数值改写', () => {
  const a = { restatement: '筛选跌停的股票', conditions: ['当日跌停'], logic: '1', timeframe: 'D' };
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG <= -10', a), 'PCT_CHG <= 0 - LIMIT_UP_PCT');
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG < 0 - 20', a), 'PCT_CHG < 0 - LIMIT_UP_PCT');
  assert.equal(trySafeLimitUpDownRewrite('0 - PCT_CHG >= 30', a), 'PCT_CHG <= 0 - LIMIT_UP_PCT');
});

test('trySafeLimitUpDownRewrite: 非涨停语义或非法形态不改写', () => {
  const a = { restatement: '筛选涨幅大于5%的股票', conditions: ['当日涨幅大于5%'], logic: '1', timeframe: 'D' };
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 5', a), null);
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 10', undefined), null);
  const b = { restatement: '筛选涨停的股票', conditions: ['当日涨停'], logic: '1', timeframe: 'D' };
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 5.5', b), null);
});

test('trySafeLimitUpDownRewrite: 涨停或跌停 OR 组合不改写(形态不匹配)', () => {
  const a = { restatement: '涨停或跌停的股票', conditions: ['涨停或跌停'], logic: '1', timeframe: 'D' };
  assert.equal(trySafeLimitUpDownRewrite('PCT_CHG >= 10 OR PCT_CHG <= -10', a), null);
});

test('buildHardConstraintSuffix: 涨停/跌停触发 LIMIT_UP_PCT 硬约束', () => {
  const a = { restatement: '筛选涨停的股票', conditions: ['当日涨停'] };
  assert.match(buildHardConstraintSuffix(a), /LIMIT_UP_PCT/);
  const b = { restatement: '筛选跌停的股票', conditions: ['当日跌停'] };
  assert.match(buildHardConstraintSuffix(b), /LIMIT_UP_PCT/);
});

test('buildSystemPrompt: 易错模式含涨停/跌停与 LIMIT_UP_PCT', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /涨停 = PCT_CHG >= LIMIT_UP_PCT/);
  assert.match(p, /禁止写死 10\/20\/30/);
});

test('validateFormula: 接受 PCT_CHG 与 LIMIT_UP_PCT 比较', () => {
  assert.deepEqual(validateFormula(META, 'PCT_CHG >= LIMIT_UP_PCT'), { ok: true });
  assert.deepEqual(validateFormula(META, 'PCT_CHG <= 0 - LIMIT_UP_PCT'), { ok: true });
});
```

- [ ] **Step 7: 运行前端测试确认绿**

Run (workdir `frontend`): `node --test tests/**/*.mjs`
Expected: 全绿（新增 ~7 个测试），含 guard 断言。

- [ ] **Step 8: TypeScript 类型检查**

Run (workdir `frontend`): `npx --yes -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop src/lib/selectNL.ts`
Expected: 无错误。

- [ ] **Step 9: 提交**

```bash
git add frontend/tests/select-nl.test.mjs
git commit -m "test(frontend): 涨停/跌停改写+硬约束+校验 单测（同步副本）
```

---

### Task 5: 覆盖生成器 + 在线回归用例

**Files:**
- Modify: `frontend/scripts/nl-coverage.mjs:7-26`（FIELD_GEN）
- Modify: `frontend/scripts/nl-test.mjs`（CASES 追加在线用例）

- [ ] **Step 1: FIELD_GEN 追加 LIMIT_UP_PCT**

在 `frontend/scripts/nl-coverage.mjs:25`（`TURN: {...}`）后追加：

```js
  LIMIT_UP_PCT: { q: '涨停的股票', sub: ['LIMIT_UP_PCT'] },
```

- [ ] **Step 2: 运行覆盖单测确认 LIMIT_UP_PCT 生成用例**

Run (workdir `frontend`): `node --test tests/**/*.mjs`
Expected: `buildCoverageCases` 现有测试不受影响；新增一个断言 LIMIT_UP_PCT 生成器存在（可选追加，见 Step 3）。

- [ ] **Step 3: 追加覆盖生成器单测**

在 `frontend/tests/select-nl.test.mjs` 末尾追加：

```js
test('buildCoverageCases: LIMIT_UP_PCT 字段有生成器可生成', () => {
  const out = buildCoverageCases({ fields: ['LIMIT_UP_PCT'], indicators: [], timeframes: ['D'], units: {}, example_queries: [], signatures: {}, descriptions: {} }, []);
  const c = out.cases[0];
  assert.equal(c.cid, 'gF_LIMIT_UP_PCT');
  assert.deepEqual(c.sub, ['LIMIT_UP_PCT']);
  assert.deepEqual(out.uncoveredFields, []);
});
```

- [ ] **Step 4: nl-test.mjs CASES 追加涨停/跌停在线用例**

在 `frontend/scripts/nl-test.mjs:78`（`u3` 行）后追加：

```js

  // ===== 类别 6: 按板别涨停/跌停 =====
  { cid: 'l1', cat: '涨停', q: '涨停的股票', sub: ['LIMIT_UP_PCT'], tf: 'D' },
  { cid: 'l2', cat: '跌停', q: '跌停的股票', sub: ['LIMIT_UP_PCT'], tf: 'D' },
  { cid: 'l3', cat: '涨停', q: '涨停或跌停的股票', sub: ['LIMIT_UP_PCT'], sub_any: ['OR'], tf: 'D' },
```

> 修正记录：l3 的 `sub` 原写作 `['LIMIT_UP_PCT', 'AND']`，但 OR 组合公式（`PCT_CHG >= LIMIT_UP_PCT OR PCT_CHG <= 0 - LIMIT_UP_PCT`）不含 `AND`，该断言必失败；已改为仅断言 `LIMIT_UP_PCT` 存在 + `OR` 任一命中（nl-test.mjs 支持 `sub` 与 `sub_any` 同时断言）。

> 注：`l3` 的 `sub`/`sub_any` 组合需检查 nl-test.mjs 的断言逻辑（`sub_any` 存在时怎样匹配）。若该脚本只认 `sub`，则 `l3` 用 `sub_any: ['OR']` 并仅断言公式含 `LIMIT_UP_PCT`。

- [ ] **Step 5: 前端全量测试确认绿**

Run (workdir `frontend`): `node --test tests/**/*.mjs`
Expected: 全绿。

- [ ] **Step 6: 提交**

```bash
git add frontend/scripts/nl-coverage.mjs frontend/scripts/nl-test.mjs frontend/tests/select-nl.test.mjs
git commit -m "test(coverage): LIMIT_UP_PCT 覆盖生成器 + 涨停/跌停在线用例
```

---

### Task 6: 指南文档更新

**Files:**
- Modify: `docs/NL-STOCK-SELECT-GUIDE.md`

- [ ] **Step 1: 字段表追加 LIMIT_UP_PCT 行**

在 `docs/NL-STOCK-SELECT-GUIDE.md` 字段表（`| \`TURN\` | 换手率 ... |` 行之后）追加：

```md
| `LIMIT_UP_PCT` | 涨停幅度（按板别） | 百分比(%) | 涨停的股票 → `PCT_CHG >= LIMIT_UP_PCT` |
```

- [ ] **Step 2: 易混淆表追加涨停/跌停行**

在 `docs/NL-STOCK-SELECT-GUIDE.md` 易混淆表述表末追加：

```md
| 涨停：`PCT_CHG >= 10` | `PCT_CHG >= LIMIT_UP_PCT` | 各板限幅不同（主板10/科创创业20/北交30），必须用派生字段 |
| 跌停：`PCT_CHG <= -10` | `PCT_CHG <= 0 - LIMIT_UP_PCT` | 对称处理，禁止写死数值 |
```

- [ ] **Step 3: 追加「涨停/跌停按板别」说明小节**

在 `docs/NL-STOCK-SELECT-GUIDE.md` 第六节前新增小节：

```md
## 五（附）涨停/跌停按板别

「涨停」「跌停」按股票所属板别自动换算（2026-07-06 新规，ST 无独立限幅）：

| 板别 | 代码前缀 | 涨停/跌停 |
|------|---------|----------|
| 沪主板 | `sh.600/601/603/605` | ±10% |
| 深主板 | `sz.000/001/002/003` | ±10% |
| 科创板 | `sh.688/689` | ±20% |
| 创业板 | `sz.300/301` | ±20% |
| 北交所 | `bj.43/83/87/88/92` | ±30% |

翻译结果统一用 `LIMIT_UP_PCT` 字段（每行按代码实时算出限幅）：
- 涨停 → `PCT_CHG >= LIMIT_UP_PCT`
- 跌停 → `PCT_CHG <= 0 - LIMIT_UP_PCT`

新股例外（首日 44%、注册制前 5 日无限制、北交首日无限制）本期不处理。
```

> 编号冲突处理：若文档已有「五、易混淆表述」与「六、组合条件与逻辑」，将新增小节放在易混淆表述之后、组合条件之前，编号自行顺延或直接以「附」命名（以上写法已用附）。

- [ ] **Step 4: 提交**

```bash
git add docs/NL-STOCK-SELECT-GUIDE.md
git commit -m "docs(guide): 涨停/跌停按板别字段 LIMIT_UP_PCT 说明
```

---

### Task 7: 全量验证 + 推送 + 线上跑批

**Files:**
- 无代码改动（仅验证）

- [ ] **Step 1: 后端全量测试**

Run (workdir `backend`): `python -m unittest discover -s tests -v`
Expected: 全部 PASS（98 + 新增 3 = 101）。

- [ ] **Step 2: 前端全量测试 + 类型检查**

Run (workdir `frontend`):
```
node --test tests/**/*.mjs
npx --yes -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop src/lib/selectNL.ts
```
Expected: 全绿 + 无错误。

- [ ] **Step 3: 检查 git status 无遗漏**

Run (workdir `E:\数据中台\blinkquant`): `git status`
Expected: 工作树干净（或仅剩未提交的计划文档）。

- [ ] **Step 4: 推送 main 触发部署**

```bash
git push origin main
```

- [ ] **Step 5: 等部署后拉取 nl-meta 确认 19 字段**

Run: `curl https://scanli-blinkquant-node1.hf.space/api/v1/nl-meta | python -c "import sys,json; d=json.load(sys.stdin); print(len(d['fields']), d['fields'])"`
Expected: `19` 且含 `LIMIT_UP_PCT`。（Windows PowerShell 中文管道可能乱码，仅读命令行非中文输出无碍；若乱码改用临时脚本文件。）

- [ ] **Step 6: 在线跑批**

Run (workdir `frontend`): `node scripts/nl-test.mjs "https://blinkquant.de5.net" "1@1.com" "22222222"`
Expected: `l1/l2/l3` PASS 且公式为 `PCT_CHG >= LIMIT_UP_PCT` / `PCT_CHG <= 0 - LIMIT_UP_PCT` / OR 组合；覆盖矩阵字段 19/19、算子 47/47。

---

## Self-Review

**1. Spec coverage：**
- 后端派生字段 → Task 1-2
- 前端提示词/硬约束 → Task 3
- 确定性改写 + 接入 chain → Task 3 Step 4-5
- 前端测试同步 + 单测 → Task 4
- 覆盖生成器 → Task 5
- 指南文档 → Task 6
- 验收（19 字段、47 算子、19/19 矩阵）→ Task 7

**2. Placeholder scan：** 每步含完整代码/命令；无 TBD/TODO。

**3. Type consistency：** `trySafeLimitUpDownRewrite(formula, analysis)` 在 Task 3 (src)、Task 4 (test 副本)、route.ts 三处签名一致；`buildHardConstraintSuffix`/`buildSystemPrompt` 分支两处同步；`LIMIT_UP_PCT` 在 registry FIELDS、security fields、前端 META、FIELD_GEN、指南文档五处命名一致。
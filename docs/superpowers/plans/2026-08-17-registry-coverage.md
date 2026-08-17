# 注册表全覆盖测试实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `nl-test.mjs` 运行前拉取 `nl-meta` 注册表，自动补齐字段/算子缺口用例，并输出覆盖矩阵（字段/算子 N/N + 未覆盖列表），实现注册表全覆盖验证。

**Architecture:** 新建 `frontend/scripts/nl-coverage.mjs` 纯函数模块（生成器表 + 缺口生成 + 覆盖矩阵计算），由脚本 `nl-test.mjs` import 并在运行前拉取 meta 补齐用例、运行后打印矩阵；单测直接 import 该模块（`.mjs` 可被 `node --test` 直接引用，无需镜像复制，比现有 selectNL.ts 的复制模式更 DRY）。

**Tech Stack:** Node 20+ ESM（`node --test` 单测，`node scripts/nl-test.mjs` 集成脚本）。

---

### Task 1: 新建 nl-coverage.mjs 纯函数模块（含测试）

**Files:**
- Create: `frontend/scripts/nl-coverage.mjs`
- Modify: `frontend/tests/select-nl.test.mjs`（追加单测，import 新模块）
- Test: `frontend/tests/select-nl.test.mjs`

- [ ] **Step 1: 在测试文件末尾追加失败测试**

在 `frontend/tests/select-nl.test.mjs` 的 `guard` 测试之后追加以下内容（测试文件已是 ESM，直接 import）：

```js
// ---- 注册表全覆盖：生成器与覆盖矩阵（import scripts/nl-coverage.mjs，纯 .mjs 无需复制）----
import {
  buildCoverageCases,
  computeCoverageMatrix,
  formatCoverageMatrix,
} from '../scripts/nl-coverage.mjs';

test('buildCoverageCases: 只生成未被现有用例断言的字段/算子缺口', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN', 'PCT_CHG'],
    indicators: ['MA', 'ABS', 'CROSS_UP'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const existing = [
    { cid: 'x1', q: '收盘价站上20日均线的股票', sub: ['CLOSE', 'MA('], tf: 'D' },
    { cid: 'x2', q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP'], tf: 'D' },
  ];
  const out = buildCoverageCases(meta, existing);
  const cids = out.cases.map((c) => c.cid).sort();
  assert.deepEqual(cids, ['gF_OPEN', 'gF_PCT_CHG', 'gI_ABS']);
  assert.deepEqual(out.uncoveredFields, []);
  assert.deepEqual(out.uncoveredInds, []);
  const openCase = out.cases.find((c) => c.cid === 'gF_OPEN');
  assert.equal(typeof openCase.q, 'string');
  assert.ok(openCase.q.length > 0);
  assert.deepEqual(openCase.sub, ['OPEN']);
});

test('buildCoverageCases: 注册表新增无生成器的项计入未覆盖', () => {
  const meta = {
    fields: ['NOVEL_FIELD'],
    indicators: ['NOVEL_IND'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const out = buildCoverageCases(meta, []);
  assert.deepEqual(out.cases, []);
  assert.deepEqual(out.uncoveredFields, ['NOVEL_FIELD']);
  assert.deepEqual(out.uncoveredInds, ['NOVEL_IND']);
});

test('computeCoverageMatrix: 从公式反推字段/算子覆盖与缺失', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN', 'PCT_CHG'],
    indicators: ['MA', 'ABS', 'CROSS_UP'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const results = [
    { ok: true, formula: 'CLOSE > MA(CLOSE, 20)' },
    { ok: true, formula: 'ABS(CLOSE - MA(CLOSE, 20)) > 2' },
  ];
  const m = computeCoverageMatrix(meta, results);
  assert.equal(m.fields.total, 3);
  assert.equal(m.fields.covered, 2);
  assert.deepEqual(m.fields.missing, ['PCT_CHG']);
  assert.equal(m.indicators.total, 3);
  assert.equal(m.indicators.covered, 2);
  assert.deepEqual(m.indicators.missing, ['CROSS_UP']);
});

test('computeCoverageMatrix: 大小写不敏感的公式 token 提取', () => {
  const meta = {
    fields: ['CLOSE'], indicators: ['MA'],
    timeframes: ['D'], units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const m = computeCoverageMatrix(meta, [{ ok: true, formula: 'close > ma(CLOSE,20)' }]);
  assert.equal(m.fields.covered, 1);
  assert.equal(m.indicators.covered, 1);
  assert.deepEqual(m.fields.missing, []);
});

test('formatCoverageMatrix: 输出含总数与缺失项', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN'], indicators: ['MA'],
    timeframes: ['D'], units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const m = computeCoverageMatrix(meta, [{ ok: true, formula: 'CLOSE > MA(CLOSE,20)' }]);
  const s = formatCoverageMatrix(m);
  assert.match(s, /字段: 1\/2/);
  assert.match(s, /缺: OPEN/);
  assert.match(s, /算子: 1\/1/);
});
```

- [ ] **Step 2: 运行测试确认失败**

Run (workdir `frontend`): `node --test tests/select-nl.test.mjs`
Expected: FAIL，报 `ERR_MODULE_NOT_FOUND` / 找不到 `../scripts/nl-coverage.mjs`。

- [ ] **Step 3: 创建 `frontend/scripts/nl-coverage.mjs`**

完整内容：

```js
// frontend/scripts/nl-coverage.mjs
// 注册表全覆盖：按 nl-meta 的字段/算子全集，补齐测试缺口用例并计算覆盖矩阵。
// 供 nl-test.mjs 集成脚本使用，也可被 node --test 直接 import 做单测（纯 .mjs）。

// 字段生成器：为每个字段写最佳口语查询（描述驱动；语义口径由 q 表达，不由描述机械拼接）。
// note: 口语罕见、易混淆的字段标注说明。
export const FIELD_GEN = {
  CLOSE: { q: '收盘价大于10元的股票', sub: ['CLOSE'] },
  OPEN: { q: '开盘价大于昨日收盘价的股票', sub: ['OPEN'] },
  HIGH: { q: '当日最高价大于20日均线的股票', sub: ['HIGH'] },
  LOW: { q: '当日最低价小于20日均线的股票', sub: ['LOW'] },
  VOL: { q: '成交量大于100万手的股票', sub: ['VOL'] },
  AMOUNT: { q: '成交额大于5亿的股票', sub: ['AMOUNT'] },
  PCT_CHG: { q: '当日涨幅大于3%的股票', sub: ['PCT_CHG'] },
  S_CLOSE: { q: '指数收盘点位高于3000的股票', sub: ['S_CLOSE'], note: '指数点位字段' },
  PE_TTM: { q: '市盈率低于15倍的股票', sub: ['PE_TTM'] },
  PB_MRQ: { q: '市净率低于1.5倍的股票', sub: ['PB_MRQ'] },
  FORECAST_YOY: { q: '预测净利润同比增长大于20%的股票', sub: ['FORECAST_YOY'], note: '预测数据' },
  IS_FORECAST_GOOD: { q: '业绩预增的股票', sub: ['IS_FORECAST_GOOD'], note: '预测利好' },
  IS_FORECAST_BAD: { q: '业绩预亏的股票', sub: ['IS_FORECAST_BAD'], note: '预测利空' },
  TOTAL_SHARES: { q: '总股本大于10亿股的股票', sub: ['TOTAL_SHARES'] },
  FLOAT_SHARES: { q: '流通股本大于5亿股的股票', sub: ['FLOAT_SHARES'] },
  TOTAL_MV: { q: '总市值大于100亿的股票', sub: ['TOTAL_MV'] },
  FLOAT_MV: { q: '流通市值大于200亿的股票', sub: ['FLOAT_MV'] },
  TURN: { q: '换手率大于5%的股票', sub: ['TURN'] },
};

// 算子生成器：为每个算子写最佳口语查询。
export const IND_GEN = {
  ABS: { q: '收盘价距20日均线绝对偏差大于2元的股票', sub: ['ABS'] },
  ATR: { q: '14日真实波幅均值大于3的股票', sub: ['ATR'] },
  BARSLAST: { q: '距上次突破20日均线不超过3天的股票', sub: ['BARSLAST'] },
  BOLL_LOWER: { q: '收盘价跌破布林下轨的股票', sub: ['BOLL_LOWER'] },
  BOLL_UPPER: { q: '收盘价突破布林上轨的股票', sub: ['BOLL_UPPER'] },
  COUNT: { q: '近5日收盘价站上20日均线的天数不少于3天的股票', sub: ['COUNT'] },
  CROSS_DOWN: { q: '5日均线下穿30日均线的股票', sub: ['CROSS_DOWN'] },
  CROSS_UP: { q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP'] },
  EMA: { q: '收盘价站上20日指数均线的股票', sub: ['EMA'] },
  HHV: { q: '创20日新高的股票', sub: ['HHV'] },
  KDJ_D: { q: 'KDJ的D值大于80的股票', sub: ['KDJ_D'] },
  KDJ_K: { q: 'KDJ的K值大于80的股票', sub: ['KDJ_K'] },
  LLV: { q: '创20日新低的股票', sub: ['LLV'] },
  MA: { q: '收盘价站上20日均线的股票', sub: ['MA'] },
  MAX: { q: '开盘价与收盘价取较大值后大于昨日最高价的股票', sub: ['MAX'] },
  MIN: { q: '开盘价与收盘价取较小值后小于昨日最低价的股票', sub: ['MIN'] },
  REF: { q: '今日收盘价高于昨日收盘价的股票', sub: ['REF'] },
  ROC: { q: '5日变动率大于5%的股票', sub: ['ROC'] },
  RSI: { q: '14日RSI大于70的股票', sub: ['RSI'] },
  STD: { q: '20日收盘价标准差大于2的股票', sub: ['STD'] },
  SUM: { q: '近5日成交额之和大于100亿的股票', sub: ['SUM'] },
};

// 现有用例断言 token 前缀匹配任意注册表字段/算子，判定该字段/算子已被手工用例覆盖。
function coveredTokens(meta, existingCases) {
  const coveredFields = new Set();
  const coveredInds = new Set();
  const allTokens = [...meta.fields, ...meta.indicators];
  for (const c of existingCases) {
    for (const s of [...(c.sub || []), ...(c.sub_any || [])]) {
      const up = String(s).toUpperCase();
      for (const tok of allTokens) {
        if (up.startsWith(tok)) {
          if (meta.fields.includes(tok)) coveredFields.add(tok);
          else coveredInds.add(tok);
        }
      }
    }
  }
  return { coveredFields, coveredInds };
}

// 生成缺口用例：未被现有用例断言的字段/算子，有生成器则生成，无生成器计入未覆盖。
export function buildCoverageCases(meta, existingCases) {
  const { coveredFields, coveredInds } = coveredTokens(meta, existingCases);
  const cases = [];
  const uncoveredFields = [];
  const uncoveredInds = [];
  for (const f of meta.fields || []) {
    if (coveredFields.has(f)) continue;
    const g = FIELD_GEN[f];
    if (!g) { uncoveredFields.push(f); continue; }
    cases.push({ cid: `gF_${f}`, cat: '覆盖-字段', q: g.q, sub: g.sub, tf: 'D' });
  }
  for (const ind of meta.indicators || []) {
    if (coveredInds.has(ind)) continue;
    const g = IND_GEN[ind];
    if (!g) { uncoveredInds.push(ind); continue; }
    cases.push({ cid: `gI_${ind}`, cat: '覆盖-算子', q: g.q, sub: g.sub, tf: 'D' });
  }
  return { cases, uncoveredFields, uncoveredInds };
}

const TOKEN_RE = /[A-Z][A-Z0-9_]*/g;

// 从执行结果反推覆盖矩阵：公式大写后提取大写标识符，与注册表全集比对。
export function computeCoverageMatrix(meta, results) {
  const hitFields = new Set();
  const hitInds = new Set();
  for (const r of results) {
    const tokens = new Set((String(r.formula || '').toUpperCase().match(TOKEN_RE)) || []);
    for (const f of meta.fields || []) if (tokens.has(f)) hitFields.add(f);
    for (const i of meta.indicators || []) if (tokens.has(i)) hitInds.add(i);
  }
  return {
    fields: {
      total: (meta.fields || []).length,
      covered: hitFields.size,
      missing: (meta.fields || []).filter((f) => !hitFields.has(f)),
    },
    indicators: {
      total: (meta.indicators || []).length,
      covered: hitInds.size,
      missing: (meta.indicators || []).filter((i) => !hitInds.has(i)),
    },
  };
}

export function formatCoverageMatrix(m) {
  const lines = [];
  lines.push('--- 覆盖矩阵 ---');
  lines.push(
    `字段: ${m.fields.covered}/${m.fields.total}` +
    (m.fields.missing.length ? ` (缺: ${m.fields.missing.join(', ')})` : '')
  );
  lines.push(
    `算子: ${m.indicators.covered}/${m.indicators.total}` +
    (m.indicators.missing.length ? ` (缺: ${m.indicators.missing.join(', ')})` : '')
  );
  if (m.uncoveredFields && m.uncoveredFields.length) {
    lines.push(`无生成器的字段: ${m.uncoveredFields.join(', ')}`);
  }
  if (m.uncoveredInds && m.uncoveredInds.length) {
    lines.push(`无生成器的算子: ${m.uncoveredInds.join(', ')}`);
  }
  return lines.join('\n');
}
```

- [ ] **Step 4: 运行测试确认通过**

Run (workdir `frontend`): `node --test tests/select-nl.test.mjs`
Expected: PASS，总用例 81 + 5 = 86。

- [ ] **Step 5: Commit**

```bash
git add frontend/scripts/nl-coverage.mjs frontend/tests/select-nl.test.mjs
git commit -m "feat: registry coverage generator + matrix pure functions"
```

---

### Task 2: nl-test.mjs 集成（拉 meta → 补缺口 → 打印矩阵）

**Files:**
- Modify: `frontend/scripts/nl-test.mjs`

- [ ] **Step 1: 文件头部加 import 与 meta 拉取**

在 `nl-test.mjs` 头部（`const BASE_URL` 之后）加：

```js
import { buildCoverageCases, computeCoverageMatrix, formatCoverageMatrix } from './nl-coverage.mjs';

// 后端节点（与 src/lib/selectNLServer.ts 的 NODES 一致），meta 从任一节点拉取。
const META_NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

async function fetchMeta() {
  for (const node of META_NODES) {
    try {
      const res = await fetch(`${node}/api/v1/nl-meta`, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (Array.isArray(json.fields) && Array.isArray(json.indicators)) return json;
      throw new Error('meta 字段不完整');
    } catch (e) {
      console.log(`  (meta ${node} 失败: ${e.message})`);
    }
  }
  throw new Error('所有节点拉取 nl-meta 均失败');
}
```

- [ ] **Step 2: run() 开头拉 meta 并追加缺口用例**

修改 `run()` 开头（`const cookie = await login();` 之后）：

```js
  let meta;
  try {
    meta = await fetchMeta();
    console.log(`注册表: 字段 ${meta.fields.length} / 算子 ${meta.indicators.length}`);
  } catch (e) {
    console.error(`拉取 nl-meta 失败: ${e.message}`);
    process.exit(2);
  }

  const { cases: genCases, uncoveredFields, uncoveredInds } = buildCoverageCases(meta, CASES);
  if (genCases.length) {
    console.log(`按注册表补齐缺口用例 ${genCases.length} 条:`);
    for (const g of genCases) console.log(`  ${g.cid} [${g.cat}] "${g.q}"`);
  }
  const casesToRun = [...CASES, ...genCases];
```

随后把循环与汇总里的 `CASES` 全部替换为 `casesToRun`：
- `for (const c of CASES) {` → `for (const c of casesToRun) {`
- `console.log(\`总计 ${CASES.length} 用例：PASS ${pass} / FAIL ${fail}\`);` → 用 `casesToRun.length`

- [ ] **Step 3: 汇总段追加覆盖矩阵输出与退出码判定**

在 `console.log('\n--- 完整公式输出（人工复核语义） ---');` 之前插入：

```js
  // ---- 覆盖矩阵 ----
  const matrix = computeCoverageMatrix(meta, results);
  matrix.uncoveredFields = uncoveredFields;
  matrix.uncoveredInds = uncoveredInds;
  console.log('\n' + formatCoverageMatrix(matrix));

  const incomplete =
    matrix.fields.covered < matrix.fields.total ||
    matrix.indicators.covered < matrix.indicators.total ||
    uncoveredFields.length > 0 ||
    uncoveredInds.length > 0;

  process.exit(fail === 0 && !incomplete ? 0 : 1);
```

并把文件末尾原有的 `process.exit(fail === 0 ? 0 : 1);` 删除（被上一步替代）。

- [ ] **Step 4: 语法检查 + 单测回归**

Run (workdir `frontend`): `node --check scripts/nl-test.mjs`
Expected: 无输出（语法 OK）。

Run: `node --test tests/select-nl.test.mjs`
Expected: 86 pass。

- [ ] **Step 5: Commit**

```bash
git add frontend/scripts/nl-test.mjs
git commit -m "feat: nl-test pulls registry meta, runs gap cases, prints coverage matrix"
```

---

### Task 3: 端到端验证（线上运行）

**Files:** 无代码改动

- [ ] **Step 1: 跑全量回归**

Run (workdir `frontend`): `node --test tests/select-nl.test.mjs`
Expected: 全部 pass（86 条）。

- [ ] **Step 2: 线上运行覆盖率脚本**

Run: `node scripts/nl-test.mjs <baseUrl> <email> <password>`
Expected:
- 输出「注册表: 字段 18 / 算子 21」
- 补齐缺口用例（人工用例已覆盖字段 6 个/算子 8 个，缺口 12 字段 + 13 算子 = 25 条）
- 矩阵显示字段 18/18、算子 21/21（或列出实际缺口）
- 退出码 0 仅当全部用例 PASS 且覆盖完整

- [ ] **Step 3: 若有缺口，人工复核矩阵列出的字段/算子公式，确认是生成器查询问题还是翻译错误；必要时微调 FIELD_GEN/IND_GEN 的 q 后重跑并提交**

- [ ] **Step 4: 更新设计文档状态**

在 `docs/superpowers/specs/2026-08-17-registry-coverage-design.md` 头部 `状态:` 改为 `已实现`。

Commit: `git commit -am "docs: mark registry coverage design implemented"`

---

## 验收对照（spec）

| spec 要求 | 对应任务 |
|-----------|----------|
| 运行前拉取 nl-meta | Task 2 Step 1 |
| 字段/算子全覆盖用例生成 | Task 1 `buildCoverageCases` + Task 2 Step 2 |
| 覆盖矩阵输出（字段/算子 N/N + 未覆盖） | Task 1 `computeCoverageMatrix`/`formatCoverageMatrix` + Task 2 Step 3 |
| 新增注册表项无生成器 → 标未覆盖 | Task 1 `uncoveredFields/uncoveredInds` + Task 2 Step 3 |
| 生成器/矩阵纯函数 + 单测 | Task 1（直接 import `.mjs`，无镜像复制） |
| 全量回归不破坏现有用例 | Task 2 Step 4 / Task 3 Step 1 |

## 已知注意点

- `MAX`/`MIN` 子串：`MAX(` 与 `MIN(` 不包含彼此，`computeCoverageMatrix` 用完整标识符 `\b[A-Z][A-Z0-9_]*` 提取，`MAX` 不会误命中 `MIN`。
- `REF`/`CLOSE` 前缀匹配：现有 `sub_any: ['REF(CLOSE']` 会以 `REF` 命中，REF 视为已覆盖。
- `t1` 周线 MA 窗口 260 存疑（此前遗留），本次不触碰断言逻辑；如需收紧另开任务。

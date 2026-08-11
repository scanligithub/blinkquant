# K线图财务指标显示 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在个股 K 线图上展示市盈率、总市值、流通市值、成交额、成交量、换手率（顶部信息栏 + 悬浮框）。

**Architecture:** 后端 `/api/v1/kline` 已返回全部所需字段，仅前端在三条数据链路上丢失：`parseParquetRecords` 未透传、`applyAdjust` 丢弃、`resampleData` 未聚合。本计划贯穿数据字段，并在 page.tsx 顶部信息栏与 KLineChart 悬浮框渲染。

**Tech Stack:** Next.js 14 (client component)、hyparquet、node --test、定制 util 函数（亿/万格式化）。

---

## 文件结构

| 动作 | 路径 | 职责 |
|------|------|------|
| Modify | `frontend/src/utils/parquet.ts` | 透传新财务字段 |
| Modify | `frontend/src/utils/applyAdjust.ts` | 保真新财务字段 |
| Modify | `frontend/src/app/page.tsx` | resampleData 聚合 + 顶部信息栏 |
| Modify | `frontend/src/components/KLineChart.tsx` | 悬浮框追加财务字段显示 |
| Create | `frontend/src/utils/format.ts` | `formatMoney` 智能货币格式化器（新共享工具） |
| Create | `frontend/tests/resample.test.mjs` | resampleData 聚合测试 |
| Modify | `frontend/tests/parquet.test.mjs` | parseParquetRecords 透传测试 |
| Modify | `frontend/tests/apply-adjust.test.mjs` | applyAdjust 保真测试 |

---

### Task 1: 数据字段贯穿 parseParquetRecords

**Files:**
- Modify: `frontend/src/utils/parquet.ts`
- Test: `frontend/tests/parquet.test.mjs`

- [ ] **Step 1: 更新实现 `frontend/src/utils/parquet.ts`**

将函数体替换为：

```ts
export function parseParquetRecords(records: any[]): any[] {
  return records.map((record) => {
    let timeValue;
    if (record.date instanceof Date) timeValue = Math.floor(record.date.getTime() / 1000);
    else throw new Error('Invalid date');
    return {
      time: timeValue,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      volume: record.volume,
      amount: record.amount,
      turn: record.turn,
      peTTM: record.peTTM,
      total_mv: record.total_mv,
      float_mv: record.float_mv,
      main_net: record.main_net || 0,
      adjustFactor: record.adjustFactor,
    };
  });
}
```

- [ ] **Step 2: 更新测试 `frontend/tests/parquet.test.mjs`**

将文件顶部复制的 `parseParquetRecords` 实现同步替换为与新实现一致，并追加用例：

```js
test('parseParquetRecords 透传财务字段', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{
    date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100,
    amount: 123456, turn: 3.5, peTTM: 20.1, total_mv: 1e10, float_mv: 5e9,
  }]);
  assert.equal(result[0].amount, 123456);
  assert.equal(result[0].turn, 3.5);
  assert.equal(result[0].peTTM, 20.1);
  assert.equal(result[0].total_mv, 1e10);
  assert.equal(result[0].float_mv, 5e9);
});

test('parseParquetRecords 财务字段缺失时为 undefined', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].amount, undefined);
  assert.equal(result[0].turn, undefined);
  assert.equal(result[0].peTTM, undefined);
  assert.equal(result[0].total_mv, undefined);
  assert.equal(result[0].float_mv, undefined);
});
```

- [ ] **Step 3: 运行测试确认全部通过**

Run: `node --test tests/parquet.test.mjs`
Expected: `# pass` 且断言全通过（含原有 4 个用例 + 新增 2 个）

- [ ] **Step 4: 提交**

```bash
git add frontend/src/utils/parquet.ts frontend/tests/parquet.test.mjs
git commit -m "feat(kline): pass through financial fields in parseParquetRecords"
```

---

### Task 2: applyAdjust 保真财务字段

**Files:**
- Modify: `frontend/src/utils/applyAdjust.ts`
- Test: `frontend/tests/apply-adjust.test.mjs`

- [ ] **Step 1: 更新实现 `frontend/src/utils/applyAdjust.ts`**

在 `result` 的 map 返回对象中追加财务字段（位置在 `main_net` 之后、`adjustFactor` 之前）：

```ts
    return {
      time: bar.time,
      open: bar.open * priceMul,
      high: bar.high * priceMul,
      low: bar.low * priceMul,
      close: bar.close * priceMul,
      volume: bar.volume * volMul,
      main_net: bar.main_net, // 资金流不受复权影响
      amount: bar.amount,
      turn: bar.turn,
      peTTM: bar.peTTM,
      total_mv: bar.total_mv,
      float_mv: bar.float_mv,
      adjustFactor: bar.adjustFactor,
    };
```

- [ ] **Step 2: 更新测试 `frontend/tests/apply-adjust.test.mjs`**

将文件顶部复制的 `applyAdjust` 实现同步替换同款 return 对象，并在 `roundBar` 后追加用例：

```js
test('财务字段不随复权变化', () => {
  const bars = [
    { time: 1, open: 5, high: 6, low: 4, close: 5.5, volume: 2000, main_net: 0, adjustFactor: 0.5,
      amount: 1000000, turn: 2.5, peTTM: 15.5, total_mv: 2e10, float_mv: 1e10 },
    { time: 2, open: 10, high: 12, low: 9, close: 11, volume: 4000, main_net: 0, adjustFactor: 0.5,
      amount: 2000000, turn: 3.0, peTTM: 16.0, total_mv: 2.2e10, float_mv: 1.1e10 },
  ];
  const resNone = applyAdjust(bars, 'none');
  const resHfq = applyAdjust(bars, 'hfq');
  for (const mode of [resNone, resHfq]) {
    assert.equal(mode[0].amount, 1000000);
    assert.equal(mode[0].turn, 2.5);
    assert.equal(mode[0].peTTM, 15.5);
    assert.equal(mode[0].total_mv, 2e10);
    assert.equal(mode[0].float_mv, 1e10);
    assert.equal(mode[1].amount, 2000000);
    assert.equal(mode[1].turn, 3.0);
    assert.equal(mode[1].peTTM, 16.0);
  }
});
```

- [ ] **Step 3: 运行测试确认全部通过**

Run: `node --test tests/apply-adjust.test.mjs`
Expected: `# pass` 且全部断言通过（含原有 5 个用例 + 新增 1 个）

- [ ] **Step 4: 提交**

```bash
git add frontend/src/utils/applyAdjust.ts frontend/tests/apply-adjust.test.mjs
git commit -m "feat(kline): preserve financial fields in applyAdjust"
```

---

### Task 3: 创建共享货币格式化工具

**Files:**
- Create: `frontend/src/utils/format.ts`

- [ ] **Step 1: 创建 `frontend/src/utils/format.ts`**

```ts
// 智能资金单位格式化器（元 → 亿/万）
export function formatMoney(value: number | null | undefined): string {
  if (!value || isNaN(value)) return '--';
  const absVal = Math.abs(value);
  if (absVal >= 100000000) return (value / 100000000).toFixed(2) + '亿';
  else if (absVal >= 10000) return (value / 10000).toFixed(2) + '万';
  return value.toFixed(2);
}

// 智能数量格式化器（股 → 亿/万）
export function formatVolume(volume: number | null | undefined): string {
  if (!volume || isNaN(volume)) return '--';
  if (volume >= 100000000) return (volume / 100000000).toFixed(2) + '亿';
  else if (volume >= 10000) return (volume / 10000).toFixed(2) + '万';
  return volume.toString();
}
```

- [ ] **Step 2: 提交**

```bash
git add frontend/src/utils/format.ts
git commit -m "feat(kline): add shared money/volume format helpers"
```

---

### Task 4: resampleData 聚合财务字段（含新增测试文件）

**Files:**
- Modify: `frontend/src/app/page.tsx` 的 `resampleData` 函数（约 341-376 行）
- Create: `frontend/tests/resample.test.mjs`

- [ ] **Step 1: 新建测试 `frontend/tests/resample.test.mjs`**

```js
// frontend/tests/resample.test.mjs
// resampleData 拷贝自 frontend/src/app/page.tsx（约定与 parquet.test.mjs 一致）
import { test } from 'node:test';
import assert from 'node:assert/strict';

function resampleData(dailyData, targetTimeframe) {
  if (targetTimeframe === 'D') return dailyData;
  const grouped = new Map();
  dailyData.forEach(item => {
    const date = new Date(item.time * 1000);
    let key;
    if (targetTimeframe === 'W') {
      const dayOfWeek = date.getDay();
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - dayOfWeek);
      key = weekStart.toISOString().split('T')[0];
    } else {
      key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
    }
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  });
  const resampled = [];
  grouped.forEach((items) => {
    const sortedItems = items.sort((a, b) => a.time - b.time);
    const first = sortedItems[0];
    const last = sortedItems[sortedItems.length - 1];
    resampled.push({
      time: first.time,
      open: first.open,
      high: Math.max(...sortedItems.map(i => i.high)),
      low: Math.min(...sortedItems.map(i => i.low)),
      close: last.close,
      volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
      amount: sortedItems.reduce((sum, i) => sum + (i.amount || 0), 0),
      turn: last.turn,
      peTTM: last.peTTM,
      total_mv: last.total_mv,
      float_mv: last.float_mv,
      main_net: sortedItems.reduce((sum, i) => sum + (i.main_net || 0), 0),
    });
  });
  return resampled.sort((a, b) => a.time - b.time);
}

test('resampleData 日线原样返回', () => {
  const data = [{ time: 1, open: 1, close: 2, volume: 100 }];
  assert.equal(resampleData(data, 'D'), data);
});

test('resampleData 周线求和金额与前收，市值取末根', () => {
  // 2024-01-08(CST) 为周一；用 UTC 0:00 构造，避免时区偏差
  const data = [
    { time: Math.floor(new Date('2024-01-08T00:00:00Z').getTime() / 1000), open: 1, high: 2, low: 0.5, close: 1.5, volume: 100, amount: 1000, turn: 1.0, peTTM: 10, total_mv: 1e9, float_mv: 5e8, main_net: 5 },
    { time: Math.floor(new Date('2024-01-09T00:00:00Z').getTime() / 1000), open: 1.5, high: 3, low: 1, close: 2.5, volume: 200, amount: 2000, turn: 2.0, peTTM: 12, total_mv: 2e9, float_mv: 1e9, main_net: 7 },
  ];
  const res = resampleData(data, 'W');
  assert.equal(res.length, 1);
  assert.equal(res[0].volume, 300);
  assert.equal(res[0].amount, 3000);
  assert.equal(res[0].turn, 2.0);
  assert.equal(res[0].peTTM, 12);
  assert.equal(res[0].total_mv, 2e9);
  assert.equal(res[0].float_mv, 1e9);
  assert.equal(res[0].main_net, 12);
});

test('resampleData 缺少财务字段时兜底', () => {
  const data = [
    { time: Math.floor(new Date('2024-01-08T00:00:00Z').getTime() / 1000), open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 },
  ];
  const res = resampleData(data, 'W');
  assert.equal(res[0].amount, 0);
  assert.equal(res[0].turn, undefined);
});
```

- [ ] **Step 2: 运行测试确认失败（当前实现无 amount/聚合）**

Run: `node --test tests/resample.test.mjs`
Expected: FAIL（`res[0].amount` 为 undefined，断言抛错）

- [ ] **Step 3: 更新 `frontend/src/app/page.tsx` `resampleData`**

添加 amount 求和与 turn/peTTM/total_mv/float_mv 取末根（在 `close: last.close,` 之后插入）：

```ts
        close: last.close,
        volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
        amount: sortedItems.reduce((sum, i) => sum + (i.amount || 0), 0),
        turn: last.turn,
        peTTM: last.peTTM,
        total_mv: last.total_mv,
        float_mv: last.float_mv,
        main_net: sortedItems.reduce((sum, i) => sum + (i.main_net || 0), 0), // 聚合资金流数据
```

同时删除现有重复的 `main_net` 聚合行（原 372 行）。

- [ ] **Step 4: 运行测试确认通过**

Run: `node --test tests/resample.test.mjs`
Expected: PASS（3 个用例全过）

- [ ] **Step 5: 提交**

```bash
git add frontend/src/app/page.tsx frontend/tests/resample.test.mjs
git commit -m "feat(kline): aggregate financial fields in resampleData"
```

---

### Task 5: 顶部信息栏（page.tsx）

**Files:**
- Modify: `frontend/src/app/page.tsx`

- [ ] **Step 1: 引入 format 工具**

在 page.tsx import 区（现有 `import { parseParquetRecords } from '../utils/parquet';` 之后）添加：

```ts
import { formatMoney, formatVolume } from '../utils/format';
```

- [ ] **Step 2: 渲染顶部信息栏**

在 chartWrapper 头部区域、股票名行（`selectedStock.code`/`selectedStock.name` 所在 `<div className="flex flex-col items-start min-w-0">`）内、板块 chips 之前，追加（`selectedStock.kind === 'stock'` 时）：

```tsx
{selectedStock.kind === 'stock' && (() => {
  const latest = selectedStock.data[selectedStock.data.length - 1];
  if (!latest) return null;
  const items = [
    { label: 'PE(TTM)', value: latest.peTTM != null ? Number(latest.peTTM).toFixed(2) : '--' },
    { label: '总市值', value: formatMoney(latest.total_mv) },
    { label: '流通市值', value: formatMoney(latest.float_mv) },
    { label: '成交额', value: formatMoney(latest.amount) },
    { label: '换手率', value: latest.turn != null ? `${Number(latest.turn).toFixed(2)}%` : '--' },
    { label: '成交量', value: formatVolume(latest.volume) },
  ];
  return (
    <div className="w-full mt-2 flex flex-wrap items-center gap-x-3 gap-y-0.5">
      {items.map((it) => (
        <span key={it.label} className="text-[9px] md:text-xs text-slate-500 whitespace-nowrap">
          {it.label}: <span className="font-mono font-medium text-slate-900">{it.value}</span>
        </span>
      ))}
    </div>
  );
})()}
```

- [ ] **Step 3: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无类型错误（或仅有 pre-existing 的报错，确认无新增）

- [ ] **Step 4: 提交**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat(kline): add financial overview bar on stock kline"
```

---

### Task 6: 悬浮框追加财务字段（KLineChart.tsx）

**Files:**
- Modify: `frontend/src/components/KLineChart.tsx`

- [ ] **Step 1: 引入 format 工具并删除本文件内重复实现**

将文件顶部 `formatVolume`（45-52 行）与 `formatMoney`（54-66 行）替换为 import：

```ts
import { formatMoney, formatVolume } from '../utils/format';
```

- [ ] **Step 2: 在 crosshairMove 中读取财务字段**

`currentTime` 目前定义在 `changePercent` 的 IIFE 内部（约 634 行），外部不可见。先在 `setTooltip({` 之前（`const date = new Date(timeValue * 1000);` 之后）新增一行：

```ts
        const currentTime = typeof param.time === 'number' ? param.time : (param.time as any).businessDay || param.time;
```

然后在 `setTooltip({...})`（约 628-645 行）对象内，`volume` 字段之后追加（`data` 是组件 prop，闭包可见；`currentTime` 已为此作用域 const）：

```ts
          volume: (volData as any)?.value || 0,
          amount: (() => {
            const cur = data.find((d: any) => d.time === currentTime);
            return cur?.amount;
          })(),
          turn: (() => {
            const cur = data.find((d: any) => d.time === currentTime);
            return cur?.turn;
          })(),
          peTTM: (() => {
            const cur = data.find((d: any) => d.time === currentTime);
            return cur?.peTTM;
          })(),
```

- [ ] **Step 3: 渲染悬浮框财务行**

在 tooltip JSX 内（`<div className="space-y-0.5">`）追加，位于现有 `涨幅` 行之后：

```tsx
            {tooltip.amount != null && (
              <div className="flex justify-between gap-4"><span className="text-slate-500">成交额:</span><span className="font-mono text-slate-900">{formatMoney(tooltip.amount)}</span></div>
            )}
            {tooltip.turn != null && (
              <div className="flex justify-between gap-4"><span className="text-slate-500">换手率:</span><span className="font-mono text-slate-900">{Number(tooltip.turn).toFixed(2)}%</span></div>
            )}
            {tooltip.peTTM != null && (
              <div className="flex justify-between gap-4"><span className="text-slate-500">市盈率:</span><span className="font-mono text-slate-900">{Number(tooltip.peTTM).toFixed(2)}</span></div>
            )}
```

- [ ] **Step 4: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无新增类型错误

- [ ] **Step 5: 提交**

```bash
git add frontend/src/components/KLineChart.tsx
git commit -m "feat(kline): show amount/turn/pe in chart tooltip"
```

---

### Task 7: 全量验证与收尾

**Files:**
- 无新增文件

- [ ] **Step 1: 运行全部前端测试**

Run: `node --test tests/`
Expected: 全部 `# pass`（auth/export/idle-timeout/invite/apply-adjust/parquet/resample）

- [ ] **Step 2: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无新增类型错误

- [ ] **Step 3: 后端 py_compile 冒烟（无改动但确认链路）**

Run: `python -m py_compile api/routes.py core/data_manager.py`（在 `backend/` 目录）
Expected: 退出码 0，无输出

- [ ] **Step 4: 日志确认字段真实存在**

Run: 搜索 `"target_cols"` 于 `backend/api/routes.py` 确认 `amount/turn/peTTM/total_mv/float_mv` 均在列表中（已在 83 行，无需修改）
Expected: 字段存在于 `target_cols`

- [ ] **Step 5: 提交收尾（若有未提交改动）**

```bash
git status
git add -u
git commit -m "chore(kline): final verification"
```
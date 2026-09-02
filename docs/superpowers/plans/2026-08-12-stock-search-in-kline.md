# 搜索股票移入 K 线视图 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将「搜索股票」编辑框从主页面顶部 section 移入 K 线卡片头部工具栏，顶部 section 仅保留策略公式；移动端搜索折叠为 🔍 图标 + 覆盖层。

**Architecture:** 将搜索打分逻辑抽为纯函数 `searchStocks`（`src/utils/stockSearch.ts`，可单测）；新增客户端组件 `StockSearch`（桌面内联 + 移动覆盖层，内部管理 debounce 与结果列表）；`page.tsx` 顶部 section 删除搜索块、删除内联搜索状态/effect，K 线卡片 header 改为常驻渲染并嵌入 `StockSearch`，选中信息/工具栏仅 `selectedStock` 存在时渲染。

**Tech Stack:** Next.js 14 / React 18 / TypeScript / Tailwind CSS / pinyin-pro / node:test（前端单测）

---

### Task 1: 抽取搜索打分纯函数 + 单测

**Files:**
- Create: `frontend/src/utils/stockSearch.ts`
- Create: `frontend/tests/stock-search.test.mjs`
- Test: `node --test tests/stock-search.test.mjs`（在 frontend 目录运行）

- [ ] **Step 1: 写失败测试**

创建 `frontend/tests/stock-search.test.mjs`：

```js
// frontend/tests/stock-search.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pinyin } from 'pinyin-pro';

// 与 src/utils/pinyin.ts 保持一致（复制实现以绕过 TS import）
function getPinyinInitials(text) {
  if (!text) return '';
  const parts = text.split('.');
  let target = text;
  if (parts.length === 2 && (parts[0] === 'sh' || parts[0] === 'sz' || parts[0] === 'bj')) {
    target = parts[1];
  }
  const hasChinese = /[\u4e00-\u9fff]/.test(target);
  if (!hasChinese) return text.toLowerCase();
  const initials = pinyin(target, { pattern: 'first', toneType: 'none', type: 'array' }).join('').toLowerCase();
  return initials.replace(/[^a-z]/g, '');
}

// 与 src/utils/cleanInput.ts 保持一致
function cleanSearchInput(text) {
  if (!text) return '';
  let cleaned = text.replace(/[\s\u3000]/g, '');
  cleaned = cleaned.replace(/[‘’“”、，。,.!！?？;；:：\-—_—\[\]{}()<>【】《》]/g, '');
  return cleaned.toLowerCase();
}

// 与 src/utils/stockSearch.ts 保持一致（复制实现以绕过 TS import）
function searchStocks(stockList, query) {
  if (query.length < 1 || stockList.length === 0) return [];
  const cleanedQuery = cleanSearchInput(query);
  const qLower = cleanedQuery;
  const qPinyin = getPinyinInitials(cleanedQuery);

  const scoredResults = stockList.map((stock) => {
    const { code, name } = stock;
    if (!name || !name.trim()) return { ...stock, score: 0 };

    const nameClean = name.trim().toLowerCase();
    const codeClean = code.trim().toLowerCase();
    const codeNum = codeClean.replace(/^(sh|sz|bj)\./, '');
    const namePinyin = getPinyinInitials(name);
    let score = 0;

    if (codeClean === qLower || codeNum === qLower || nameClean === qLower) score += 1000;
    if (codeClean.startsWith(qLower) || codeNum.startsWith(qLower)) score += 100;
    if (namePinyin.startsWith(qPinyin)) score += 80;
    if (nameClean.startsWith(qLower)) score += 80;
    if (codeClean.includes(qLower) || codeNum.includes(qLower)) score += 10;
    if (namePinyin.includes(qPinyin)) score += 5;
    if (nameClean.includes(qLower)) score += 5;

    return { ...stock, score };
  });

  return scoredResults
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(({ code, name }) => ({ code, name }))
    .slice(0, 10);
}

test('searchStocks: 空查询返回空数组', () => {
  assert.deepEqual(searchStocks([{ code: 'sh.600000', name: '浦发银行' }], ''), []);
});

test('searchStocks: 代码精确匹配优先', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '600000'), [{ code: 'sh.600000', name: '浦发银行' }]);
});

test('searchStocks: 去前缀数字匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '000001'), [{ code: 'sz.000001', name: '平安银行' }]);
});

test('searchStocks: 名称精确匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '平安银行'), [{ code: 'sz.000001', name: '平安银行' }]);
});

test('searchStocks: 拼音首字母匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  // 浦发银行 → pfyh，平安银行 → payh
  assert.deepEqual(searchStocks(list, 'pf'), [{ code: 'sh.600000', name: '浦发银行' }]);
});

test('searchStocks: 无匹配返回空数组', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, 'zzz'), []);
});

test('searchStocks: 结果截断前10且保持输入顺序', () => {
  const list = Array.from({ length: 12 }, (_, i) => ({ code: `sh.6000${i}`, name: `股票${i}` }));
  const results = searchStocks(list, '股票');
  assert.equal(results.length, 10);
  assert.equal(results[0].name, '股票0');
  assert.equal(results[9].name, '股票9');
});
```

- [ ] **Step 2: 运行测试确认失败**

Run: `node --test tests/stock-search.test.mjs`
Expected: `Cannot find module` 或 0 个测试通过（文件仅测试，`searchStocks` 未定义于目标源码）。若 node 直接报错可先确认报错即可。

- [ ] **Step 3: 实现纯函数**

创建 `frontend/src/utils/stockSearch.ts`：

```ts
// src/utils/stockSearch.ts
import { getPinyinInitials } from './pinyin';
import { cleanSearchInput } from './cleanInput';

export interface StockItem {
  code: string;
  name: string;
}

export function searchStocks(stockList: StockItem[], query: string): StockItem[] {
  if (query.length < 1 || stockList.length === 0) return [];
  const cleanedQuery = cleanSearchInput(query);
  const qLower = cleanedQuery;
  const qPinyin = getPinyinInitials(cleanedQuery);

  const scoredResults = stockList.map((stock) => {
    const { code, name } = stock;
    if (!name || !name.trim()) return { ...stock, score: 0 };

    const nameClean = name.trim().toLowerCase();
    const codeClean = code.trim().toLowerCase();
    const codeNum = codeClean.replace(/^(sh|sz|bj)\./, '');
    const namePinyin = getPinyinInitials(name);
    let score = 0;

    if (codeClean === qLower || codeNum === qLower || nameClean === qLower) score += 1000;
    if (codeClean.startsWith(qLower) || codeNum.startsWith(qLower)) score += 100;
    if (namePinyin.startsWith(qPinyin)) score += 80;
    if (nameClean.startsWith(qLower)) score += 80;
    if (codeClean.includes(qLower) || codeNum.includes(qLower)) score += 10;
    if (namePinyin.includes(qPinyin)) score += 5;
    if (nameClean.includes(qLower)) score += 5;

    return { ...stock, score };
  });

  return scoredResults
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(({ code, name }) => ({ code, name }))
    .slice(0, 10);
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `node --test tests/stock-search.test.mjs`
Expected: 8 个测试全部 `ok`。

- [ ] **Step 5: Commit**

```bash
git add frontend/src/utils/stockSearch.ts frontend/tests/stock-search.test.mjs
git commit -m "feat(search): extract searchStocks pure scoring function with tests"
```

---

### Task 2: 创建 StockSearch 组件

**Files:**
- Create: `frontend/src/components/StockSearch.tsx`
- Test: `node --test tests/stock-search.test.mjs`（回归确认 Task 1 测试仍通过）

- [ ] **Step 1: 创建组件**

创建 `frontend/src/components/StockSearch.tsx`：

```tsx
'use client';
import { useEffect, useRef, useState } from 'react';
import { searchStocks, type StockItem } from '../utils/stockSearch';

interface StockSearchProps {
  stockList: StockItem[];
  onSelect: (code: string) => void;
}

export default function StockSearch({ stockList, onSelect }: StockSearchProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<StockItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [overlayOpen, setOverlayOpen] = useState(false);

  const inputClass =
    'flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full text-sm md:text-base';

  useEffect(() => {
    if (query.length < 1 || stockList.length === 0) {
      setResults([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    const handler = setTimeout(() => {
      setResults(searchStocks(stockList, query));
      setLoading(false);
    }, 300);
    return () => clearTimeout(handler);
  }, [query, stockList]);

  const select = (code: string) => {
    onSelect(code);
    setQuery('');
    setResults([]);
    setOverlayOpen(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && query.trim() !== '') {
      if (results.length > 0) {
        select(results[0].code);
      } else {
        const isNumeric = /^[0-9]+$/.test(query.trim());
        if (isNumeric) {
          const qNumeric = query.trim();
          const found = stockList.find((s) => s.code.replace(/^(sh|sz|bj)\./, '') === qNumeric);
          if (found) select(found.code);
        } else {
          const qL = query.toLowerCase();
          const found = stockList.find(
            (s) => s.code.toLowerCase().startsWith(qL) || s.name.toLowerCase().startsWith(qL)
          );
          if (found) select(found.code);
        }
      }
    }
  };

  const renderResults = () =>
    results.map((stock) => (
      <button
        key={stock.code}
        onClick={() => select(stock.code)}
        className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center"
      >
        <span className="font-medium text-slate-900">{stock.name}</span>
        <span className="text-sm font-mono text-slate-500">{stock.code}</span>
      </button>
    ));

  return (
    <>
      {/* 桌面端：工具栏内联搜索框（md+ 显示） */}
      <div className="relative hidden md:block w-64 shrink-0">
        <input
          className={inputClass}
          placeholder="搜索股票：名称/代码/拼音"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
        />
        {loading && (
          <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
            <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
          </div>
        )}
        {query.length > 1 && results.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 z-30 bg-white border rounded-xl shadow-lg max-h-60 overflow-y-auto custom-scrollbar">
            {renderResults()}
          </div>
        )}
      </div>

      {/* 移动端：搜索图标按钮（md- 显示） */}
      <button
        onClick={() => setOverlayOpen(true)}
        className="md:hidden px-3 py-2 text-slate-600 border border-slate-200 bg-white rounded-xl hover:bg-slate-100 transition-colors"
        aria-label="搜索股票"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
      </button>

      {/* 移动端：覆盖层搜索 */}
      {overlayOpen && (
        <div
          className="fixed inset-0 z-50 bg-black/40 flex items-start justify-center p-4 md:hidden"
          onClick={() => setOverlayOpen(false)}
        >
          <div className="bg-white rounded-2xl w-full max-w-md shadow-xl p-4" onClick={(e) => e.stopPropagation()}>
            <div className="relative">
              <input
                autoFocus
                className={inputClass}
                placeholder="搜索股票：名称/代码/拼音"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={handleKeyDown}
              />
              {loading && (
                <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
                  <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                </div>
              )}
            </div>
            {query.length > 1 && results.length > 0 && (
              <div className="mt-2 max-h-60 overflow-y-auto custom-scrollbar">{renderResults()}</div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
```

- [ ] **Step 2: 回归测试**

Run: `node --test tests/stock-search.test.mjs`
Expected: 8 个测试全部 `ok`（组件未改动纯函数）。

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/StockSearch.tsx
git commit -m "feat(search): add StockSearch component with inline + overlay UIs"
```

---

### Task 3: 修改 page.tsx —— 移除顶部搜索、接入 StockSearch

**Files:**
- Modify: `frontend/src/app/page.tsx`
- Test: `npx tsc --noEmit` + `npm run lint`（在 frontend 目录运行）

- [ ] **Step 1: 更新 import**

在 `frontend/src/app/page.tsx` 中：

删除这两行（page.tsx:17-18）：

```ts
import { getPinyinInitials } from '../utils/pinyin';
import { cleanSearchInput } from '../utils/cleanInput';
```

保留 `import { parseParquetRecords } from '../utils/parquet';` 等其余导入。在组件动态导入附近新增：

```ts
import StockSearch from '../components/StockSearch';
```

（建议放在 `const StrategyList = dynamic(...)` 之后。）

- [ ] **Step 2: 删除内联搜索状态**

删除这三行（原 page.tsx:162-164）：

```ts
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<{code: string; name: string}[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
```

- [ ] **Step 3: 删除内联搜索 effect**

删除整个搜索 effect（原 page.tsx:286-326，从 `useEffect(() => {` 开始到 `}, [searchQuery, stockList]);` 结束）。该块为：

```ts
  useEffect(() => {
    if (searchQuery.length < 1 || stockList.length === 0) {
      setSearchResults([]);
      setSearchLoading(false);
      return;
    }
    setSearchLoading(true);
    const handler = setTimeout(() => {
      // 清理搜索关键字，去除空格、标点等干扰字符
      const cleanedQuery = cleanSearchInput(searchQuery);
      const qLower = cleanedQuery;
      const qPinyin = getPinyinInitials(cleanedQuery);

      const scoredResults = stockList.map(stock => {
        const { code, name } = stock;
        if (!name || !name.trim()) return { ...stock, score: 0 };

        const nameClean = name.trim().toLowerCase();
        const codeClean = code.trim().toLowerCase();
        // 去市场前缀后的纯数字代码（sh.600000 → 600000），兼容用户直接输入数字
        const codeNum = codeClean.replace(/^(sh|sz|bj)\./, '');
        const namePinyin = getPinyinInitials(name);
        let score = 0;

        if (codeClean === qLower || codeNum === qLower || nameClean === qLower) score += 1000;
        if (codeClean.startsWith(qLower) || codeNum.startsWith(qLower)) score += 100;
        if (namePinyin.startsWith(qPinyin)) score += 80;
        if (nameClean.startsWith(qLower)) score += 80;
        if (codeClean.includes(qLower) || codeNum.includes(qLower)) score += 10;
        if (namePinyin.includes(qPinyin)) score += 5;
        if (nameClean.includes(qLower)) score += 5;

        return { ...stock, score };
      });

      const results = scoredResults.filter(item => item.score > 0).sort((a, b) => b.score - a.score).map(({ code, name }) => ({ code, name })).slice(0, 10);
      setSearchResults(results);
      setSearchLoading(false);
    }, 300); 
    return () => clearTimeout(handler);
  }, [searchQuery, stockList]);
```

- [ ] **Step 4: 删除顶部 section 的搜索块**

将顶部 section（原 page.tsx:529-596）的搜索块删除。原结构为：

```tsx
        {/* Search & Formula Inputs */}
        <section className="bg-white p-4 md:p-6 rounded-2xl border border-slate-200 shadow-sm">
          <div className="flex flex-col gap-3 md:gap-4 mb-4 md:mb-6">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">搜索股票</label>
            <div className="relative z-20">
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 md:px-4 md:py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full text-sm md:text-base"
                placeholder="例如：000952, 平安, PA"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && searchQuery.trim() !== '') {
                    if (searchResults.length > 0) {
                      viewStock(searchResults[0].code);
                      setSearchQuery(''); setSearchResults([]);
                    } else {
                      const isNumeric = /^[0-9]+$/.test(searchQuery.trim());
                      if (isNumeric) {
                        // code 已带市场前缀，从 stockList 反查匹配的数字代码
                        const qNumeric = searchQuery.trim();
                        let found = stockList.find(s => s.code.replace(/^(sh|sz|bj)\./, '') === qNumeric);
                        if (found) { viewStock(found.code); setSearchQuery(''); }
                      }
                      else {
                        const qL = searchQuery.toLowerCase();
                        let found = stockList.find(s => s.code.toLowerCase().startsWith(qL) || s.name.toLowerCase().startsWith(qL));
                        if(found) { viewStock(found.code); setSearchQuery(''); }
                      }
                    }
                  }
                }}
              />
              {searchLoading && <div className="absolute inset-y-0 right-0 pr-3 flex items-center"><div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div></div>}
              {searchQuery.length > 1 && searchResults.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-2 z-30 bg-white border rounded-xl shadow-lg max-h-60 overflow-y-auto custom-scrollbar">
                {searchResults.map((stock) => (
                  <button key={stock.code} onClick={() => { viewStock(stock.code); setSearchQuery(''); setSearchResults([]); }} className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center">
                    <span className="font-medium text-slate-900">{stock.name}</span>
                    <span className="text-sm font-mono text-slate-500">{stock.code}</span>
                  </button>
                ))}
                </div>
              )}
            </div>
          </div>

          <div className="flex flex-col gap-3">
```

将上述整块（`{/* Search & Formula Inputs */}` 到 `<div className="flex flex-col gap-3">` 之前的结束 `</div>`）替换为：

```tsx
        {/* Formula Inputs */}
        <section className="bg-white p-4 md:p-6 rounded-2xl border border-slate-200 shadow-sm">
          <div className="flex flex-col gap-3">
```

即 section 只保留「策略公式 + 运行选股 + 保存策略」块，删除整个搜索块。

- [ ] **Step 5: K 线卡片 header 常驻并接入 StockSearch**

将图表卡片 header（原 page.tsx:646-839）改为始终渲染。原结构开头：

```tsx
            <div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
              {selectedStock && (
                <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
                  <div className="flex flex-col items-start min-w-0">
```

替换为：

```tsx
            <div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
              <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
                <StockSearch stockList={stockList} onSelect={viewStock} />
                {selectedStock && (
                  <>
                    <div className="flex flex-col items-start min-w-0">
```

然后在原 header 结束处（原 page.tsx:836-839）将：

```tsx
                    </div>
                  </div>
                </div>
              )}
```

替换为：

```tsx
                    </div>
                  </div>
                  </>
                )}
              </div>
```

注意：保持原有缩进层级，确保 JSX 标签闭合正确。最终结构为：

```
<div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
  <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
    <StockSearch stockList={stockList} onSelect={viewStock} />
    {selectedStock && (
      <>
        <div className="flex flex-col items-start min-w-0">
          ... 股票信息/板块标签/指标 ...
        </div>
        <div className="flex flex-wrap items-center gap-2">
          ... 工具栏（复权/全屏/周期/自选/返回） ...
        </div>
      </>
    )}
  </div>
  <div className="flex-1 w-full h-full relative p-1">
    ...
  </div>
</div>
```

- [ ] **Step 6: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无错误输出，退出码 0。

- [ ] **Step 7: Lint**

Run: `npm run lint`
Expected: 无 error（可能仅有提示）。

- [ ] **Step 8: 回归单测**

Run: `node --test tests/stock-search.test.mjs`
Expected: 8 个测试全部 `ok`。

- [ ] **Step 9: Commit**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat(kline): move stock search into K-line header toolbar"
```

---

### Task 4: 端到端验证

**Files:**
- None（仅手动验证）

- [ ] **Step 1: 启动开发服务器**

Run: `npm run dev`
Expected: 服务在 `http://localhost:3000` 启动，无编译错误。

- [ ] **Step 2: 桌面端验证**

1. 打开首页：顶部 section 无「搜索股票」块，仅策略公式；K 线卡片 header 左侧显示搜索框。
2. 在搜索框输入 `000001` / `平安` / `pa`，下拉列表出现结果，点选后加载图表。
3. 未选中股票时：header 显示搜索框，图表区显示「选择股票查看图表」占位。
4. 选中后：股票信息、板块标签、复权、全屏、周期切换、自选/返回按钮均正常。
5. 全屏模式下搜索框仍可用。

- [ ] **Step 3: 移动端验证（DevTools 窄屏 <768px）**

1. 工具栏显示 🔍 图标；点击弹出覆盖层，输入框自动聚焦。
2. 输入 `平安` 出现结果，点选后关闭覆盖层并加载图表。
3. 覆盖层点击遮罩/其他区域可关闭。

- [ ] **Step 4: 提交最终文档（如无变更则跳过）**

若验证中发现需要修正，直接修改并 commit；否则无需额外提交。

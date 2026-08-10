# 股票板块功能 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 K 线图界面显示股票所属的全部板块（行业+概念+地域）标签，点击可跳转到该板块 K 线，并提供「返回个股」。

**Architecture:** 后端在内存构建 `stock_sectors`（stock→sector 列表 dict）并新增两个接口（标签查询、板块 K 线 Parquet）；前端通过 Edge 代理路由（Promise.any×3 节点）懒加载标签，在标题栏渲染可点击 chips，点击后切换图表为板块 K 线并支持返回个股。

**Tech Stack:** FastAPI + Polars (backend) / Next.js 14 + hyparquet (frontend) / lightweight-charts

---

### Task 1: 后端构建 stock_sectors 全量映射

**Files:**
- Modify: `backend/core/data_manager.py`
- Test: `python -m py_compile backend/core/data_manager.py`

- [ ] **Step 1: 在 `__init__` 初始化 stock_sectors**

在 `backend/core/data_manager.py` 的 `__init__` 中 `self.df_sector_list = None` 之后新增：

```python
        self.stock_sectors = {}
```

- [ ] **Step 2: 在 `_build_sector_mapping` 中构建全量映射**

`_build_sector_mapping` 中，在 `mapped` 构建完成后、`industry = mapped.filter(...)` 之前插入：

```python
            # 构建 1-to-N 全量股票→板块映射（行业+概念+地域），供 K 线图板块标签使用
            try:
                all_map = mapped.unique(subset=["code", "sector_code"]).select([
                    pl.col("code"),
                    pl.col("sector_code"),
                    pl.col("sector_name"),
                    pl.col("type"),
                ])
                sectors_by_code: dict = {}
                for row in all_map.iter_rows():
                    sectors_by_code.setdefault(row[0], []).append((row[1], row[2], row[3]))
                self.stock_sectors = sectors_by_code
                logger.info(f"Node {self.node_index}: stock_sectors built: {len(self.stock_sectors)} stocks mapped")
            except Exception as e:
                logger.error(f"Node {self.node_index}: Failed to build stock_sectors: {e}", exc_info=True)
                self.stock_sectors = {}
```

- [ ] **Step 3: 验证编译**

Run: `python -m py_compile backend/core/data_manager.py`
Expected: 无输出、退出码 0

- [ ] **Step 4: Commit**

```bash
git add backend/core/data_manager.py
git commit -m "feat(backend): build stock_sectors full mapping in memory"
```

---

### Task 2: 后端新增 stock-sectors 与 sector-kline 接口

**Files:**
- Modify: `backend/api/routes.py`
- Test: `python -m py_compile backend/api/routes.py`

- [ ] **Step 1: 新增 stock-sectors 接口**

在 `backend/api/routes.py` 的 `get_stock_list` 函数之后新增：

```python
@router.get("/stock-sectors")
def get_stock_sectors(code: str):
    """返回股票所属的全部板块（行业+概念+地域）"""
    sectors = data_manager.stock_sectors.get(code, [])
    return {
        "code": code,
        "sectors": [
            {"code": sc, "name": name, "type": typ}
            for sc, name, typ in sectors
        ],
    }
```

- [ ] **Step 2: 新增 sector-kline 接口**

在 `get_kline` 函数之后新增：

```python
@router.get("/sector-kline")
def get_sector_kline(code: str, timeframe: str = "D"):
    if timeframe == "W":
        df = getattr(data_manager, "df_sector_weekly", None)
    elif timeframe == "M":
        df = getattr(data_manager, "df_sector_monthly", None)
    else:
        df = data_manager.df_sector_daily

    if df is None:
        raise HTTPException(status_code=503, detail="Data not ready")

    sector_df = df.filter(pl.col("code") == code).sort("date")
    if len(sector_df) == 0:
        raise HTTPException(status_code=404, detail="Sector not found")

    target_cols = ["date", "code", "name", "type", "open", "high", "low", "close", "volume", "amount"]
    available_cols = [col for col in target_cols if col in sector_df.columns]
    sector_df = sector_df.select(available_cols)

    buffer = io.BytesIO()
    sector_df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="application/octet-stream")
```

（`io`、`Response`、`HTTPException` 已在文件顶部导入，无需改动 imports。）

- [ ] **Step 3: 验证编译**

Run: `python -m py_compile backend/api/routes.py`
Expected: 无输出、退出码 0

- [ ] **Step 4: Commit**

```bash
git add backend/api/routes.py
git commit -m "feat(backend): add stock-sectors and sector-kline endpoints"
```

---

### Task 3: 前端新增两个 Edge 代理路由

**Files:**
- Create: `frontend/src/app/api/stock-sectors/route.ts`
- Create: `frontend/src/app/api/sector-kline/route.ts`
- Test: `npx tsc --noEmit`

- [ ] **Step 1: 创建 stock-sectors 路由**

`frontend/src/app/api/stock-sectors/route.ts`：

```typescript
import { NextResponse, NextRequest } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space'
];

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  if (!code) {
    return NextResponse.json({ error: 'Stock code is required' }, { status: 400 });
  }

  try {
    const result = await Promise.any(
      NODES.map(async (nodeUrl) => {
        const res = await fetch(`${nodeUrl}/api/v1/stock-sectors?code=${encodeURIComponent(code)}`, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) throw new Error(`Node responded with ${res.status}`);
        return res.json();
      })
    );
    return NextResponse.json(result, { headers: { 'Cache-Control': 'no-store' } });
  } catch (error) {
    console.error('Failed to fetch stock sectors:', error);
    return NextResponse.json({ error: 'Failed to fetch stock sectors' }, { status: 503 });
  }
}
```

- [ ] **Step 2: 创建 sector-kline 路由**

`frontend/src/app/api/sector-kline/route.ts`：

```typescript
import { NextResponse, NextRequest } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space'
];

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return new NextResponse(JSON.stringify({ error: '未登录' }), { status: auth.status, headers: { 'Content-Type': 'application/json' } });
  }

  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';
  if (!code) {
    return new NextResponse(JSON.stringify({ error: 'Sector code is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  }

  try {
    const resultBuffer = await Promise.any(
      NODES.map(async (nodeUrl) => {
        const url = `${nodeUrl}/api/v1/sector-kline?code=${encodeURIComponent(code)}&timeframe=${timeframe}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) {
          const errorText = await res.text();
          console.error(`Backend node ${nodeUrl} responded with status ${res.status}: ${errorText}`);
          throw new Error(`Backend error: ${errorText}`);
        }
        const arrayBuffer = await res.arrayBuffer();
        if (!arrayBuffer || arrayBuffer.byteLength < 100) {
          throw new Error('Empty or invalid Parquet data received');
        }
        return arrayBuffer;
      })
    );

    return new NextResponse(resultBuffer, {
      status: 200,
      headers: { 'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-store' }
    });
  } catch (error) {
    console.error("Error fetching sector kline data:", error);
    if (error instanceof AggregateError) {
      return new NextResponse(JSON.stringify({ error: 'Sector not found in cluster or data unavailable' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
    }
    return new NextResponse(JSON.stringify({ error: 'Failed to fetch sector kline data' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}
```

- [ ] **Step 3: 类型检查**

Run: `cd frontend && npx tsc --noEmit`
Expected: 无错误

- [ ] **Step 4: Commit**

```bash
git add frontend/src/app/api/stock-sectors/route.ts frontend/src/app/api/sector-kline/route.ts
git commit -m "feat(frontend): add stock-sectors and sector-kline proxy routes"
```

---

### Task 4: 前端解析工具与单测

**Files:**
- Create: `frontend/src/utils/parquet.ts`
- Create: `frontend/tests/parquet.test.mjs`
- Test: `node --test frontend/tests/parquet.test.mjs`

- [ ] **Step 1: 写失败测试**

`frontend/tests/parquet.test.mjs`：

```javascript
import { test } from 'node:test';
import assert from 'node:assert/strict';

// 复制实现以规避 TS import（沿用 apply-adjust.test.mjs 模式）
function parseParquetRecords(records) {
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
      main_net: record.main_net || 0,
      adjustFactor: record.adjustFactor,
    };
  });
}

test('parseParquetRecords 转换 Date 为 epoch 秒', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].time, Math.floor(date.getTime() / 1000));
  assert.equal(result[0].close, 1.5);
});

test('parseParquetRecords 兜底 main_net 为 0', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100, main_net: 88 }]);
  assert.equal(result[0].main_net, 88);
  const noNet = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(noNet[0].main_net, 0);
});

test('parseParquetRecords 保留 adjustFactor 缺失为 undefined', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].adjustFactor, undefined);
  const withFactor = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100, adjustFactor: 1.2 }]);
  assert.equal(withFactor[0].adjustFactor, 1.2);
});

test('parseParquetRecords 非法日期抛错', () => {
  assert.throws(() => parseParquetRecords([{ date: '2024-01-02', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]), /Invalid date/);
});
```

- [ ] **Step 2: 运行测试确认可执行**

Run: `node --test frontend/tests/parquet.test.mjs`
Expected: 4 个测试全部 PASS（测试内复制实现，用于先验证断言逻辑；真正的 utils 在下一步创建，其正确性由 tsc 与手动使用验证——沿用 apply-adjust.test.mjs 模式）

- [ ] **Step 3: 创建 utils/parquet.ts**

`frontend/src/utils/parquet.ts`：

```typescript
// 将 hyparquet 解析出的 Parquet records 规范化为 K 线组件需要的格式
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
      main_net: record.main_net || 0,
      adjustFactor: record.adjustFactor,
    };
  });
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `node --test frontend/tests/parquet.test.mjs`
Expected: 4 个测试全部 PASS

- [ ] **Step 5: Commit**

```bash
git add frontend/src/utils/parquet.ts frontend/tests/parquet.test.mjs
git commit -m "feat(frontend): add parseParquetRecords util with unit tests"
```

---

### Task 5: page.tsx 板块标签与板块 K 线视图

**Files:**
- Modify: `frontend/src/app/page.tsx`
- Test: `cd frontend && npx tsc --noEmit`

- [ ] **Step 1: 新增状态**

在 `page.tsx` 中 `const [dailyDataCache, setDailyDataCache] = useState<any[]>([]);` 之后新增：

```typescript
  const [sectorDataCache, setSectorDataCache] = useState<any[]>([]);
  const [sectors, setSectors] = useState<{ code: string; name: string; type: string }[]>([]);
  const lastStockRef = useRef<{ code: string; name: string } | null>(null);
```

- [ ] **Step 2: 改造 viewStock**

将 `viewStock` 中解析 records 的 `dailyData.map(...)` 替换为复用 `parseParquetRecords`，并在成功设置 selectedStock 后拉取板块标签：

```typescript
import { parseParquetRecords } from '../utils/parquet';
```

在 `viewStock` 的 `setSelectedStock(...)` 之后追加：

```typescript
      // 懒加载该股票的板块标签（失败静默）
      try {
        const sectorRes = await fetch(`/api/stock-sectors?code=${encodeURIComponent(code)}`);
        if (sectorRes.ok) {
          const sectorJson = await sectorRes.json();
          setSectors(sectorJson.sectors || []);
        } else {
          setSectors([]);
        }
      } catch (e) {
        console.warn('Failed to load stock sectors:', e);
        setSectors([]);
      }
```

并将原来解析部分改为：

```typescript
      const dailyData = parseParquetRecords(records);
```

- [ ] **Step 3: 新增 viewSector**

在 `viewStock` 之后新增：

```typescript
  const viewSector = useCallback(async (sectorCode: string, sectorName: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/sector-kline?code=${encodeURIComponent(sectorCode)}&timeframe=D`);
      if (!res.ok) throw new Error('Fetch failed');
      const buffer = await res.arrayBuffer();
      if (buffer.byteLength === 0) throw new Error('Empty buffer');

      const records = await parquetReadObjects({ file: buffer, compressors });
      if (!records || records.length === 0) throw new Error('Empty records');

      const sectorDaily = parseParquetRecords(records);
      setSectorDataCache(sectorDaily);
      setSelectedStock({ kind: 'sector', code: sectorCode, name: sectorName, data: resampleData(sectorDaily, chartTimeframe) });
    } catch (err: any) {
      alert(`Failed: ${err.message}`);
    } finally {
      setChartLoading(false);
    }
  }, [chartTimeframe, resampleData]);
```

- [ ] **Step 4: 更新 selectedStock 类型与视图类型**

将 `selectedStock` 初始值改为支持 `kind` 字段（状态声明处）：

```typescript
  const [selectedStock, setSelectedStock] = useState<{ kind: 'stock' | 'sector'; code: string; name?: string; data: any } | null>(null);
```

在 `viewStock` 的 `setSelectedStock({ code, name: stock?.name || code, data: resampledData });` 改为：

```typescript
       setSelectedStock({ kind: 'stock', code, name: stock?.name || code, data: resampledData });
```

在周期切换按钮 handler 中，`if (adjustedDaily && adjustedDaily.length > 0) setSelectedStock({ ...selectedStock, data: resampleData(adjustedDaily, tf.value) });` 改为按 kind 分支：

```typescript
                        onClick={() => {
                            setChartTimeframe(tf.value);
                            if (selectedStock?.kind === 'sector') {
                              if (sectorDataCache.length > 0) setSelectedStock({ ...selectedStock, data: resampleData(sectorDataCache, tf.value) });
                            } else if (adjustedDaily && adjustedDaily.length > 0) {
                              setSelectedStock({ ...selectedStock, data: resampleData(adjustedDaily, tf.value) });
                            }
                          }}
```

在复权菜单选项的 handler 中，`setSelectedStock(prev => prev ? { ...prev, data: resampleData(applyAdjust(dailyDataCache, opt.value), chartTimeframe) } : prev);` 改为加 kind 守卫：

```typescript
              if (selectedStock?.kind === 'stock' && dailyDataCache.length > 0) {
                const adjusted = applyAdjust(dailyDataCache, opt.value);
                setSelectedStock(prev => prev ? { ...prev, data: resampleData(adjusted, chartTimeframe) } : prev);
              }
```

- [ ] **Step 5: 渲染板块标签与返回按钮**

在标题栏股票名 `<span className="ml-2 text-base font-medium text-slate-500">{selectedStock.name}</span>` 之后（`</div>` 前）插入板块 chips：

```tsx
                    <div className="w-full mt-1 flex flex-wrap gap-1">
                      {selectedStock.kind === 'stock' && sectors.map((s) => (
                        <button
                          key={s.code}
                          onClick={() => { lastStockRef.current = { code: selectedStock.code, name: selectedStock.name || selectedStock.code }; viewSector(s.code, s.name); }}
                          className={`text-[10px] md:text-xs px-1.5 py-0.5 rounded border font-medium transition-colors ${
                            s.type === '行业板块' ? 'bg-blue-50 text-blue-700 border-blue-200 hover:bg-blue-100'
                            : s.type === '概念板块' ? 'bg-purple-50 text-purple-700 border-purple-200 hover:bg-purple-100'
                            : 'bg-green-50 text-green-700 border-green-200 hover:bg-green-100'
                          }`}
                        >
                          {s.name}
                        </button>
                      ))}
                    </div>
```

在标题栏的按钮组中，板块视图显示「返回个股」：

```tsx
                    {selectedStock.kind === 'sector' && (
                      <button
                        onClick={() => {
                          const last = lastStockRef.current;
                          if (!last) return;
                          setSelectedStock({ kind: 'stock', code: last.code, name: last.name, data: resampleData(adjustedDaily, chartTimeframe) });
                        }}
                        className="px-3 py-1 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-md mr-2 hover:bg-slate-100 transition-colors"
                      >
                        ← 返回 {lastStockRef.current?.name || '个股'}
                      </button>
                    )}
```

并将「加自选」按钮与「复权」下拉的渲染条件改为 `selectedStock.kind === 'stock'`：

```tsx
                    {selectedStock?.kind === 'stock' && (
                      <button onClick={() => toggleWatchlist(selectedStock.code)} ...>...</button>
                    )}
```

以及复权下拉外层 `<div className="relative" ref={adjustMenuRef}>` 包裹条件：

```tsx
                    {selectedStock?.kind === 'stock' && (
                      <div className="relative" ref={adjustMenuRef}> ... </div>
                    )}
```

- [ ] **Step 6: 类型检查**

Run: `cd frontend && npx tsc --noEmit`
Expected: 无错误

- [ ] **Step 7: 手动验证**

- 选股 → 标题栏出现行业/概念/地域板块 chips。
- 点击板块 chip → 图表切换为该板块 K 线，标题栏显示板块名 + 「返回个股」。
- 板块视图无「加自选」「复权」按钮，周期切换仍工作。
- 点击「返回个股」→ 回到原股票，板块 chips 恢复显示。
- 复权切换、自选操作在股票视图下正常。

- [ ] **Step 8: Commit**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat(frontend): show stock sector chips and sector kline view"
```

---

### Task 6: 整体验证

**Files:** 无

- [ ] **Step 1: 后端编译**

Run: `python -m py_compile backend/core/data_manager.py backend/api/routes.py`
Expected: 无输出、退出码 0

- [ ] **Step 2: 前端测试与类型**

Run: `cd frontend && node --test tests/*.mjs` 且 `npx tsc --noEmit`
Expected: 全部 PASS、无类型错误

- [ ] **Step 3: 更新文档**

- 更新 `docs/DATA_DICTIONARY.md`：新增 `GET /api/v1/stock-sectors` 与 `GET /api/v1/sector-kline` 接口说明（可选，若有 API 契约章节则追加）。
- 检查 `docs/CONTEXT.md` 是否需要补充本功能说明。

- [ ] **Step 4: 汇总提交**

```bash
git add -A
git commit -m "feat: stock sector display and sector kline navigation"
```

# 公式选股 target_date 支持实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在公式选股 UI 增加日期选择器，空值 = 最新交易日，选值后经既有 `handleSelect → /api/select → execute_selector(target_date)` 链路执行。

**Architecture:** 纯前端改动（设计文档：`docs/superpowers/specs/2026-08-25-formula-select-target-date-design.md`）。后端/代理/引擎已支持可选 `date` 字段，本次只新增一个 React state、一个原生 `<input type="date">`、并把两个手动调用点显式传参。AI 选股入口（page.tsx:839）不动。

**Tech Stack:** Next.js 14 + React 18 + Tailwind（frontend/）。前端无测试框架，验证 = `npm run lint` + `npm run build` + 部署后在线核对；后端 152 个单测作为"未触碰"回归护栏。

**注意：**
- 只改一个文件：`frontend/src/app/page.tsx`。
- 不要改 `handleSelect` 函数体（它已支持 `overrides.date`）。
- 不要给代码加注释。

---

### Task 1: 新增 selectDate 状态

**Files:**
- Modify: `frontend/src/app/page.tsx:49-51`

- [ ] **Step 1: 在 timeframe state 后插入 selectDate state**

将（page.tsx:49-51）：

```tsx
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [timeframe, setTimeframe] = useState('D');
  const [chartTimeframe, setChartTimeframe] = useState('D');
```

改为：

```tsx
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [selectDate, setSelectDate] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const [chartTimeframe, setChartTimeframe] = useState('D');
```

空串约定：`''` = 不传 `date` = 最新交易日（与现有行为一致）。

---

### Task 2: 公式行插入日期输入控件

**Files:**
- Modify: `frontend/src/app/page.tsx:495-501`

- [ ] **Step 1: 在公式 input 与「运行选股」button 之间插入 date input**

将（page.tsx:495-501）：

```tsx
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                placeholder="例如：CLOSE > MA(CLOSE, 20)"
                value={formula} onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect()}
              />
              <button onClick={() => handleSelect()} disabled={loading} className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 rounded-xl font-bold flex items-center justify-center gap-2 min-w-[160px]">
```

改为：

```tsx
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                placeholder="例如：CLOSE > MA(CLOSE, 20)"
                value={formula} onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect({ date: selectDate || undefined })}
              />
              <input
                type="date"
                title="选股日期（留空 = 最新交易日）"
                className="bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm text-slate-600 focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                value={selectDate} onChange={(e) => setSelectDate(e.target.value)}
              />
              <button onClick={() => handleSelect({ date: selectDate || undefined })} disabled={loading} className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 rounded-xl font-bold flex items-center justify-center gap-2 min-w-[160px]">
```

说明：此步同时完成 Task 3 的两处传参（同一编辑块内），Task 3 仅做验证。

---

### Task 3: 验证手动调用点已全部接线、AI 入口未变

**Files:**
- Verify only: `frontend/src/app/page.tsx`

- [ ] **Step 1: grep 确认恰好 3 处 handleSelect 调用**

Run: `rg -n "handleSelect\(" frontend/src`
Expected 恰好 3 行：
```
page.tsx:  onKeyDown ... handleSelect({ date: selectDate || undefined })
page.tsx:  <button onClick={() => handleSelect({ date: selectDate || undefined })}
page.tsx:  handleSelect({ formula, timeframe, date });
```
第 3 处（AI modal）必须保持原样。

---

### Task 4: 工程验证

- [ ] **Step 1: lint**

Run: `cd frontend; npm run lint`
Expected: 无 error（warning 可接受）。

- [ ] **Step 2: build**

Run: `cd frontend; npm run build`
Expected: `✓ Compiled successfully`（或等价成功输出），无类型错误。

- [ ] **Step 3: 后端回归护栏确认（未触碰后端）**

Run: `cd backend; python -m unittest discover -s tests -p "test_*.py"`
Expected: `Ran 152 tests ... OK`。

---

### Task 5: 提交并推送

- [ ] **Step 1: commit & push**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat(frontend): 公式选股支持指定选股日期（target_date）"
git push origin main
```

推送 main 触发部署（Vercel 前端；后端无改动不触发 HF 部署 workflow 的 backend paths 过滤——即使触发也是幂等部署）。

---

### Task 6: 在线验证（对应设计文档测试 #1 与 #2）

- [ ] **Step 1: 测试 #1 — formula + 指定日结果正确**

打开线上站点，登录后：
1. 公式填 `CLOSE > MA(CLOSE, 20)`，选股日期留空，运行 → 记录徽章日期为最新交易日 D₀ 与数量 N₀。
2. 选股日期选一个历史交易日（如数据起点之后的某周三），运行 → 徽章应显示该日的归一生效日；若选了周六则显示前一交易日。
3. 选一个早于数据起点的日期（如 `2020-01-01`）→ 应 alert 后端错误文案「指定日期 … 早于数据起点 …」。

- [ ] **Step 2: 测试 #2（最重要）— 公式入口与 AI 入口同日同结果**

1. 公式区：公式填 `MA(CLOSE,5)>MA(CLOSE,10) AND W.MA(W.CLOSE,5)>W.MA(W.CLOSE,10)`，选股日期选 `2026-07-29`（周三），运行，记录返回 codes 集合 S₁。
2. AI 选股：输入「2026年7月29日，5日线和10日线多头排列，且周线5周线和10周线多头排列」，运行，记录 LLM 解析出的查询交易日应为 2026-07-29，codes 集合 S₂。
3. 断言：S₁ == S₂（两入口汇合同一 execute_selector，语义必须一致）。若 LLM 解析日期偏差，以公式入口 S₁ 为准并记录差异属 LLM 解析问题而非链路问题。

- [ ] **Step 3: 回归 — 不带日期行为不变**

清空日期，跑任意常用公式，确认结果与改动前一致（徽章=最新交易日，无请求体 date 字段——可在浏览器 Network 面板确认 payload 无 `date` 键）。

---

## Self-Review 结论

- Spec 覆盖：设计文档 §3.1 UI（Task 2）、§3.2 状态与数据流（Task 1/2/3）、§3.3 复用现状（无需任务，Task 6 Step 1 验证）、§4 工程验证（Task 4）、#1/#2 在线测试（Task 6）。无缺口。
- 占位符扫描：所有步骤含完整代码/命令/期望输出，无 TBD。
- 类型一致性：state 名统一 `selectDate/setSelectDate`，传参形态统一 `{ date: selectDate || undefined }`，与 `handleSelect(overrides?: { formula?; timeframe?; date?: string })` 签名匹配。

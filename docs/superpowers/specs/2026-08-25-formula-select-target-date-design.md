# 公式选股 target_date 支持设计（Formula Selection Target-Date Support）

- 日期：2026-08-25
- 状态：已批准（用户确认范围为"仅前端 UI 接线"）
- 基线提交：`be7e20c`（单周期 per-code as-of 修复）

## 1. 背景与问题

AI 选股已支持从自然语言中解析查询交易日并传入执行层；但公式选股 UI 没有日期输入，
用户无法手动指定 target_date 进行历史回测式选股。

**探索结论（缩小范围的关键发现）**：target_date 链路在代码中已全线打通，唯一缺口是
公式区没有日期输入控件：

| 层 | 现状 | 位置 |
|---|---|---|
| 后端 API | ✅ `SelectionRequest.date`（可选）→ `execute_selector(target_date=req.date)` | backend/api/routes.py:29,66 |
| 引擎 | ✅ 归一非交易日、早于数据起点返回错误、D/W/M 统一 as-of、per-code last bar | backend/core/engine.py:88-101 |
| Next.js 代理 | ✅ 转发 `date`；聚合返回生效日 `date`；全节点一致 4xx 时确定性透传错误 | frontend/src/app/api/select/route.ts:33,44,82-93,111 |
| page.tsx 数据层 | ✅ `handleSelect({ date })` 已带 date 发请求；结果区已有生效日徽章 | frontend/src/app/page.tsx:284-292,543-547 |
| AI 选股入口 | ✅ `analysis.date` → `onRun(formula, timeframe, date)` → 同一 `handleSelect` | frontend/src/components/AISelectModal.tsx:291 |
| **公式选股 UI** | ❌ 无日期输入框 | frontend/src/app/page.tsx:490-520 |

## 2. 目标 / 非目标

### 目标
- 公式选股支持手动指定选股日期（target_date），空值 = 最新交易日（现有行为不变）。
- 公式入口与 AI 入口最终汇合于同一个 `/api/select` → `execute_selector(..., target_date)`，
  D/W/M as-of 语义完全一致。

### 非目标（明确不做）
- 不修改后端 routes/engine/proxy 任何代码。
- 不做 `{requested_date, effective_date}` 双字段 API 契约。
- 不把日期写进公式语言（如 `DATE=2024-06-19`）；日期是执行上下文参数，不是公式的一部分。
- 不做策略保存/加载日期、请求日→生效日映射展示等 UX 增强。

## 3. 设计

### 3.1 UI 与交互

在公式输入区（page.tsx:494 的 flex 行内、公式输入框与「运行选股」按钮之间）插入原生日期控件：

```
[公式输入框................] [选股日期📅] [运行选股] [AI 选股] [保存为策略]
```

- 控件：原生 `<input type="date">`。浏览器保证输出 `YYYY-MM-DD` 或空串，
  天然满足"API 边界统一格式"，无需手写正则校验。
- 默认空 = 最新交易日：不传 `date` 字段，与现状完全一致（零破坏）。
- 移动端唤起系统日期选择器；窄屏随现有 flex-wrap 换行。
- 样式沿用现有 class 体系（`bg-slate-50 border-slate-200 rounded-xl font-mono text-sm` 等）。

### 3.2 状态与数据流

```tsx
const [selectDate, setSelectDate] = useState('');   // '' = 最新交易日

onClick={() => handleSelect({ date: selectDate || undefined })}
onKeyDown={(e) => e.key === 'Enter' && handleSelect({ date: selectDate || undefined })}
```

- **只在手动运行时注入 `selectDate`**：「运行选股」按钮与公式框回车两处调用点显式传参；
  AI 选股仍走自己的 `analysis.date`（`handleSelect` 本体签名不动）。
- 设计取舍：若 AI 运行时也回落到 `selectDate`，用户留了日期再跑 AI 无日期语句会产生
  意外耦合，故不采用 fallback 写法。

### 3.3 错误与非交易日处理（复用现状，无新代码）

- 非交易日归一：引擎将 2024-06-22（周六）归一到最近交易日 2024-06-21，
  结果徽章显示生效日（page.tsx:543 现有逻辑）。
- 早于数据起点：后端返回 400 → 代理确定性透传（route.ts:82-93）→ alert 显示后端文案。

## 4. 测试与验证

前端无测试框架（无 jest/vitest），验证方式：

| 用户提议的测试 | 覆盖方式 |
|---|---|
| #3 不传 target_date 保持现状 | 由"空=不传字段"实现保证 + 在线核对 |
| #4 非交易日归一 | 引擎单测已覆盖（test_engine.py `test_non_trading_target_date_normalization` 等）+ 在线核对徽章 |
| #5 D/W/M 混合同一生效日 | 引擎单测已覆盖 + 在线核对 |
| #1 formula+指定日结果正确 | 部署后在线人工核对 |
| #2 公式 vs AI 同日同结果（最重要） | 部署后在线人工核对：同一公式带同一 date 从两个入口发起，比对 codes 一致 |

工程验证命令：
- `cd frontend && npm run lint`
- `cd frontend && npm run build`
- 后端回归：`cd backend && python -m unittest discover -s tests -p "test_*.py"`（应 152 全绿，证明未触碰后端）

## 5. 风险评估

- 极低：改动仅 page.tsx 一个文件（新增一个 state + 一个 input + 两处调用点传参）。
- 后端/代理零改动，152 个后端单测作为回归护栏。

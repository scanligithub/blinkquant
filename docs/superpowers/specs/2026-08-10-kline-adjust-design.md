# 设计文档：K 线图复权切换（无复权 / 前复权 / 后复权）

## 背景
- 现有 `/api/v1/kline` 已在后端通过 `data_manager._apply_forward_adjustment` 将原始价格前复权（QFQ），并保留 `adjustFactor` 列供复权逆算。
- 前端 `page.tsx` 只取了 OHLC、volume、main_net，未使用 `adjustFactor`。
- 需求：在页面添加复权按钮，提供三种复权方式，用户切换后 K 线图即时更新（包括日/周/月重采样），并在本地持久化用户选择。

## 数据流与复权公式
| 模式 | 价格公式 | 成交量公式 |
|------|----------|------------|
| **无复权** (none) | `price = qfq_price * latest_factor / factor_t` | `volume = qfq_volume * factor_t / latest_factor` |
| **前复权** (qfq) | `price = qfq_price` | `volume = qfq_volume` |
| **后复权** (hfq) | `price = qfq_price * latest_factor / first_factor` (常数倍) | `volume = qfq_volume * first_factor / latest_factor` |

- `factor_t` 为 `adjustFactor` 前向填充后的值，`latest_factor` 为序列最后一天的填充值，`first_factor` 为序列第一天的填充值。
- 当 `latest_factor <= 0`（后端 guard）视为全 1.0，三种模式等价。
- `main_net`、`time` 保持不变。

## 前端实现思路（纯前端）
1. **提取 `adjustFactor`**：在 `viewStock` 读取 Parquet 时把 `record.adjustFactor` 加入 `dailyDataCache`。
2. **纯函数 `applyAdjust`**（`frontend/src/utils/applyAdjust.ts`）
   - 前向填充 null，前导 null → 1.0，若 `latest_factor <= 0` 则全 1.0。
   - 根据 `AdjustMode`（`'none'|'qfq'|'hfq'`）返回已复权的 OHLC/volume 数组。
3. **数据派生**：
   ```tsx
   const adjustedDaily = useMemo(() => applyAdjust(dailyDataCache, adjustMode), [dailyDataCache, adjustMode]);
   const chartData = useMemo(() => resampleData(adjustedDaily, chartTimeframe), [adjustedDaily, chartTimeframe]);
   ```
   `chartData` 直接喂给 `KLineChart`，切换复权或周期时立即 recompute，无网络请求。
4. **UI**：在图表标题栏（与时间周期按钮同排）加入下拉按钮，显示当前选项（如 `无复权 ▼`），点击弹出 3 项；采用与用户菜单相同的 `clickOutside` 关闭逻辑，样式与现有按钮保持一致。
5. **持久化**：使用 `localStorage['klineAdjustMode']` 保存最近一次选择；页面挂载时读取并校验，写入在用户切换时同步。

## 边界处理
- **缺失 `adjustFactor` 列**：后端已经在 `routes.py` 中排除此列，若前端未收到，`applyAdjust` 会把所有因子视为 1.0，三种模式等价，页面表现不变。
- **因子全为 null**：前向填充后全为 1.0，公式自然退化为原始数据。
- **`latest_factor <= 0`**：直接返回未复权数据（即 `adjustFactor` 全部 1.0），保持与后端的 `qfq_expr` guard 一致。
- **成交量换算**：仅在无复权和后复权时按公式换算，`qfq` 保持原始后端返回的 volume。
- **指标重算**：`KLineChart` 所有指标基于传入的 `data` 自动重算，无额外修改。

## 测试计划
- 新增纯函数单元测试 `frontend/tests/apply-adjust.test.mjs`（复制实现方式以规避 TS import），覆盖:
  - 前复权直接返回。
  - 无复权正确恢复原始价、成交量换算。
  - 后复权常数缩放。
  - `adjustFactor` 为 `[null, 1.2, null, 1.5]` 时的前向填充逻辑。
  - 因子缺失/`latest_factor <= 0` 时三种模式等价。
- 运行 `node --test frontend/tests/*.mjs`，确保全部通过。
- `npx tsc --noEmit` 检查 TypeScript 编译无误。
- 手动在浏览器点击复权按钮切换，确认图表、指标、周期切换均即时更新且状态在刷新后保持。

## 影响范围
- 前端：`frontend/src/app/page.tsx`、`frontend/src/utils/applyAdjust.ts`、新增测试文件。
- 后端：无修改，保持现有前复权实现。
- 文档：本设计文档、后续实现计划。

## 里程碑
1. 完成设计文档并提交 (`docs/superpowers/specs/...`).
2. 编写实施计划并提交 (`docs/superpowers/plans/...`).
3. 实现代码、单元测试、UI。
4. 本地 `npm run lint`、`node --test`、手动 UI 验证。
5. 提交 PR 并通过 CI（暂无 CI，仅人工检查）。

# 实施计划：K 线复权切换（无复权 / 前复权 / 后复权）

## 前置条件
- 已完成设计文档 `docs/superpowers/specs/2026-08-10-kline-adjust-design.md` 并提交。
- 项目已构建成功，可运行 `npm --prefix frontend run lint` 与 `node --test frontend/tests/*.mjs`。

## 任务列表
1. **新增纯函数**
   - 路径：`frontend/src/utils/applyAdjust.ts`
   - 实现 `AdjustMode`、`ADJUST_OPTIONS`、`applyAdjust`（含因子前向填充、guard、价格/成交量换算）。
2. **新增单元测试**
   - 路径：`frontend/tests/apply-adjust.test.mjs`
   - 按设计文档中的场景复制实现，覆盖前复权、无复权、后复权、null 填充、因子缺失等。
3. **页面代码修改** (`frontend/src/app/page.tsx`)
   - 导入 `applyAdjust`、`ADJUST_OPTIONS`。
   - 添加 `adjustMode`、`adjustMenuOpen` 状态，读取/写入 `localStorage['klineAdjustMode']`。
   - 在 `viewStock` 中把 `record.adjustFactor` 加入 `dailyDataCache`（结构保持 `time, open, high, low, close, volume, main_net, adjustFactor`）。
   - 使用 `useMemo` 计算 `adjustedDaily` 并在 `resampleData` 调用处换成 `adjustedDaily`。
   - 实现复权按钮 UI（与时间周期按钮同排、下拉列表、点击外部关闭）。
   - 在复权切换处理函数中更新 `adjustMode`、写入 `localStorage`，并同步重新计算 `selectedStock.data`（调用 `applyAdjust` + `resampleData`）。
4. **样式检查**：确保新按钮的 CSS 与现有按钮保持一致（`px-3 py-1 text-xs font-bold rounded-md` 等），在暗/浅模式下可读。
5. **本地验证**
   - 运行 `node --test frontend/tests/apply-adjust.test.mjs`，确保全部通过。
   - 执行 `npx tsc --noEmit`（前端类型检查）。
   - 启动开发服务器 `npm --prefix frontend run dev`，手动切换复权、时间周期，确认图表、指标即时更新，刷新后复权状态从 `localStorage` 恢复。
6. **提交**
   - `git add docs/superpowers/specs/2026-08-10-kline-adjust-design.md`
   - `git add docs/superpowers/plans/2026-08-10-kline-adjust.md`
   - `git add frontend/src/utils/applyAdjust.ts frontend/src/app/page.tsx frontend/tests/apply-adjust.test.mjs`
   - `git commit -m "feat(kline): add price adjustment selector (none/qfq/hfq) with persistent UI"`
   - `git push origin main`

## 验收标准
- 单元测试全部通过。
- 手动 UI 检查：复权按钮默认显示「无复权」，下拉含三项，选中后按钮文字更新，图表立即刷新（包括日/周/月）且所有指标重新计算。
- 刷新页面后，`localStorage` 中保存的复权模式被读取并自动生效。
- 代码通过 `npm lint`、`npm test`，无 TypeScript 错误。
- 代码已提交并推送至 `main` 分支，CI（如果有）通过。

# P4.3 — 回测策略与结果可追溯关联

**Date:** 2026-09-28
**Status:** Completed
**Depends on:** P4.1 / P4.2

## Goal

建立“用户模板 → 单次回测任务 → 回测结果”的稳定追溯关系。

核心原则：

- Node1 Scheduler SQLite 继续作为模板与任务状态的持久化真源；
- 单次回测的日期、初始资金仍属于任务，不进入模板；
- 回测任务继续保存实际提交的完整策略配置，因此模板后续修改不会改变历史回测；
- 模板关联必须由 Node1 校验，不能信任前端直接提供的模板归属；
- 如果任务实际配置与模板当前配置不一致，则不建立模板关联，避免错误标注；
- 不增加参数优化、多组合搜索或结果比较功能。

## P4.3.1 Task → Template 关联

在 `task_queue` 增加：

- `strategy_template_id`
- `strategy_template_name`
- `strategy_template_updated_at`

创建 backtest task 时：

1. Vercel 认证用户后可附带模板 id；
2. Node1 使用 task 的 `user_id` 查询模板；
3. 校验模板属于当前用户；
4. 将模板的 `strategy + fee_policy + benchmark + min_listing_days + exclude_st` 与任务实际配置做规范化后精确比较；
5. 一致则写入模板 id、名称、updated_at；
6. 不一致则任务仍可正常创建，但模板关联为空。

这样前端即使保留了过期的模板选择状态，也不会把错误模板写入回测历史。

## P4.3.2 Result Traceability

任务完成时：

- 结果 `meta.json` 保存模板 id、名称、模板 updated_at；
- `task_queue` 中保留同样的模板元数据；
- 回测实际策略配置继续保存在任务 payload / result meta 中；
- 模板被删除或后续修改后，历史任务仍能知道当时来源模板及实际配置。

## P4.3.3 Frontend

任务记录中的 backtest 显示：

- 模板名称（若存在）；
- 模板 id（辅助追踪）；
- 原有收益、回撤、交易数等摘要保持不变。

回测参数页显示“策略模板”信息。

## Acceptance

### Backend

- 新 SQLite 数据库自动创建新字段；
- 已存在数据库通过 migration 自动增加字段；
- 不存在模板 / 跨用户模板 / 配置不一致时不能产生错误模板关联；
- 配置一致时正确保存模板 id/name/updated_at；
- task result meta 保留模板元数据；
- 原有 task CRUD / result persistence 不回归。

### Frontend

- 使用并保存用户模板后直接运行回测，任务记录显示模板来源；
- 模板修改后再次回测，产生新的模板 updated_at 快照；
- 修改回测面板使其与所选模板不一致后运行，不标记为该模板；
- 内置模板不产生数据库模板 id；
- 原有 P4.1/P4.2 功能保持通过。

### Scope boundary

P4.3 不实现：

- 多次回测收益横向比较；
- 参数优化；
- 多组合批量回测；
- 模板版本分支 / Git 式版本管理。

## Production Acceptance

- **Status:** PASS
- **Environment:** https://blinkquant.de5.net/
- **Verified:** template binding, immutable historical template snapshot, updated template version attribution, and stale/manual parameter modification without false attribution.
- **Evidence:** user production acceptance on 2026-09-28 using template `P4.3-Test` (#9), tasks #6/#7/#8.

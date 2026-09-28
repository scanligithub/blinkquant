# P4.4 — 历史回测任务重跑与来源追踪

**Date:** 2026-09-28  
**Status:** Completed  
**Depends on:** P4.3 模板来源追溯

## Goal

让已完成/失败/取消/被抢占的历史 backtest task 可以一键重跑，并建立明确的 source_task_id 来源关系。

核心原则：

- 历史 task 的 payload 是重跑的唯一输入快照；
- 新任务获得独立 task id，不修改原任务；
- 重跑时保留原始日期、初始资金和完整策略配置；
- 若原任务绑定用户模板，则按当前模板状态重新校验；
- 模板仍存在且 canonical config 一致 → 新任务保留当前模板 id/name/updated_at；
- 模板已修改或删除 → 新任务照常创建，但不产生错误模板归属；
- source task 必须属于当前用户，不能跨用户重跑；
- selection task 不提供重跑；
- 不进入参数优化、多组合回测、批量搜索。

## P4.4.1 Task rerun API

Node1 Scheduler 新增：

POST /internal/tasks/{task_id}/rerun

行为：

1. 校验 task 存在；
2. 校验当前用户访问权限；
3. 仅允许 task_type=backtest；
4. 仅允许 terminal 状态：done / failed / cancelled / preempted；
5. 从原 task 读取完整 payload；
6. 使用原 task 的 template id 尝试重新验证当前模板；
7. 插入新的 pending task；
8. 新任务写入 source_task_id = 原 task id。

原 task 不发生任何修改。

## P4.4.2 Persistence

task_queue 增加：

- source_task_id INTEGER

增加索引：

idx_tq_source_task

任务 API 返回 source_task_id。

## P4.4.3 Frontend

历史 backtest task 增加：

- 重跑按钮；
- 显示 重跑自 #N 来源信息。

点击重跑后：

- 创建新 task；
- 新 task 进入正常 Scheduler 调度；
- 原 task 保持不变；
- 页面继续通过任务轮询获取结果。

## Traceability rules

### 原模板仍未变化

原 task：

template #9 / version V1

重跑：

source_task_id = old_task_id
template #9 / version V1

### 原模板已修改

原 task：

template #9 / version V1

当前模板：

template #9 / version V2

重跑旧 payload：

source_task_id = old_task_id
strategy_template_id = NULL

回测仍正常执行。

### 原模板已删除

重跑旧 payload：

source_task_id = old_task_id
strategy_template_id = NULL

回测仍正常执行。

## Acceptance

### Backend

- 新 DB 自动包含 source_task_id；
- 老 DB migration 正常增加字段；
- 同用户 terminal backtest 可以 rerun；
- running/pending task 不能 rerun；
- selection 不能 rerun；
- 跨用户访问被拒绝；
- 新 task payload 与原 task 完全一致；
- 新 task source_task_id 正确；
- 模板当前一致时重新绑定；
- 模板修改/删除时取消绑定但仍能运行；
- 原 task 不被修改。

### Frontend

- 完成的 backtest 显示“重跑”；
- 点击后产生新的 task；
- 新任务显示来源 task；
- 原任务仍保留原结果及模板快照。

## Scope boundary

P4.4 不实现：

- 参数优化；
- 多组合比较；
- 批量回测搜索；
- Git 式模板分支；
- 自动参数寻优。

# BlinkQuant IA v2：IA3.2 成果库

状态：实现中

## 范围

首版成果库基于现有已完成任务结果构建统一浏览入口，不改变 Scheduler 和结果计算内核。

成果引用使用完成任务 ID 作为当前阶段稳定引用键：

- 选股成果：`/artifacts/selections/[artifactId]`
- 回测成果：`/artifacts/backtests/[artifactId]`

## 数据来源

- 选股：Node1 `task_queue.result`，任务输入中的 `selection_strategy_snapshot` 用于策略版本溯源。
- 回测：Node1 `task_queue.result_summary`、`result_uri` 以及任务 `payload`；大型数据继续由现有 Artifact API 提供。

## 不变语义

成果详情只读取完成任务时保存的 payload/result/summary，不重新读取当前策略配置，因此策略后续版本变化不会改变历史成果展示。

回测基础选股策略与回测当前 Entry 是两个独立快照；成果详情分别展示。

## 后续增强

当前阶段仍以任务结果作为成果存储基础。独立 artifact registry、独立保留策略和跨 100 条任务分页属于后续持久化增强，不在本阶段改变现有任务执行链。
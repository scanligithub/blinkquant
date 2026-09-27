# P4.1 — 回测策略模板

## Goal

将当前通用回测的完整策略配置保存为用户级可复用模板，同时保持已有选股策略存储不变。

## Template boundary

模板保存：
- `strategy`：Universe / Entry / Exit / Sizing / Rebalance / Mode
- `fee_policy`
- `benchmark`
- `min_listing_days`
- `exclude_st`

模板不保存：
- `start_date`
- `end_signal_date`
- `initial_cash`

因此模板描述的是“怎么回测”，而不是某一次具体回测任务。

## Persistence

`backtest_strategy_templates` 独立于旧 `strategies` 表，避免把旧选股策略的 `formula + timeframe` 数据模型强行升级为回测模型。

## UI

`BacktestPanel` 提供：
- 保存当前配置为模板
- 加载模板
- 删除模板

加载模板只覆盖策略配置，不覆盖本次回测的日期和初始资金。

## API

`/api/backtest-strategy-templates`：
- GET：当前用户模板列表
- POST：创建模板
- PUT：更新模板
- DELETE：删除模板

所有操作均按当前登录用户隔离。

## Acceptance

1. 新模板可保存完整 canonical strategy。
2. 页面重新打开后可读取模板。
3. 应用模板后 Entry/Exit/Universe/Sizing/Rebalance/Mode/Fee/Benchmark/过滤条件全部恢复。
4. 日期和初始资金不被模板覆盖。
5. 删除模板只影响当前用户自己的模板。
6. 旧 `/api/strategies` 与选股策略功能不受影响。

## Next

P4.1 后续再增加模板编辑/复制和内置策略模板；本阶段不进入参数优化与多组合回测。
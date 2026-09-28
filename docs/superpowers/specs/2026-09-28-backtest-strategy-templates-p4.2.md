# P4.2 — 回测策略模板增强

**Date:** 2026-09-28
**Status:** Active
**Depends on:** P4.1 回测策略模板

## Goal

在 P4.1 的“保存 / 应用 / 删除”基础上，继续增强模板复用能力，同时保持：
- 模板仍由用户级策略配置组成；
- Node1 Scheduler SQLite 为持久化真源；
- Vercel 负责认证与代理；
- 日期与初始资金仍属于单次回测，不进入模板；
- 不进入参数优化、多组合回测或组合搜索。

## P4.2.1 模板编辑

用户可以选择一个已有模板：
1. 将模板配置加载到回测面板；
2. 修改 Entry / Exit / Universe / Sizing / Rebalance / Mode / Fee / Benchmark / 过滤条件；
3. 使用原模板 id 通过 PUT 更新；
4. 名称在当前用户内继续受 UNIQUE(user_id,name) 约束。

编辑过程中：
- 不修改 start_date / end_signal_date / initial_cash；
- 取消编辑不会改变数据库；
- 更新失败时保留当前编辑状态。

## P4.2.1 模板复制

用户可以复制一个已有模板：
- 复制完整 config；
- 默认名称为“原名称 副本”；
- 新模板使用 POST 创建，因此获得新的 id；
- 复制结果仍属于当前登录用户；
- 不复制其他用户模板。

## P4.2.2 内置模板

增加只读内置模板集合，用于快速开始常见回测策略。

内置模板不写入用户 SQLite，且不可被用户 PUT / DELETE。
用户点击“使用”后，仅将配置加载到回测面板；用户如需保存，可另存为自己的模板。

MVP 内置模板：
- MA20 趋势：CLOSE > MA(CLOSE,20)
- MA5/MA20 趋势：MA(CLOSE,5) > MA(CLOSE,20)
- MA5/MA60 趋势：MA(CLOSE,5) > MA(CLOSE,60)
- 沪深300 周调仓：000300 + MA(CLOSE,20) + weekly Top-N

## Acceptance

### Edit
- 当前用户可以编辑自己的模板；
- 更新后刷新页面，修改后的 config 保留；
- 其他用户看不到该模板。

### Copy
- 复制得到新 id；
- 原模板不受影响；
- 新模板与原模板 config 相同；
- 新模板可以再次编辑。

### Built-in
- 所有用户都能看到内置模板；
- 内置模板不可删除、不可覆盖；
- 使用内置模板不会覆盖日期和初始资金；
- 使用后可以另存为用户模板。

### Regression
- P4.1 CRUD / persistence / restart / isolation 保持通过；
- Template → real backtest 保持通过；
- Golden / full backend tests 保持通过；

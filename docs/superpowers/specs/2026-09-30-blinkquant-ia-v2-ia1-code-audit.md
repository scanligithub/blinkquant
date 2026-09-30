# BlinkQuant IA-1 前代码盘点

日期：2026-09-30
基线 commit：f13a0746164d9c54f29b2f1950b5a7ef496f4c92
目标：为 IA-1 页面拆分建立真实代码基线；本阶段不修改业务实现。

## 1. 当前前端路由现状

App Router 当前实际页面只有：

- `/`：全部业务聚合在 `frontend/src/app/page.tsx`
- `/login`
- `/register`
- `/admin`

目标 IA v2 的 `/select`、`/stocks`、`/watchlists`、`/strategies`、`/backtests`、`/artifacts`、`/tasks`、`/system` 尚未建立为独立页面。

当前 API 已存在较多业务能力，但 API 路由与产品页面尚未一一映射。

## 2. page.tsx 现状

文件：`frontend/src/app/page.tsx`

规模：1206 行，约 53.7 KB。

这是一个大型 Client Component，同时承担：

1. 登录态检查与退出登录
2. 顶部用户菜单
3. 集群健康/节点/队列展示
4. 选股公式编辑与选股任务提交
5. 选股结果展示
6. 股票搜索
7. 股票 K 线加载、复权、日/周/月重采样
8. 股票板块加载与板块 K 线
9. 自选股增删与展示
10. 回测参数面板与回测任务提交
11. 回测结果展示
12. 任务列表
13. 策略列表弹窗
14. 保存策略弹窗
15. AI 选股弹窗
16. 移动端全屏/横竖屏处理

因此当前 `page.tsx` 同时包含“页面编排、业务状态、数据获取、任务轮询、股票研究交互、弹窗控制”。

## 3. page.tsx 状态分组

### A. 全局/认证状态

```
user
authLoading
userMenuOpen
```

归属：AppShell / Auth shell。

### B. 选股状态

```
formula
timeframe
selectDate
results
loading
selectMeta
```

归属：Selection Workspace。

### C. 股票研究状态

```
selectedStock
chartLoading
dailyDataCache
sectorDataCache
sectors
expandedSectors
stockList
chartTimeframe
subChartType
mainChartType
adjustMode
adjustMenuOpen
isFullScreen
showRotateHint
```

归属：Stock Research。

### D. 自选股状态

```
watchlistCodes
```

归属：Watchlist。

注意：当前只有一个 `watchlist` 集合，没有“一个用户多个自选股列表”的数据模型。

### E. 回测状态

```
backtestResult
backtestLoading
backtestTemplateId
```

归属：Backtest Research。

### F. 策略弹窗状态

```
showStrategies
saveStrategyOpen
strategyName
```

归属：Strategy Library。

### G. AI 选股

```
showAISelect
```

归属：Selection Workspace 的辅助入口。

### H. 集群状态

```
clusterStatus
useCluster().nodes
useCluster().queues
useCluster().myTasks
```

当前存在两套集群信息来源。

## 4. 当前组件与页面职责映射

| 当前组件 | 当前职责 | IA v2 目标 |
|---|---|---|
| StockSearch | 股票搜索 | 股票研究 |
| KLineChart | K 线/技术图表 | 股票研究 |
| Watchlist | 单一自选列表展示 | 自选股 |
| StrategyList | 旧版选股策略列表弹窗 | 策略库 |
| BacktestPanel | 回测完整参数配置 | 回测研究 |
| BacktestStrategyTemplates | 回测策略模板 CRUD | 策略库/回测研究 |
| BacktestResults | 回测报告 | 成果库 |
| TaskList | 任务列表 + 取消/删除/重跑/查看成果 | 任务中心 |
| AISelectModal | AI 选股入口 | 选股 |
| ClusterStatusBar | 集群状态组件 | 系统状态 |
| EquityCurveChart | 回测权益曲线 | 成果库 |
| TradesTable | 回测交易明细 | 成果库 |
| PositionsTable | 回测持仓明细 | 成果库 |

其中 `ClusterStatusBar` 已存在，但当前首页没有直接使用它，而是自己实现了一套节点状态卡片。

## 5. page.tsx 关键业务函数

### 选股

`handleSelect()`

流程：

```
公式/日期
  ↓
submitTask('selection')
  ↓
/api/v1/tasks/{id} 轮询
  ↓
task.result.codes
  ↓
results
```

这是 IA-1 第一优先级，必须原样保留。

### 回测

`handleBacktest()`

流程：

```
BacktestPanel
  ↓
submitTask('backtest')
  ↓
/api/v1/tasks/{id} 轮询
  ↓
result_summary/result_uri
  ↓
BacktestResults
```

### 股票研究

`viewStock()`

流程：

```
/api/kline
  ↓
Parquet ArrayBuffer
  ↓
hyparquet 客户端解析
  ↓
复权
  ↓
D/W/M 重采样
  ↓
KLineChart
```

并额外调用：

`/api/stock-sectors`

### 板块研究

`viewSector()`

流程：

```
/api/sector-kline
  ↓
Parquet
  ↓
重采样
  ↓
KLineChart
```

### 自选股

`toggleWatchlist()`

当前只操作：

```
/api/watchlist
GET/POST/DELETE
```

当前后端模型也是单一 watchlist。

### 策略保存

`handleSaveStrategy()`

当前操作：

```
/api/strategies
POST
```

这是旧版选股策略存储模型。

## 6. 当前 API → 产品对象映射

| API | 当前用途 | IA v2 对象 |
|---|---|---|
| `/api/select` | 旧选股接口 | 选股执行兼容层 |
| `/api/v1/tasks` | 统一任务创建/列表 | Task |
| `/api/v1/tasks/[id]` | 任务详情/取消/删除 | Task |
| `/api/v1/tasks/[id]/rerun` | 回测重跑 | Task |
| `/api/v1/tasks/[id]/artifact` | 成果 Artifact 下载 | Artifact |
| `/api/strategies` | 旧选股策略 CRUD | Strategy（待迁移） |
| `/api/backtest-strategy-templates` | 回测模板 CRUD | Backtest Strategy（当前实现） |
| `/api/watchlist` | 单一自选股集合 | Watchlist（待升级为多列表） |
| `/api/kline` | 股票数据 | Stock Research |
| `/api/sector-kline` | 板块数据 | Stock Research |
| `/api/stock-sectors` | 股票板块关系 | Stock Research |
| `/api/stock-list` | 股票搜索基础列表 | Stock Research |
| `/api/v1/cluster/status` | 集群状态 | System |
| `/api/status` | 旧集群状态接口 | System/兼容层 |

## 7. 持久化现状与 IA v2 差距

### 用户数据数据库

`frontend/scripts/init_db.sql` 当前定义：

```
users
watchlist
strategies
```

其中 `watchlist` 是：

```
user_id + code
```

没有：

```
watchlist_id
watchlist_name
watchlist_members
```

所以当前还不能表达 IA v2 中：

```
一个用户
  ├── 自选股列表 A
  ├── 自选股列表 B
  └── 自选股列表 C
```

### Node1 Scheduler SQLite

`backend/scheduler/schema.sql` 当前已经有：

```
cluster_nodes
task_queue
backtest_strategy_templates
node_heartbeats
```

其中 `task_queue` 已保存：

- task_type
- payload
- result_summary
- result_uri
- strategy_template_id
- strategy_template_name
- strategy_template_updated_at
- source_task_id
- progress

因此“任务 → 回测结果 Artifact”的基础已经存在。

### 策略模型存在两套

当前形成：

```
/api/strategies
    ↓
Vercel 用户数据库
    ↓
旧选股策略

/api/backtest-strategy-templates
    ↓
Node1 Scheduler SQLite
    ↓
回测策略模板
```

这与 IA v2 的：

```
统一 Strategy 域
├── Selection Strategy
└── Backtest Strategy
```

仍有结构性差距。

本次代码拆分不直接解决这个数据迁移问题，但必须保留清晰边界，避免新页面继续扩大两套模型的耦合。

## 8. 当前任务与成果关系

当前已经具备：

```
Task
  ├── payload
  ├── result_summary
  ├── result_uri
  ├── strategy_template_id
  └── source_task_id
```

Backtest Artifact 当前包括：

```
equity_curve
trades
positions_daily
```

这已经接近 IA v2 的：

```
策略版本
   ↓
任务
   ↓
成果
   ↓
Artifact
```

但“独立成果对象/成果库页面”仍未建立，当前成果入口仍挂在任务列表和首页状态里。

## 9. 当前代码中值得在 IA-1 一并收口的问题

### 9.1 集群状态存在重复来源

`page.tsx` 自己每 5 秒调用 `/api/status`。

`useCluster()` 又每 2 秒调用 `/api/v1/cluster/status`。

结果是首页同时维护：

```
clusterStatus
clusterNodes
queues
myTasks
```

IA-1 应统一为一个集群状态数据源。

### 9.2 TaskList 自己再次调用 useCluster()

首页已经调用一次：

`useCluster()`

而 `TaskList` 内部再次调用：

`useCluster()`

因此同一页面可能产生重复的任务/集群轮询。

IA-1 不应让页面级状态和 TaskList 各自拥有一套轮询状态。

### 9.3 任务中心仍嵌套在回测页

当前：

```
回测 tab
 ├── BacktestPanel
 ├── BacktestResults
 └── TaskList
```

IA v2 应调整为：

```
回测研究
任务中心
成果库
```

三个独立职责。

### 9.4 回测结果存在双重渲染入口

当前回测 tab 左侧包含 `BacktestResults`，右侧又包含 `BacktestResults`。

这是历史聚合式页面留下的结构，拆页后应只保留一个正式结果页面。

### 9.5 选股策略和回测策略语义尚未统一

`StrategyList` 是旧选股策略；

`BacktestStrategyTemplates` 是回测策略模板。

IA v2 中二者都属于 Strategy 域，但不能通过“一个旧组件改名”简单合并，必须保留类型与版本边界。

### 9.6 自选股目前不是多列表

这是产品对象层面的真实缺口，不能仅靠页面拆分解决，需要后续数据模型升级。

### 9.7 page.tsx 有历史兼容代码

当前仍存在：

- 旧的 `/api/backtest` 兼容路径相关常量
- 旧回测异步状态 localStorage 恢复逻辑
- 新任务队列与旧直连结果格式兼容
- 旧策略 CRUD 与新回测策略模板并存

IA-1 不直接删除，先完成迁移；等新页面经过 E2E 后再清理。

## 10. IA-1 最小安全拆分方案

### IA-1.1 应用壳

新增：

```
frontend/src/components/app/AppShell.tsx
frontend/src/components/app/MainNav.tsx
```

负责：

- 登录后全局布局
- 一级导航
- 用户菜单
- 管理员入口
- 全局系统状态入口

不负责选股/回测业务逻辑。

### IA-1.2 选股工作台

新增：

```
frontend/src/app/select/page.tsx
frontend/src/components/select/SelectionWorkspace.tsx
frontend/src/components/select/SelectionResults.tsx
```

第一阶段直接复用当前：

- formula
- timeframe
- selectDate
- handleSelect
- AISelectModal
- StrategyList 的“应用策略”能力

不改变任务 API 和调度逻辑。

### IA-1.3 股票研究先抽组件，不先建复杂路由

从首页抽出：

```
StockResearchPanel
StockHeader
StockChartToolbar
```

保留：

```
viewStock
viewSector
resampleData
applyAdjust
```

这样可以把股票研究从选股工作台中剥离，但不立即引入复杂数据层重构。

### IA-1.4 首页兼容策略

第一阶段：

```
/
  ↓
继续可用

/select
  ↓
使用新的 SelectionWorkspace
```

二者先共用组件，不立即删除旧首页。

只有 `/select` E2E 稳定后，才考虑：

```
/
  → /select
```

## 11. IA-1 明确不做的事情

本阶段不做：

- Scheduler 重构
- 任务状态机重构
- 回测引擎重构
- Artifact 存储重构
- 选股引擎重构
- 策略数据库迁移
- 多自选股数据库迁移
- 大规模 CSS/视觉重设计

原因：这些属于产品对象/数据层或计算层变化，不应与页面 IA 拆分同时发生。

## 12. IA-1 验收标准

完成 IA-1 后必须满足：

1. `/select` 可直接访问。
2. 登录状态正常。
3. 现有选股公式执行结果不变。
4. 选股任务仍通过 `/api/v1/tasks` → Node1 Scheduler。
5. 不修改选股优先/回测抢占语义。
6. 首页 `/` 继续可用。
7. 股票详情研究逻辑没有回归。
8. 后续可以在不复制业务逻辑的情况下增加 `/stocks`、`/watchlists`、`/strategies`、`/backtests`、`/tasks`、`/artifacts`。

## 13. 推荐的实际实施顺序

```
IA-1.1 AppShell
   ↓
IA-1.2 /select + SelectionWorkspace
   ↓
IA-1.3 将股票研究抽成 StockResearchPanel
   ↓
IA-1.4 / 与 /select 双入口回归
   ↓
IA-1.5 再开始 Strategy / Watchlist / Backtest / Task / Artifact 页面
```

结论：

> 当前最大的技术问题不是计算引擎，而是 `page.tsx` 同时拥有过多业务状态和页面职责。
>
> IA-1 应先建立“应用壳 → 选股工作台 → 股票研究组件”的边界，再进行后续对象页面化。

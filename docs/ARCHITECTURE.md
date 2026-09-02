# BlinkQuant 系统架构文档

版本: v1.1 | 更新: 2026-08-24

---

## 整体架构

BlinkQuant 采用四层分离架构：数据生产端 + 计算端 + 服务端 + 展示端。

分片策略：code.hash() % total_nodes 确保分布均匀，单节点内存 ~2-3GB。
前复权：qfq_expr = adj_col / latest_adj 纯向量化，无循环。
板块映射：行业优先 1-to-1，概念兜底，unique(subset=[code], keep=first) 防止 1-to-N 膨胀。
热 JIT：首次遇到新指标时，仅在最后 1 年数据上计算并广播挂载到全量 DataFrame。

---

## 数据流向

### 1. 数据生产 (stockA 仓库)

GitHub Actions (每日 03:00 CST) 下载 GBBQ.zip，Go TDX Engine 19 并行分片获取全量 K线 + 复权因子 + 股本变动，东方财富 F10 (37 字段财务) + 雪球主营业务，板块自愈数据，merge_and_push.py (DuckDB 零拷贝合并) 年度分片 Parquet (ZSTD)，复权因子 4 规则审计 (A1/A2/B/C)，TTM 净利润向量化计算，财务 ASOF JOIN (TradeDate >= NoticeDate)，推送 HF Dataset scanli/stocka-data。

### 2. 数据消费

用户请求 -> Vercel 前端。选股: POST /api/v1/select (并发 3 节点 Promise.all) -> FastAPI + Polars Lazy 执行 -> 返回代码列表。K线: GET /api/v1/kline (Parquet 二进制流) -> hyparquet WASM 解析 -> 前端轻量图表计算指标。搜索/列表: 缓存 + 后端索引。

### 3. 多周期选股 (v3.0)

用户输入含周期前缀的公式（如 `W.MA(W.CLOSE, 20) > 10`）-> `parse_multi_tf` 返回 plan tree -> 每个 atom 独立求值（D 走 df_daily，W/M 走 `build_asof_frame`）-> 布尔折叠（AND→交集，OR→并集）-> 返回代码列表。

---

## 多周期选股 DSL (v3.0)

### 语法

```
周期前缀 := D | W | M
字段 := [周期前缀 '.'] FIELD_NAME
函数 := [周期前缀 '.'] FUNC_NAME '(' 参数列表 ')'
原子 := 字段 比较运算符 (字段 | 常量) | 函数 比较运算符 (字段 | 常量)
表达式 := 原子 (AND | OR 原子)*
```

### 示例

| 公式 | 含义 |
|------|------|
| `CLOSE > 10` | 日线收盘价 > 10（无前缀 = 日线） |
| `W.MA(W.CLOSE, 20) > 10` | 周线20周均线 > 10 |
| `M.CLOSE > M.MA(M.CLOSE, 10)` | 月线收盘价 > 10月均线 |
| `W.MA(W.CLOSE,5) > W.MA(W.CLOSE,20) AND VOL > MA(VOL,5)*2` | 周线均线多头 + 日线放量 |
| `CROSS_UP(W.MACD_DIF(12,26), W.MACD_DEA(12,26,9))` | 周线MACD金叉 |

### 约束规则

1. **无前缀 = 日线**：`CLOSE` 等价于 `D.CLOSE`
2. **显式前缀**：`D.` = 强制日线；`W.` = 周线；`M.` = 月线
3. **同一原子内禁止混用**：`W.MA(CLOSE, 20)` 非法，须写 `W.MA(W.CLOSE, 20)`
4. **板块字段限制**：`S_CLOSE` 等板块字段仅允许在无前缀原子中使用
5. **跨周期组合**：不同周期原子通过 AND/OR 组合时，各自独立求值后取交集/并集

### As-of Frame 构建

对于非基础周期（W/M），`build_asof_frame(tf, target_date)` 构建截至 target_date 的完整数据：

```
completed = 周期表中 date < cur_start 的完整周期行
partial   = 日线数据 cur_start ≤ date ≤ target_date，按 code 分组合成单行
result    = concat([completed, partial])
```

- 周线 cur_start = target_date 所在周的周一
- 月线 cur_start = target_date 所在月的第一天
- LRU 缓存最近 8 个 (tf, target_date) 结果

### Plan Tree

`parse_multi_tf` 返回的 plan tree 结构：

```json
{"type": "atom", "tf": "W", "expr": pl.Expr}
{"type": "bool", "op": "AND", "children": [...]}
```

引擎 `_fold_plan` 递归折叠：atom 返回 code set，bool 按 op 做集合运算。

---

## 技术栈

| 层级 | 技术选型 | 版本/说明 |
|------|----------|-----------|
| 前端框架 | Next.js 14 + React 18 | Vercel 部署 |
| UI/样式 | Tailwind CSS | JIT 模式 |
| 图表 | lightweight-charts | 高性能 Canvas |
| 数据解析 | hyparquet + hyparquet-compressors (WASM) | ZSTD 解压 |
| 拼音搜索 | pinyin-pro | 首字母匹配 |
| 后端框架 | FastAPI + Uvicorn | Python 3.10 |
| 计算引擎 | Polars (Lazy/Streaming) | 向量化、内存高效 |
| 存储格式 | Parquet + ZSTD | 列式压缩、Predicate Pushdown |
| 数据源 | Hugging Face Dataset | scanli/stocka-data |
| 部署平台 | HF Spaces + Vercel | 3 节点 16GB RAM |
| CI/CD | GitHub Actions | 每日冷启动 + 手动触发 |
| 数据生产 | Go (TDX) + Python (DuckDB/Polars) | stockA 仓库 |

---

## 部署拓扑

### Hugging Face Spaces (后端 3 节点)

| 节点 | Space 名称 | 内存 | 数据分片 |
|------|-----------|------|---------|
| Node 0 | scanli-blinkquant-node1 | 16GB | code.hash() % 3 == 0 |
| Node 1 | scanli-blinkquant-node2 | 16GB | code.hash() % 3 == 1 |
| Node 2 | scanli-blinkquant-node3 | 16GB | code.hash() % 3 == 2 |

每日冷启动流程 (GitHub Actions daily_cron.yml)：
1. 生成唯一 BUILD_ID (纳秒时间戳) 写入 build_id.txt
2. git push -f 触发 HF Space 重新构建
3. 轮询 HF API 监控 runtime.stage (BUILD_ERROR/CRASHED 即失败)
4. 容器 RUNNING 且 SHA 匹配后，轮询 /api/v1/health 验证 status=healthy 且 build_id 一致
5. 全流程约 4-5 分钟/节点，3 节点并行约 6-8 分钟完成

### Vercel (前端)
- 自动部署：推送 main 分支触发
- API 重写：next.config.js 代理到 3 个 HF 节点
- 无构建依赖：node_modules 不提交，构建时 npm install

### 用户体系 (v2.1)
- 认证全部在前端 Next.js API 层，用户数据存 Vercel Postgres（`POSTGRES_URL`）
- JWT HS256 存 HttpOnly Cookie `__auth_token`（httpOnly + secure + sameSite=lax，7 天）
- 密码 bcryptjs (cost 10)；`AUTH_SECRET` 为 JWT 密钥，生产必须配置
- 管理员引导：`AUTH_ADMIN_EMAIL`/`AUTH_ADMIN_PASSWORD` 环境变量，首次登录幂等创建
- 后端 3 节点保持无状态，不感知用户体系

---

## 核心模块职责

| 模块 | 文件 | 核心职责 |
|------|------|---------|
| 数据管理 | backend/core/data_manager.py | 流式下载、分片加载、前复权、内存优化、重采样、板块映射、as-of frame 构建 |
| 选股引擎 | backend/core/engine.py | 热 JIT 编译、Lazy 执行、安全板块 Join、多周期 plan tree 折叠 |
| 安全解析 | backend/core/security.py | AST 白名单解析器、防注入、多周期 parse_multi_tf |
| API 路由 | backend/api/routes.py | 选股/K线/搜索/状态/健康检查 |
| AI 选股 | frontend/src/lib/selectNL.ts | 公式校验、提示词构建、多周期前缀支持 |
| 主页面 | frontend/src/app/page.tsx | 状态管理、选股/图表联动、本地缓存 |
| K线图表 | frontend/src/components/KLineChart.tsx | lightweight-charts、20+指标、十字光标 |
| 技术指标 | frontend/src/utils/indicators/*.ts | 19 个指标纯前端实现 |
| 数据生产 | stockA/scripts/*.py + tdx_fetcher.go | 全量历史/每日增量生产管线 |

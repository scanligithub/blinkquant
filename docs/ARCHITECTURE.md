# BlinkQuant 系统架构文档

版本: v1.0 | 更新: 2026-08-04

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

---

## 核心模块职责

| 模块 | 文件 | 核心职责 |
|------|------|---------|
| 数据管理 | backend/core/data_manager.py | 流式下载、分片加载、前复权、内存优化、重采样、板块映射 |
| 选股引擎 | backend/core/engine.py | 热 JIT 编译、Lazy 执行、安全板块 Join |
| 安全解析 | backend/core/security.py | AST 白名单解析器，防注入 |
| API 路由 | backend/api/routes.py | 选股/K线/搜索/状态/健康检查 |
| 主页面 | frontend/src/app/page.tsx | 状态管理、选股/图表联动、本地缓存 |
| K线图表 | frontend/src/components/KLineChart.tsx | lightweight-charts、20+指标、十字光标 |
| 技术指标 | frontend/src/utils/indicators/*.ts | 19 个指标纯前端实现 |
| 数据生产 | stockA/scripts/*.py + tdx_fetcher.go | 全量历史/每日增量生产管线 |

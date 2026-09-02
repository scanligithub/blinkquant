# BlinkQuant 开发指南

版本: 1.0.0 | 更新: 2026-08-04

---

## 环境准备

## 后端环境

- Python 3.11+
- 依赖管理: pip install -r requirements.txt
- 核心依赖: fastapi, uvicorn, polars, pyarrow, akshare, redis, psutil
- 开发依赖: pytest, ruff, mypy, pre-commit

## 前端环境

- Node.js 20+ (推荐使用 fnm 或 nvm 管理版本)
- 包管理器: pnpm (已配置 packageManager 字段)
- 安装依赖: pnpm install
- 核心框架: Next.js 14 (App Router), React 18, TypeScript 5
- 图表库: lightweight-charts (K线图), echarts (板块热力图)
- 样式: Tailwind CSS, shadcn/ui

## stockA 数据环境 (可选)

- 用于本地数据生产/回测
- 依赖: akshare, pandas, numpy
- 运行: cd stockA && python -m stockA.data_producer
- 输出: Parquet 文件写入 data/ 目录 (按日期分区)

---

## 常用开发命令

## 后端命令

`ash
# 进入后端目录
cd backend

# 类型检查
mypy .

# 代码格式化
ruff format .
ruff check . --fix

# 运行测试
pytest -v

# 启动开发服务器 (热重载)
uvicorn main:app --reload --port 8000

# 生产模式启动
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
`

## 前端命令

`ash
# 进入前端目录
cd frontend

# 类型检查
pnpm tsc --noEmit

# Lint 检查
pnpm lint

# 代码格式化
pnpm prettier --write .

# 启动开发服务器
pnpm dev

# 构建生产版本
pnpm build

# 预览生产构建
pnpm start
`

## 数据生产测试

`ash
# 进入 stockA 目录
cd stockA

# 单日数据生产测试 (最近 1 个交易日)
python -m stockA.data_producer --days 1

# 全量历史数据生产 (需谨慎，耗时较长)
python -m stockA.data_producer --full

# 验证 Parquet 文件完整性
python -c "import polars as pl; df=pl.read_parquet('data/stock_kline/2024-01-01.parquet'); print(df.shape); print(df.head())"
`

---

## 调试技巧

## 后端调试

### Docker 环境调试

`ash
# 查看容器日志
docker logs -f blinkquant-backend

# 进入容器内部
docker exec -it blinkquant-backend bash

# 容器内手动测试 API
curl "http://localhost:8000/api/v1/kline?code=000001&start=2024-01-01&end=2024-01-10"
`

### 本地调试

- 使用 VS Code launch.json 配置 uvicorn 附加调试
- 断点调试 services/kline_service.py 中的 query_kline 函数
- 关注 Polars LazyFrame 执行计划: df.explain()

## 前端调试

### DevTools 技巧

- Network 面板: 筛选 fetch/XHR 查看 API 请求/响应
- React DevTools: 组件树检查 KlineChart / SectorHeatmap props
- Console: 输入 __NEXT_DATA__ 查看 SSR 初始数据
- Sources: 设置断点调试 hooks/useKlineData.ts 数据获取逻辑

### 常见问题速查表

| 现象 | 可能原因 | 排查步骤 |
|------|----------|----------|
| K线图空白 | API 返回 404/500 | Network 面板看请求参数；后端日志看 Polars 报错 |
| 复权数据不生效 | adjust_type 参数错误 | 检查前端 adjustType 映射：none/forward/backward |
| 板块热力图不显示 | sector_kline 表缺失 | 验证数据生产是否包含板块数据 |
| 搜索无结果 | stock_list 表未加载 | 检查启动日志 Loading stock_list... |
| 类型报错 | TS 版本不匹配 | 确保 pnpm tsc --noEmit 通过 |

## 通用调试

- 后端健康检查: curl http://localhost:8000/health
- 前端构建产物分析: pnpm build && npx @next/bundle-analyzer
- 数据文件大小检查: du -sh data/*/

---

## 代码规范

## Python (后端)

- 格式化: ruff format (遵循 Black 风格，行长 88)
- Lint: ruff check (含 isort 导入排序)
- 类型: mypy --strict (所有新代码需通过)
- 命名: snake_case (函数/变量), PascalCase (类), UPPER_SNAKE_CASE (常量)
- 文档字符串: Google 风格 ("""Summary.

Args:
    x: Description.

Returns:
    Description.""")

## TypeScript (前端)

- 格式化: prettier (单引号, 分号, 行宽 100)
- Lint: eslint (Next.js 推荐规则 + @typescript-eslint)
- 类型: strict: true (tsconfig.json)，禁用 any
- 命名: camelCase (变量/函数), PascalCase (组件/类型), kebab-case (文件名)
- React: 函数组件 + Hooks，避免类组件

## Git 提交规范

- 格式: <type>(<scope>): <subject>
- 类型: feat/fix/docs/refactor/perf/test/chore
- 示例: feat(kline): 添加前复权支持
- Body: 说明动机和对比，必要时关联 Issue

## PR 清单

- [ ] 通过 mypy / tsc --noEmit 类型检查
- [ ] 通过 ruff check / pnpm lint 代码规范
- [ ] 通过 pytest / pnpm test 单元测试
- [ ] 更新相关文档 (docs/ 下对应 .md)
- [ ] 无 console.log / print 残留
- [ ] 变更范围聚焦，单一职责

# BlinkQuant 会话交接说明

> 用途：跨会话保持连续性。新会话开始前先读本文件 + `docs/v2.0-data-migration-plan.md` + `docs/Parquet文件规格说明书.txt`。
> 最后更新：v2.0 全部完成并部署，main @ `b38402e`，tag `v2.0`。

## 1. 项目架构（现状）

- **数据源**：HF Dataset `scanli/stocka-data`，v2.0 起为**年份分片** Parquet 文件：`stock_kline_{YYYY}.parquet`、`stock_basic.parquet`、`adjust_factor.parquet`、`sector_list.parquet`、`sector_constituents_{YYYY}.parquet`、`index_kline_{YYYY}.parquet`、`money_flow_{YYYY}.parquet`、`fundamental_{YYYY}.parquet`、`kline_extend_{YYYY}.parquet`。
- **后端**：FastAPI + Polars，部署在 3 个 HF Space（`scanli-blinkquant-node1/2/3.hf.space`），**每节点 16GB RAM，总节点数 `total_nodes=3`**。
- **前端**：Next.js 14，部署在 Vercel（Vercel 负责构建，本地无 node_modules）。
- **CI/CD**：`.github/workflows/deploy_backend.yml` 仅 push 到 `main` 且路径含 `backend/**` 时触发，推 3 个 Space；前端无 CI 文件。

## 2. v2.0 已完成的改造（不要重复做）

| 文件 | 改动要点 |
|---|---|
| `backend/core/data_manager.py` | 年份分片加载（含新文件分支）；`_normalize_code_expr` 向量化补前缀（6→sh. / 0,3→sz. / 4,8,9→bj.）；`_build_sector_mapping`（行业优先 1-to-1 + 概念兜底 + `unique(subset=["code"],keep="first")`）；`_optimize_memory` 新增 keep_f64 列表（TOTAL_SHARES 等大数字保持 Float64 防溢出）；前复权公式保持不变 |
| `backend/core/security.py` | 白名单新增 PE_TTM / PB_MRQ / FORECAST_YOY / IS_FORECAST_GOOD / IS_FORECAST_BAD / TOTAL_SHARES / FLOAT_SHARES / TOTAL_MV / FLOAT_MV / TURN |
| `backend/api/routes.py` | `/api/v1/kline` 的 `target_cols` 扩展 9 个新字段 |
| `frontend/src/app/page.tsx` | 删除 `formatStockCode()`；搜索评分支持去前缀数字匹配；Enter 数字反查 stockList |
| `frontend/src/utils/pinyin.ts` | 转拼音前剥离 `sh./sz./bj.` 前缀 |

**验证结果**：py_compile 通过；4 组逻辑单测全过（T1 normalize / T2 行业优先 1-to-1 / T3 概念兜底 / T4 sector_list 缺失降级）；前端 `npx -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop` 无语法错误；线上 3 节点选股 + K 线图正常。

## 3. 不可妥协的约束

- **节点分片必须保留**：`pl.col("code").hash() % self.total_nodes == self.node_index`（total_nodes=3，单节点 16GB，任何移除分片/改分片键的行为都是架构回归）。
- **前复权公式保持原样**：`qfq_expr = adj_col / latest_adj`（无需翻转）。
- **板块 Join 必须 1-to-1**：行业优先、概念兜底，防止 1-to-N 行爆炸。
- 货币单位：资金流净额单位为**万元**；股票代码格式 `sh.600000 / sz.000001 / bj.8xxxxx`。

## 4. Git 状态

- main @ `b38402e`（merge PR #1），工作树干净，tag `v1.0` / `v2.0` 已推远端。
- `feature/v2-data-migration` 分支已删（本地+远端）。

## 5. 环境注意事项

- Windows PowerShell：`ls` 不可用，用 `Get-ChildItem`；PowerShell 管道传 UTF-8 中文给 python 会乱码误判，**必须写脚本文件再运行**。
- `gh` CLI 未安装，GitHub 操作用 git 命令 + webfetch/API。
- GitHub 网络间歇 `Connection was reset`，大操作需重试或加大超时（120-180s）。

## 6. 可能的后继工作（未做，需用户确认）

- 搜索逻辑补充：代码前缀归一化后，是否还要处理不带前缀用户输入的其他映射（bj 前缀反查等）。
- 前端本地完整类型检查需 `npm install`（仓库未提交 node_modules）。
- 若后续数据规模增长超单节点内存，需评估更大 Space 或换分片策略（**需重新设计，勿擅自改分片键**）。

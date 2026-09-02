# BlinkQuant 会话交接说明

> 用途：跨会话保持连续性。新会话开始前先读本文件 + `docs/v2.0-data-migration-plan.md` + `docs/Parquet文件规格说明书.txt`。
> 最后更新：v2.1 用户管理功能开发完成（代码已实现，数据库初始化与部署待执行），main @ `b38402e` 前的用户管理改动尚未合并。

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

## 6. 用户管理功能 (v2.1，新增)

### 架构要点
- **认证归属前端**：所有用户数据存 Vercel Postgres（`@vercel/postgres`，env `POSTGRES_URL`），后端 3 节点零改动。
- **会话**：JWT HS256 存 HttpOnly Cookie `__auth_token`（7 天）。`AUTH_SECRET` 为密钥，生产必须配置（缺失则启动抛错）。
- **密码**：bcryptjs (cost 10)。登录统一返回「邮箱或密码错误」防枚举。
- **管理员引导**：`AUTH_ADMIN_EMAIL`/`AUTH_ADMIN_PASSWORD` 环境变量，首次调用登录接口时幂等创建 admin。

### 已完成的文件
| 路径 | 说明 |
|---|---|
| `frontend/src/lib/auth.ts` | 守卫 requireAuth/requireAdmin + JWT 签发校验 + ensureAdmin + Cookie 助手 |
| `frontend/src/lib/db.ts` | `@vercel/postgres` sql/db 封装 |
| `frontend/src/app/api/auth/{register,login,logout,session}/route.ts` | 认证 4 接口 |
| `frontend/src/app/api/watchlist/route.ts` | 自选股 GET/POST/DELETE |
| `frontend/src/app/api/strategies/route.ts` + `[id]/route.ts` | 策略 CRUD（归属校验） |
| `frontend/src/app/api/admin/users/route.ts` + `[id]/route.ts` | 管理员用户管理（requireAdmin） |
| `frontend/src/app/{login,register,admin}/page.tsx` | 登录/注册/管理后台页面 |
| `frontend/src/components/{Watchlist,StrategyList}.tsx` | 自选股/策略组件 |
| `frontend/src/app/page.tsx` | 会话守卫 + 用户菜单 + 加自选 + 保存策略 |
| `frontend/scripts/init_db.sql` | 建表 SQL（users/watchlist/strategies） |
| `frontend/tests/auth.test.mjs` | 11 个 node --test 单测 |

### 待完成（部署步骤）
1. Vercel 环境变量新增：`AUTH_SECRET`、`AUTH_ADMIN_EMAIL`、`AUTH_ADMIN_PASSWORD`（`POSTGRES_URL` 已有）。
2. 在 Vercel Postgres 控制台或 psql 执行一次 `frontend/scripts/init_db.sql`。
3. 推送 main 触发前端部署（后端无需重新部署）。
4. 手动验收：注册→登录→选股→加自选→保存策略→登出→禁用账号登录被拒→admin 管理。
5. （可选）`AUTH_INVITE_CODE`：邀请码列表，逗号分隔。配置后注册必须提交匹配邀请码；留空则不要求。

### 注意事项
- 现有 `select`/`kline`/`search`/`stock-list` 前端 API 已加登录守卫；`status` 与后端 `/api/v1/health` 保持公开。
- 测试命令：`cd frontend && node --test tests/auth.test.mjs`；类型检查 `npx tsc --noEmit`。
- jose `setExpirationTime` 传秒数必须 `Math.floor(Date.now()/1000) + TTL`（jose 按 epoch 秒解释数字），已在实现中正确处理。
- 项目 `strict: false`（strictNullChecks 关闭），AuthResult 设计为始终携带 user/status 字段，避免判别联合收窄失效。
- 邀请码校验逻辑在 `frontend/src/lib/invite.ts`（纯函数，`frontend/tests/invite.test.mjs` 单测）；注册页通过 `GET /api/auth/meta` 按需显示邀请码输入框。
- 注册页在 `GET /api/auth/meta` 返回前禁用提交按钮，避免 meta 竞态导致误报「邀请码无效」。
- 用户数据导出：`GET /api/admin/users/export`（全量 CSV）、`/api/admin/users/:id/export`（单用户 JSON）、`/api/me/export`（本人 JSON）；导出逻辑在 `frontend/src/lib/export.ts`（纯函数，`frontend/tests/export.test.mjs` 单测），所有导出均不含 password_hash。
- 全量 ZIP 导出：`GET /api/admin/users/export-zip` 返回 zip，每用户一个 JSON 文件（基本信息+自选股+策略），用 `fflate` 打包（edge 兼容）。
- 空闲自动登出：前端 `IdleTimeoutProvider` 监听交互事件，30 分钟无操作自动调用 `/api/auth/logout` 并跳转 `/login`；组件在 `frontend/src/components/IdleTimeoutProvider.tsx`，全局生效。

## 7. 可能的后继工作（未做，需用户确认）

- 搜索逻辑补充：代码前缀归一化后，是否还要处理不带前缀用户输入的其他映射（bj 前缀反查等）。
- 前端本地完整类型检查需 `npm install`（仓库未提交 node_modules）。
- 若后续数据规模增长超单节点内存，需评估更大 Space 或换分片策略（**需重新设计，勿擅自改分片键**）。
- 注册限流为内存计数（每实例），生产多实例下建议换集中式限流。
- 用户可自行修改密码/邮箱的功能未实现（本次范围外）。

## 8. 股票板块功能 (v2.2，已实现)

### 架构要点
- **数据**：后端 `data_manager.stock_sectors`（dict：股票代码 → `[(sector_code, sector_name, type)]`），在 `_build_sector_mapping` 中于行业/概念过滤前构建，按 `code+sector_code` 去重，覆盖行业/概念/地域全类型。不影响 1-to-1 `df_mapping`。
- **后端接口**：`GET /api/v1/stock-sectors?code=` 返回 `{code, sectors:[{code,name,type}]}`；`GET /api/v1/sector-kline?code=&timeframe=D` 从 `df_sector_daily/weekly/monthly` 返回 Parquet（`getattr` 兜底未构建分表）。
- **前端**：Edge 代理 `/api/stock-sectors`、`/api/sector-kline`（Promise.any×3 节点）。`viewStock` 选股后懒加载标签；`viewSector` 切换板块 K 线并记录 `lastStockRef` 提供「返回个股」。板块视图隐藏「加自选」「复权」。
- **解析工具**：`frontend/src/utils/parquet.ts` 的 `parseParquetRecords` 统一股票/板块 Parquet 解析（`frontend/tests/parquet.test.mjs` 单测）。

### 注意事项
- 板块类型中文值：行业板块 / 概念板块 / 地域板块（前端 chips 据此配色）。
- 板块 K 线无 `adjustFactor`/`main_net`，`parseParquetRecords` 兜底为 1.0/0，复权不作用于板块。
- 需要后端 3 节点重新部署（推 main 触发）后，`stock_sectors` 才会构建。

## 9. 自然语言选股 + 指标注册表 (v2.3，已实现)

### 架构要点
- **指标注册表 = 单一事实来源**：`backend/core/indicator_registry.py` 统一声明字段白名单（FIELDS）/ 指标（INDICATORS，`window: True`）/ 单位（UNITS）/ 示例（EXAMPLE_QUERIES）。新增指标只需在 `INDICATORS` 加一项，以下三处**自动派生**：
  - `core/security.py` 的 `_visit(ast.Call)` 分支查 `INDICATORS`（配合 `_require_whitelist_field` / `_require_positive_int` 参数形态校验）；Hot-JIT 快路径保留（命中挂载列直接返回列引用，未命中走注册表 `func` 慢路径）
  - `core/data_manager.py` `INDICATOR_MAP = dict(INDICATOR_FUNCS)`（window 型纯函数子集）
  - `core/engine.py` `metric_pattern` 由 `INDICATOR_NAMES` 生成（正则用于 `metrics_stats` 指标统计）；`api/routes.py` 的 `METRIC_REGEX = selection_engine.metric_pattern` 亦指向同一正则
- **后端接口**：`GET /api/v1/nl-meta`（`routes.py`）返回 `{fields, indicators, timeframes, units, example_queries}`，注册表驱动的公开只读元数据。
- **前端 Edge 路由**：`/api/select-nl`（`frontend/src/app/api/select-nl/route.ts`）职责链 = 登录守卫 → 限流 → 拉取 nl-meta（缓存 24h，`Promise.any`×3 节点）→ 调 LLM → JSON 解析（非 JSON 输出直接 400 `INVALID_LLM`，无重试）→ 强校验公式/周期 → 成功才计入配额。**注意：该路由用 Node.js runtime（`maxDuration=60`），不能用 edge**——Edge 要求 25s 内返回首字节，gpt-oss-120b 推理必超。
- **前端弹窗**：`frontend/src/components/AISelectModal.tsx` 提供自然语言输入 + 公式预览（可编辑）+ 周期切换，确认后复用现有选股管道（`page.tsx` 的 `handleSelect(overrides?)`）。
- **纯函数**：`frontend/src/lib/selectNL.ts`（`buildSystemPrompt` / `parseSelectNLText` / `validateFormula` / `checkRateLimit` / `recordRequest`），`frontend/tests/select-nl.test.mjs` 复制实现做单测（仓库惯例）。

### 配置与部署
- **Vercel 新增环境变量（必填）**：`LLM_ENDPOINT` / `LLM_API_KEY` / `LLM_MODEL`；未配置则 AI 选股入口返回 503 `NOT_CONFIGURED`。可选 `LLM_TIMEOUT_MS`（默认 50000，代码钳制上限 55000——Hobby 函数硬限 60s 必须留余量）、`LLM_REASONING_EFFORT`（默认 `low`，NVIDIA gpt-oss reasoning 模型专用）、`LLM_MAX_TOKENS`（默认 1024）。`LLM_ENDPOINT` 需填完整端点（如 `https://integrate.api.nvidia.com/v1/chat/completions`），不是 base URL。
- **限流**：每用户内存计数（Vercel 每实例近似），3 次/分、20 次/天，仅成功翻译才计数，超限返回 429 且携带 `retryAfterMs`。
- **后端需推 main 重新部署**：`backend/**` 变更触发 3 节点重部署后注册表派生才生效；前端部署在 Vercel。

### 已知对齐
- 前端 `NOT` 不在 token 白名单：后端不支持 `ast.Not`，提示词只允许 `AND`/`OR`。
- 公式支持签名允许的嵌套调用（CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))、COUNT(CLOSE > MA(CLOSE,20), 10)），但仅限一层：series 参数可嵌套窗口指标、cond 参数可 AND/OR 组合；更深嵌套（MA(MA(...)) 或 cond 内嵌套 COUNT）前后端均拒绝。
- 窗口上限 500：`MA(CLOSE, 501)` 前后端均拒绝。
- cond 比较式前端要求左侧为 series（比后端更严格）：`5 > CLOSE` 前端拒绝、后端接受；spec 定义 cond 左操作数为 series，属有意收紧，fail-closed。

### 扩展指南
- 新增指标：注册表 `INDICATORS` 加一项 → security / data_manager / engine / nl-meta 自动派生，无需改动其他文件。
- 新增字段：需**同步**注册表 `FIELDS` 与 `security.py` 的 `fields` 键集、`/api/v1/kline` 的 `target_cols`（`test_fields_match_registry` 锁一致性防 drift）。

### 后续工作（final review 记录，v2.3 交付时跳过未实施）
- **软白名单**：`security.py` `_visit(ast.Name)` 对未列入 `FIELDS` 的裸名称会 fallthrough 到 `pl.col(name.lower())`（如 `main_net` 可查询）。函数参数位有 `_require_whitelist_field` 强校验，但裸字段位无。LLM 路径安全（前端拒绝 `_net`），仅手动公式框与无认证直接访问后端时可触达。后续可选：收紧裸名称白名单或文档化内部列可查询。
- **前端测试测复制版**：`select-nl.test.mjs` 复制实现而非导入真实 `selectNL.ts`（仓库既有惯例）。防 drift 可加 ~20 行守卫测试：读 TS 源码断言各函数体出现在测试复制版中。
- **运算符接受集**：前端 `validateFormula` 放行 `= != % ** ^ &&` 而后端拒绝（400 报错信息较模糊）。可前端拒绝或扩展后端 operators 映射。


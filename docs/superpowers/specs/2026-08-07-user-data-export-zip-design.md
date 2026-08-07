# 设计文档：全量用户数据 ZIP 导出（需求变更）

日期：2026-08-07
状态：已认可，待实施

## 背景

「用户数据导出」功能已上线（见 `2026-08-07-user-data-export-design.md`）：管理员可导出单用户 JSON、全量用户 CSV（仅基本信息）、普通用户导出本人 JSON。

新需求：管理员下载的内容需包含**全部用户的股票列表和策略**。经确认，方案为**新增一个全量 JSON 导出**（zip 打包，每用户一个 JSON 文件），现有 CSV 全量导出保留不动。

## 目标

- 新增 `GET /api/admin/users/export-zip`，返回 zip 附件
- zip 内含所有用户各自的 JSON 文件（每用户一份：基本信息 + watchlist + strategies）
- 现有 `/api/admin/users/export`（全量 CSV）保持不变

## 非目标（YAGNI）

- 不做压缩等级配置、加密、zip 内目录结构
- 不做增量导出、批量筛选导出
- 不做每个用户单独下载的交互

## 方案

### 1. 新端点 `GET /api/admin/users/export-zip`

文件：`frontend/src/app/api/admin/users/export-zip/route.ts`（新增，`runtime = 'edge'`）

- `requireAdmin` 鉴权（403→'无权限'，否则→'未登录'）
- **3 次查询**（避免 N 用户 × 2 的 N+1 查询）：
  1. `SELECT id, email, role, status, created_at, last_login_at FROM users ORDER BY created_at DESC`
  2. `SELECT user_id, code, created_at FROM watchlist`
  3. `SELECT user_id, name, formula, timeframe, created_at, updated_at FROM strategies`
- 服务端将 watchlist / strategies 按 `user_id` 分组为 `Map<string, row[]>`
- 每个用户调用 `buildUserExport(user, watchlist, strategies)` 组装（复用现有纯函数，白名单字段，不含 password_hash）
- `fflate.zipSync()` 打包为 `Record<filename, Uint8Array>`，zip 内文件名 `<email>_<yyyy-mm-dd>.json`（经 `sanitizeFilename` 清洗）
- 返回 `Content-Type: application/zip`，`Content-Disposition: attachment; filename="users_export_<yyyy-mm-dd>.zip"`
- 无用户时返回空 zip（合法）；异常 → 500 '导出失败'

### 2. 依赖

- 新增 `fflate`（纯 JS 零依赖，edge 兼容）：`npm install fflate`

### 3. 前端入口

文件：`frontend/src/app/admin/page.tsx`

- 顶部工具条新增「导出全量数据」按钮 → `downloadFromResponse('/api/admin/users/export-zip')`
- `frontend/src/lib/download.ts` 无需改动（zip 同为 blob 附件，走同一套下载逻辑；Content-Disposition 的 `filename=` 解析已支持）

### 4. 测试

文件：`frontend/tests/export.test.mjs`（追加）

- 抽纯函数 `buildUserExports(users, watchlistByUser, strategiesByUser): Array<{ filename, content }>` 置于 `lib/export.ts`
  - 验证按 user_id 分组正确、每用户输出含 user/watchlist/strategies、**不含 password_hash**
- zip 组装验证：用 `fflate` 的 `unzipSync` 解包，验证文件数与文件名、内容一致

## 数据流

```
[admin] 点「导出全量数据」→ GET /api/admin/users/export-zip
  → requireAdmin
  → 3 次查询（users / watchlist / strategies）
  → 按 user_id 分组 → 逐用户 buildUserExport
  → fflate.zipSync 打包 → application/zip 附件返回
  → downloadFromResponse 保存 users_export_<date>.zip
```

## 错误处理

- 鉴权：沿用 requireAdmin 的 401/403
- 无用户：返回空 zip（合法文件）
- 查询/打包异常：500 '导出失败'
- 前端下载失败：沿用 download.ts 的 alert 处理

## 安全

- 复用 `buildUserExport` 白名单，**不含 password_hash**
- zip 内文件名经 `sanitizeFilename` 清洗（非字母数字→`_`），防 zip-slip 与路径注入
- `requireAdmin` 限定管理员访问
- 参数化查询，无 SQL 注入

## 验证

- `npm install fflate` 后 `npx tsc --noEmit` 无错误
- `node --test tests/export.test.mjs` 全部通过
- `next build` 编译成功
- 手动验证：admin 下载 zip，解压后每用户一个 JSON，Excel/编辑器打开内容正确且无 password_hash

## 相关文件

- 新增：`frontend/src/app/api/admin/users/export-zip/route.ts`
- 修改：`frontend/src/lib/export.ts`（新增 `buildUserExports` 纯函数）、`frontend/tests/export.test.mjs`、`frontend/src/app/admin/page.tsx`、`frontend/package.json`、`docs/API_CONTRACT.md`、`docs/CONTEXT.md`

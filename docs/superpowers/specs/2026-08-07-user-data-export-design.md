# 设计文档：用户数据导出

日期：2026-08-07
状态：已认可，待实施

## 背景

系统已有 admin 用户管理（列表/禁用/删除），删除为**物理删除**（users 记录 DELETE，watchlist/strategies 外键级联清空）。用户数据一旦删除不可恢复。为满足数据留档与用户自助需求，新增用户数据导出能力。

## 目标

- 管理员删除某用户前可下载该用户完整数据（JSON）留档
- 管理员可一键导出全量用户列表（CSV，Excel 可打开）
- 普通用户可下载自己的数据（JSON）
- 所有导出**绝不包含** `password_hash`

## 非目标（YAGNI）

- 不做 zip 打包、邮件发送、批量多用户导出、导出历史记录
- 不做导入功能
- 不做导出格式选择 UI（每场景格式固定）

## 方案

采用「服务端生成文件，前端 fetch 后触发下载」：各导出端点直接返回附件响应（`Content-Type` + `Content-Disposition`），CSV 生成逻辑封装为纯函数置于 `lib/`，可单测；鉴权/组装/转义统一在服务端一处把关。

### 1. API 端点（3 个，均 `runtime = 'edge'`）

| 方法 | 路径 | 鉴权 | 返回 |
|------|------|------|------|
| GET | `/api/admin/users/[id]/export` | requireAdmin | 单用户 JSON 附件 |
| GET | `/api/admin/users/export` | requireAdmin | 全量用户 CSV 附件 |
| GET | `/api/me/export` | requireAuth | 当前用户 JSON 附件 |

路由说明：`/api/admin/users/export` 为静态段，Next.js 优先于 `[id]` 动态段，与现有 `[id]/route.ts` 不冲突。

响应头：
- `Content-Type: application/json` 或 `text/csv; charset=utf-8`
- `Content-Disposition: attachment; filename=...`
  - 单用户：`{email}_{yyyy-mm-dd}.json`（`@`/`.`/空格等替换为 `_`）
  - 全量：`users_{yyyy-mm-dd}.csv`

### 2. 单用户 JSON 结构

```json
{
  "exported_at": "2026-08-07T12:00:00.000Z",
  "user": { "id": "...", "email": "...", "role": "user", "status": "active",
            "created_at": "...", "last_login_at": null },
  "watchlist": [ { "code": "sh.600000", "created_at": "..." } ],
  "strategies": [ { "name": "...", "formula": "...", "timeframe": "D",
                    "created_at": "...", "updated_at": "..." } ]
}
```

`/api/me/export` 自动取当前登录用户，不带 `[id]`。

### 3. 全量 CSV 列

`id, email, role, status, created_at, last_login_at`

UTF-8 + BOM（`\uFEFF` 前缀），Excel 直接打开中文不乱码。

### 4. 共用工具（`frontend/src/lib/export.ts`，纯函数，新增）

```ts
// CSV 生成：逗号/引号/换行转义 + UTF-8 BOM 前缀
toCSV(headers: string[], rows: (string | number | null)[][]): string

// 单用户导出 JSON 组装：过滤字段，绝不带 password_hash
buildUserExport(user, watchlist, strategies): object

// 从 Content-Disposition 解析文件名（含中文/空格，处理 RFC 5987）
parseExportFilename(header: string | null, fallback: string): string
```

### 5. 前端交互

**管理后台 `frontend/src/app/admin/page.tsx`**
- 每行「操作」列加「导出」按钮 → `GET /api/admin/users/[id]/export`
- 删除弹窗文案改为：`确定删除用户 {email}？该用户的自选股和策略将一并删除。如需备份请先导出。`
- 顶部（搜索框旁）加「导出用户列表」按钮 → `GET /api/admin/users/export`

**首页用户菜单 `frontend/src/app/page.tsx`**
- 「我的策略」下方加「导出我的数据」→ `GET /api/me/export`

**下载助手（`frontend/src/lib/download.ts`，新增）**
```ts
downloadFromResponse(url: string): Promise<void>
```
- `fetch(url, { cache: 'no-store' })`
- 401 → 跳 `/login`；403 → `alert('无权限')`；!ok → `alert('导出失败，请重试')`
- 读 blob，从 `Content-Disposition` 解析文件名，创建 objectURL，`<a download>` 点击后 revokeObjectURL

### 6. 测试（`frontend/tests/export.test.mjs`，沿用 `node --test` + 内嵌实现副本约定）

- `toCSV`：基本行 / 逗号转义（双引号包裹 + 内部引号翻倍）/ 含引号字段 / 换行字段 / BOM 前缀
- `buildUserExport`：输出含 `exported_at`/`user`/`watchlist`/`strategies`；**不含 password_hash**
- `parseExportFilename`：普通 / 中文 / 含空格 / RFC 5987 `filename*=UTF-8''` / 缺失回退

## 数据流

```
[admin] 点「导出」→ GET /api/admin/users/[id]/export
[admin] 点「导出用户列表」→ GET /api/admin/users/export
[user] 点「导出我的数据」→ GET /api/me/export
  → requireAdmin/requireAuth 鉴权
  → 查询（users + watchlist + strategies，按权限确定 userId）
  → 组装（buildUserExport）或生成 CSV（toCSV）
  → 返回附件响应（Content-Type + Content-Disposition）
  → 前端 downloadFromResponse 保存文件
```

## 错误处理

- 单用户导出目标用户不存在：404 `{ error: '用户不存在' }`
- 未登录/无权限：沿用 `requireAuth`/`requireAdmin` 的 401/403 语义
- 查询异常：500 `{ error: '导出失败' }`
- 前端下载失败：`alert('导出失败，请重试')`

## 安全

- 所有导出端点均不含 `password_hash`；`buildUserExport` 为唯一组装出口
- 单用户导出限 admin 或本人（userId 恒等校验）
- 文件名防注入：非字母数字字符替换为 `_`
- edge runtime 无文件系统，全部纯字符串拼接 + `new Response(body)`

## 验证

- `npx tsc --noEmit` 无错误
- `node --test tests/export.test.mjs` 全部通过
- `next build` 编译成功
- 手动验证：admin 导出单用户 JSON 可下载且无密码字段；全量 CSV 用 Excel 打开中文正常；普通用户导出本人数据；403/401 权限正确

## 相关文件

- 新增：`frontend/src/lib/export.ts`、`frontend/src/lib/download.ts`、`frontend/tests/export.test.mjs`
- 新增接口：`frontend/src/app/api/admin/users/[id]/export/route.ts`、`frontend/src/app/api/admin/users/export/route.ts`、`frontend/src/app/api/me/export/route.ts`
- 修改：`frontend/src/app/admin/page.tsx`、`frontend/src/app/page.tsx`、`docs/API_CONTRACT.md`、`docs/CONTEXT.md`

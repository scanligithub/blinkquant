# BlinkQuant API Contract

版本: v2.1 | 更新: 2026-08-07

---

## 认证说明

自 v2.1 起，除 `/api/status` 与 `/api/v1/health` 外，前端 API 全部需要登录（HttpOnly Cookie `__auth_token`，JWT HS256，有效期 7 天）。未登录返回 401。

## 前端 API 路由 (Next.js, Vercel Postgres)

| 方法 | 路径 | 说明 | 权限 |
|------|------|------|------|
| POST | /api/auth/register | 注册（邮箱+密码，密码≥8位；AUTH_INVITE_CODE 启用时需 inviteCode） | 公开 |
| GET | /api/auth/meta | 注册元信息 { requireInvite } | 公开 |
| POST | /api/auth/login | 登录，签发 JWT Cookie | 公开 |
| POST | /api/auth/logout | 退出，清除 Cookie | 登录 |
| GET | /api/auth/session | 当前用户 { user } \| null | 公开 |
| GET | /api/watchlist | 自选股列表 | 登录 |
| POST | /api/watchlist | 添加自选股 { code } | 登录 |
| DELETE | /api/watchlist?code=xxx | 删除自选股 | 登录 |
| GET | /api/strategies | 我的策略列表 | 登录 |
| POST | /api/strategies | 保存策略 { name, formula, timeframe } | 登录 |
| PUT | /api/strategies/:id | 更新策略 | 登录（归属校验） |
| DELETE | /api/strategies/:id | 删除策略 | 登录（归属校验） |
| GET | /api/admin/users | 用户列表（keyword/status/page 分页） | admin |
| PATCH | /api/admin/users/:id | 修改角色/状态 { role?, status? } | admin |
| DELETE | /api/admin/users/:id | 删除用户（级联删自选股/策略） | admin |
| GET | /api/admin/users/export | 导出全量用户 CSV（附件） | admin |
| GET | /api/admin/users/export-zip | 导出全部用户 JSON（zip，每用户一文件含自选股/策略） | 管理员 |
| GET | /api/admin/users/:id/export | 导出单用户 JSON（附件，含自选股/策略） | admin |
| GET | /api/me/export | 导出当前用户 JSON（附件） | 登录 |

## 后端节点 API (HF Spaces, 经前端代理)

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/v1/select | Execute stock selection formula |
| GET | /api/v1/kline | Get K-line data (Parquet binary) |
| GET | /api/v1/search | Search stocks |
| GET | /api/v1/stock-list | Get all stocks |
| GET | /api/v1/status | Node health |
| GET | /api/v1/health | Health probe |
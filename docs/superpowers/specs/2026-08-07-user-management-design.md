# BlinkQuant 用户管理功能设计文档

版本: v1.0 | 日期: 2026-08-07

---

## 1. 目标

为 BlinkQuant 增加完整用户体系：认证 + 个人数据（自选股/选股策略）+ 管理员管理。

## 2. 架构决策

- **认证归属**：全部在前端 Next.js API 层（`src/app/api/*`）。后端 3 个 HF Space 节点保持无状态，零改动。
- **存储**：Vercel Postgres（`@vercel/postgres`，env `POSTGRES_URL`），与后端 metrics_stats 同库。
- **会话**：JWT (HS256) 存 HttpOnly Cookie（`httpOnly + secure + sameSite=lax + path=/`），有效期 7 天，载荷 `{ sub, email, role }`。
- **技术选型**（均兼容 Edge Runtime）：`jose`（JWT）、`bcryptjs`（密码哈希）、`@vercel/postgres`（已装）。

## 3. 数据模型

```sql
users(id uuid PK, email text UNIQUE, password_hash text,
      role text DEFAULT 'user', status text DEFAULT 'active',
      created_at timestamptz, last_login_at timestamptz)

watchlist(id serial PK, user_id uuid FK, code text,
          created_at timestamptz, UNIQUE(user_id, code))

strategies(id serial PK, user_id uuid FK, name text,
           formula text, timeframe text DEFAULT 'D',
           created_at timestamptz, updated_at timestamptz)
```

## 4. 管理员引导

`AUTH_ADMIN_EMAIL` / `AUTH_ADMIN_PASSWORD` 环境变量。认证模块首次调用时幂等执行 `ensureAdmin()`：若该邮箱不存在则以 admin 角色创建。

## 5. API 接口

### 认证
```
POST /api/auth/register   { email, password }          → 201 或 409（邮箱已存在）
POST /api/auth/login      { email, password }          → 设置 Cookie；统一错误防枚举
POST /api/auth/logout                                   → 清除 Cookie
GET  /api/auth/session                                   → { user } | null
```

### 自选股（需登录）
```
GET    /api/watchlist                    → code 列表
POST   /api/watchlist  { code }          → UPSERT 添加
DELETE /api/watchlist  ?code=xxx         → 删除
```

### 策略（需登录，校验归属 user_id）
```
GET    /api/strategies
POST   /api/strategies { name, formula, timeframe }
PUT    /api/strategies/:id { name?, formula?, timeframe? }
DELETE /api/strategies/:id
```

### 管理员（需 admin）
```
GET    /api/admin/users?keyword=&status= → 分页+搜索
PATCH  /api/admin/users/:id { role?, status? }
DELETE /api/admin/users/:id              → 级联删除
```

### 受保护改造
`select` / `kline` / `search` / `stock-list` 四个 route 开头调用 `requireAuth`。`status` 保持公开（登录页显示集群健康）。

## 6. 前端结构

- 新页面：`login`、`register`、`admin`
- 新组件：`Watchlist.tsx`、`StrategyList.tsx`
- `page.tsx`：挂载时读 session，未登录跳 /login；Header 加用户菜单（email/角色徽标/自选股/策略/管理后台/退出）
- 新库：`src/lib/auth.ts`（守卫）、`src/lib/db.ts`（连接封装）

## 7. 安全与错误处理

- 密码 bcryptjs cost 10；登录统一错误防枚举
- 资源归属校验，跨用户 403
- SQL 全参数化
- `AUTH_SECRET` 环境变量；生产缺失拒绝启动，开发回退随机密钥并告警
- Cookie httpOnly/secure/sameSite=lax
- 邮箱格式 + 密码长度（≥8）校验；注册限流（每 IP 每 5 分钟 5 次，内存计数）
- 统一错误格式 `{ error }`：400/401/403/404/409/500
- 实现注意：项目 `strict: false`（strictNullChecks 关闭）导致判别联合收窄失效，`AuthResult` 统一携带 `user`/`status` 字段（`{ user: SessionUser|null, status }`）而非判别联合

## 8. 测试

- `node --test`：`auth.test.mjs`（JWT 签发/校验/过期、密码哈希、requireAuth 401）
- 后端 `py_compile` 回归
- 手动验收清单：注册→登录→选股→加自选→保存策略→登出→禁用账号被拒→admin 管理

## 9. 部署

- Vercel 环境变量：`AUTH_SECRET`、`AUTH_ADMIN_EMAIL`、`AUTH_ADMIN_PASSWORD`（`POSTGRES_URL` 已有）
- 执行 `frontend/scripts/init_db.sql` 一次（Vercel Postgres 控制台或 psql）
- 前端 push main 自动部署

## 10. 文件清单

新增：`src/lib/auth.ts`、`src/lib/db.ts`、`src/app/api/auth/{register,login,logout,session}/route.ts`、`src/app/api/watchlist/route.ts`、`src/app/api/strategies/route.ts`、`src/app/api/admin/users/route.ts`、`src/app/login/page.tsx`、`src/app/register/page.tsx`、`src/app/admin/page.tsx`、`src/components/Watchlist.tsx`、`src/components/StrategyList.tsx`、`frontend/scripts/init_db.sql`

修改：`package.json`（+jose +bcryptjs）、`src/app/page.tsx`、`src/app/api/{select,kline,search,stock-list}/route.ts`

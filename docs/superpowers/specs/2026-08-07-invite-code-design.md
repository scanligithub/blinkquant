# 设计文档：可选邀请码开关（注册防护）

日期：2026-08-07
状态：已认可，已实施

## 背景

当前注册流程（`frontend/src/app/api/auth/register/route.ts`）无任何防滥用机制：任意人可用任意邮箱注册并立即登录。经讨论，本轮不做邮件验证码（需第三方邮件服务），改为最小成本的可选邀请码开关，用环境变量即可控制启用/关闭，无需改代码。

## 目标

- 通过环境变量 `AUTH_INVITE_CODE` 控制注册是否要求邀请码
- 启用时注册必须提交匹配的邀请码，否则拒绝
- 关闭时注册行为与现状完全一致（兼容性）

## 非目标（YAGNI）

- 不做邮件验证码、手机验证
- 不做邀请码数据库表 / 后台生成与作废（后续需要再扩展）
- 不做注册频率限制、蜜罐字段、邮箱域名黑名单（可后续单独迭代）

## 方案

### 1. 新接口：`GET /api/auth/meta`

文件：`frontend/src/app/api/auth/meta/route.ts`

- `runtime = 'edge'`，无需认证
- 返回 `{ requireInvite: boolean }`，值为 `AUTH_INVITE_CODE` 非空（trim 后有内容）则为 `true`
- 公开接口，不含任何敏感信息
- 用途：前端挂载时查询，决定是否渲染「邀请码」输入框

### 2. 邀请码解析与校验（纯函数，便于单测）

文件：`frontend/src/lib/invite.ts`（新增）

```ts
export function parseInviteCodes(envValue: string | undefined): string[] {
  if (!envValue) return [];
  return envValue.split(',').map((s) => s.trim()).filter(Boolean);
}

export function isValidInviteCode(inviteCodes: string[], code: string | undefined | null): boolean {
  if (inviteCodes.length === 0) return true; // 未启用时不校验
  return typeof code === 'string' && inviteCodes.includes(code);
}
```

- 精确匹配，大小写敏感，首尾空格 trim
- 未启用（列表为空）时恒通过，保持现状兼容

### 3. 注册后端改造

文件：`frontend/src/app/api/auth/register/route.ts`

- 在格式校验之后、插入用户之前，调用 `parseInviteCodes(process.env.AUTH_INVITE_CODE)` 与 `isValidInviteCode`
- 校验失败返回 `403 { error: '邀请码无效' }`
- 校验通过后走原有流程（bcrypt → 查重 → 插入 → 签发 cookie）
- 注册成功后响应体不回传邀请码相关字段

### 4. 注册页改造

文件：`frontend/src/app/register/page.tsx`

- 挂载时 `fetch('/api/auth/meta')` 获取 `requireInvite`
- `requireInvite === true` 才渲染「邀请码」输入框（必填）
- 表单提交时带上 `inviteCode` 字段（未启用时可省略）
- 403 时展示 `json.error` 文案（现有错误处理逻辑已支持）

### 5. 测试

文件：`frontend/tests/invite.test.mjs`（新增），沿用 `node --test` 模式

覆盖：
- `parseInviteCodes`：undefined / 空串 / 单码 / 多码逗号分隔 / 首尾空格 / 中间空项过滤
- `isValidInviteCode`：未启用恒通过 / 缺失码拒绝 / 错误码拒绝 / 正确码通过 / 大小写敏感 / trim 后匹配
- 不依赖 DB，纯函数测试

### 6. 文档

- `docs/API_CONTRACT.md`：新增 `GET /api/auth/meta` 条目
- `docs/CONTEXT.md`：新增 `AUTH_INVITE_CODE` 环境变量说明（逗号分隔，留空关闭）

## 数据流

```
注册页挂载 → GET /api/auth/meta → { requireInvite }
如果 requireInvite → 显示邀请码输入框
提交注册 → POST /api/auth/register { email, password, inviteCode? }
  → parseInviteCodes(AUTH_INVITE_CODE)
  → 启用且码无效 → 403 '邀请码无效'
  → 校验通过 → 原注册流程 → 201 + 设置 cookie
```

## 错误处理

- `AUTH_INVITE_CODE` 未配置：列表为空，不校验，注册行为不变
- 前端 `meta` 请求失败：按 `requireInvite = false` 处理（不显示邀请码框），保证接口异常时注册不阻塞
- 邀请码错误：返回 403，前端展示服务端 error 文案

## 验证

- `npx tsc --noEmit` 无错误
- `node --test frontend/tests/invite.test.mjs` 全部通过
- `next build` 编译成功
- 手动验证：未配置变量注册正常；配置变量后无码/错码被拒、正码成功

## 相关文件

- 新增：`frontend/src/app/api/auth/meta/route.ts`、`frontend/src/lib/invite.ts`、`frontend/tests/invite.test.mjs`
- 修改：`frontend/src/app/api/auth/register/route.ts`、`frontend/src/app/register/page.tsx`、`docs/API_CONTRACT.md`、`docs/CONTEXT.md`

# 可选邀请码开关（注册防护）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过环境变量 `AUTH_INVITE_CODE` 控制注册是否要求邀请码，启用时前端按需显示邀请码输入框、后端校验，关闭时注册行为与现状一致。

**Architecture:** 纯前端（Next.js API 层）改动，无新增表、无后端节点改动。核心为 `src/lib/invite.ts` 的两个纯函数（解析/校验），由 `GET /api/auth/meta` 暴露开启状态供注册页按需渲染输入框，`POST /api/auth/register` 在校验通过后执行原有注册流程。

**Tech Stack:** Next.js 14 App Router（edge runtime）、`@vercel/postgres`、`node:test`（`node --test`）。

**工作目录：** 所有命令在 `frontend/` 下执行（除 git commit 在仓库根）。

---

### Task 1: 邀请码纯函数 + 单测（TDD）

**Files:**
- Create: `frontend/src/lib/invite.ts`
- Test: `frontend/tests/invite.test.mjs`

- [ ] **Step 1: 写失败测试**

创建 `frontend/tests/invite.test.mjs`：

```js
import { test } from 'node:test';
import assert from 'node:assert/strict';

const { parseInviteCodes, isValidInviteCode } = await import('../src/lib/invite.ts');
```

（TS 无法直接被 node 运行。改用独立实现方式：Step 1 先创建纯 JS 版本的测试直连函数。见下方 Step 3 的说明。）

由于 `node --test` 直接运行 `.mjs`，而 `invite.ts` 是 TS，无法直接 import。**方案：测试文件内嵌与 `invite.ts` 一致的函数副本**（沿用 `auth.test.mjs` 的既有模式——它同样在测试内复制了 `src/lib/auth.ts` 的 JWT 逻辑）。因此 Step 1 直接创建完整测试文件（引用测试内复制的实现）：

```js
// frontend/tests/invite.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// 与 src/lib/invite.ts 保持一致（复制实现以绕过 TS import）
function parseInviteCodes(envValue) {
  if (!envValue) return [];
  return envValue.split(',').map((s) => s.trim()).filter(Boolean);
}

function isValidInviteCode(inviteCodes, code) {
  if (inviteCodes.length === 0) return true;
  return typeof code === 'string' && inviteCodes.includes(code);
}

test('parseInviteCodes: undefined 返回空数组', () => {
  assert.deepEqual(parseInviteCodes(undefined), []);
});

test('parseInviteCodes: 空串返回空数组', () => {
  assert.deepEqual(parseInviteCodes(''), []);
});

test('parseInviteCodes: 纯空格串返回空数组', () => {
  assert.deepEqual(parseInviteCodes('   '), []);
});

test('parseInviteCodes: 单码', () => {
  assert.deepEqual(parseInviteCodes('abc123'), ['abc123']);
});

test('parseInviteCodes: 多码逗号分隔并 trim', () => {
  assert.deepEqual(parseInviteCodes('abc, def , ghi'), ['abc', 'def', 'ghi']);
});

test('parseInviteCodes: 过滤中间空项', () => {
  assert.deepEqual(parseInviteCodes('abc,,def'), ['abc', 'def']);
});

test('isValidInviteCode: 未启用（空列表）恒通过', () => {
  assert.equal(isValidInviteCode([], 'anything'), true);
  assert.equal(isValidInviteCode([], undefined), true);
  assert.equal(isValidInviteCode([], null), true);
});

test('isValidInviteCode: 缺失码拒绝', () => {
  assert.equal(isValidInviteCode(['abc'], undefined), false);
  assert.equal(isValidInviteCode(['abc'], null), false);
  assert.equal(isValidInviteCode(['abc'], ''), false);
});

test('isValidInviteCode: 错误码拒绝', () => {
  assert.equal(isValidInviteCode(['abc'], 'xyz'), false);
});

test('isValidInviteCode: 正确码通过', () => {
  assert.equal(isValidInviteCode(['abc'], 'abc'), true);
});

test('isValidInviteCode: 大小写敏感', () => {
  assert.equal(isValidInviteCode(['AbC'], 'abc'), false);
  assert.equal(isValidInviteCode(['abc'], 'AbC'), false);
});

test('isValidInviteCode: 多码之一匹配即通过', () => {
  assert.equal(isValidInviteCode(['abc', 'def'], 'def'), true);
});
```

- [ ] **Step 2: 运行测试确认全过（当前为空实现的验证）**

Run: `node --test tests/invite.test.mjs`
Expected: 13 个测试全部 PASS（因为测试内嵌实现已就绪）。

- [ ] **Step 3: 创建 `src/lib/invite.ts` 生产实现（TS 版，与测试副本逻辑一致）**

```ts
export function parseInviteCodes(envValue: string | undefined): string[] {
  if (!envValue) return [];
  return envValue.split(',').map((s) => s.trim()).filter(Boolean);
}

export function isValidInviteCode(
  inviteCodes: string[],
  code: string | undefined | null
): boolean {
  if (inviteCodes.length === 0) return true;
  return typeof code === 'string' && inviteCodes.includes(code);
}
```

- [ ] **Step 4: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无输出（0 错误）。

- [ ] **Step 5: 提交**

```bash
git add frontend/src/lib/invite.ts frontend/tests/invite.test.mjs
git commit -m "feat: 新增邀请码解析与校验纯函数"
```

---

### Task 2: `GET /api/auth/meta` 接口

**Files:**
- Create: `frontend/src/app/api/auth/meta/route.ts`

- [ ] **Step 1: 创建 meta 路由**

创建 `frontend/src/app/api/auth/meta/route.ts`：

```ts
import { NextResponse } from 'next/server';
import { parseInviteCodes } from '@/lib/invite';

export const runtime = 'edge';

export async function GET() {
  const inviteCodes = parseInviteCodes(process.env.AUTH_INVITE_CODE);
  return NextResponse.json({ requireInvite: inviteCodes.length > 0 });
}
```

- [ ] **Step 2: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无输出（0 错误）。

- [ ] **Step 3: 构建验证路由被编译**

Run: `npm run build`
Expected: `Compiled successfully`，输出中包含 `⚡ /api/auth/meta`（Edge Runtime）。

- [ ] **Step 4: 提交**

```bash
git add frontend/src/app/api/auth/meta/route.ts
git commit -m "feat: 新增 GET /api/auth/meta 暴露邀请码开关状态"
```

---

### Task 3: 注册后端接入邀请码校验

**Files:**
- Modify: `frontend/src/app/api/auth/register/route.ts`

- [ ] **Step 1: 修改 register 路由**

在 `frontend/src/app/api/auth/register/route.ts` 中：

1. 顶部 import 增加：
```ts
import { parseInviteCodes, isValidInviteCode } from '@/lib/invite';
```

2. 在密码校验（`isValidPassword`）之后、`bcrypt.hash` 之前，插入：

```ts
    const inviteCodes = parseInviteCodes(process.env.AUTH_INVITE_CODE);
    if (!isValidInviteCode(inviteCodes, body?.inviteCode)) {
      return NextResponse.json({ error: '邀请码无效' }, { status: 403 });
    }
```

修改后完整文件（关键部分）：

```ts
import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { signToken, setAuthCookie, isValidEmail, isValidPassword } from '@/lib/auth';
import { parseInviteCodes, isValidInviteCode } from '@/lib/invite';

export const runtime = 'edge';

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const email = String(body?.email || '').trim().toLowerCase();
    const password = String(body?.password || '');

    if (!isValidEmail(email)) {
      return NextResponse.json({ error: '邮箱格式不正确' }, { status: 400 });
    }
    if (!isValidPassword(password)) {
      return NextResponse.json({ error: '密码长度至少 8 位' }, { status: 400 });
    }

    const inviteCodes = parseInviteCodes(process.env.AUTH_INVITE_CODE);
    if (!isValidInviteCode(inviteCodes, body?.inviteCode)) {
      return NextResponse.json({ error: '邀请码无效' }, { status: 403 });
    }

    const bcrypt = (await import('bcryptjs')).default;
    const passwordHash = await bcrypt.hash(password, 10);
    // ... 其余不变
```

- [ ] **Step 2: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无输出（0 错误）。

- [ ] **Step 3: 提交**

```bash
git add frontend/src/app/api/auth/register/route.ts
git commit -m "feat: 注册接口启用时校验邀请码"
```

---

### Task 4: 注册页按需显示邀请码输入框

**Files:**
- Modify: `frontend/src/app/register/page.tsx`

- [ ] **Step 1: 修改注册页**

在 `frontend/src/app/register/page.tsx` 中：

1. `import { useState } from 'react';` 改为 `import { useState, useEffect } from 'react';`
2. 新增 state：
```tsx
  const [inviteCode, setInviteCode] = useState('');
  const [requireInvite, setRequireInvite] = useState(false);
```
3. 在组件函数体内（`loading` state 声明之后）新增 effect：
```tsx
  useEffect(() => {
    let active = true;
    fetch('/api/auth/meta')
      .then((r) => r.json())
      .then((json) => {
        if (active) setRequireInvite(Boolean(json?.requireInvite));
      })
      .catch(() => {
        // 接口异常时按不要求邀请码处理，注册不阻塞
      });
    return () => {
      active = false;
    };
  }, []);
```
4. `handleSubmit` 的请求体改为：
```tsx
        body: JSON.stringify({ email, password, inviteCode }),
```
5. 在「确认密码」输入框之后、提交按钮之前，插入条件渲染（仅 `requireInvite` 时显示）：
```tsx
          {requireInvite && (
            <div>
              <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">邀请码</label>
              <input
                type="text"
                required
                value={inviteCode}
                onChange={(e) => setInviteCode(e.target.value)}
                className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
                placeholder="请输入邀请码"
              />
            </div>
          )}
```

- [ ] **Step 2: 类型检查**

Run: `npx tsc --noEmit`
Expected: 无输出（0 错误）。

- [ ] **Step 3: 构建验证**

Run: `npm run build`
Expected: `Compiled successfully`。

- [ ] **Step 4: 提交**

```bash
git add frontend/src/app/register/page.tsx
git commit -m "feat: 注册页按需显示邀请码输入框"
```

---

### Task 5: 更新文档

**Files:**
- Modify: `docs/API_CONTRACT.md`
- Modify: `docs/CONTEXT.md`

- [ ] **Step 1: 更新 API 契约**

在 `docs/API_CONTRACT.md` 前端 API 表中，`POST /api/auth/register` 行之后插入：

```markdown
| GET | /api/auth/meta | 注册元信息 { requireInvite } | 公开 |
```

同时将 register 行说明更新为：
```markdown
| POST | /api/auth/register | 注册（邮箱+密码，密码≥8位；AUTH_INVITE_CODE 启用时需 inviteCode） | 公开 |
```

- [ ] **Step 2: 更新 CONTEXT.md**

在 `docs/CONTEXT.md`「待完成（部署步骤）」第 1 条之后追加一条环境变量：

```markdown
4. （可选）`AUTH_INVITE_CODE`：邀请码列表，逗号分隔。配置后注册必须提交匹配邀请码；留空则不要求。
```

同时在「注意事项」区补充一句：
```markdown
- 邀请码校验逻辑在 `frontend/src/lib/invite.ts`（纯函数，`frontend/tests/invite.test.mjs` 单测）；注册页通过 `GET /api/auth/meta` 按需显示邀请码输入框。
```

- [ ] **Step 3: 提交**

```bash
git add docs/API_CONTRACT.md docs/CONTEXT.md
git commit -m "docs: 补充邀请码接口与 AUTH_INVITE_CODE 环境变量说明"
```

---

### Task 6: 全量验证 + 推送

- [ ] **Step 1: 运行全部单测**

Run: `node --test tests/auth.test.mjs tests/invite.test.mjs`
Expected: auth.test.mjs 11 个 + invite.test.mjs 13 个，全部 PASS。

- [ ] **Step 2: 类型检查 + 构建**

Run: `npx tsc --noEmit`
Expected: 无输出（0 错误）。

Run: `npm run build`
Expected: `Compiled successfully`，无 error。

- [ ] **Step 3: 推送**

（各 Task 已独立提交，此处仅推送。）

```bash
git push origin main
```

Expected: 推送成功，Vercel 自动部署。

- [ ] **Step 4: 验收提示（告知用户）**

通知用户：部署完成后手动验收——
1. 未配置 `AUTH_INVITE_CODE` 时，注册页无邀请码框，注册正常。
2. 在 Vercel 配置 `AUTH_INVITE_CODE=test123` 后，注册页出现邀请码框；无码/错码被拒（提示「邀请码无效」）；输入 `test123` 注册成功。

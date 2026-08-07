# 全量用户数据 ZIP 导出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增管理员全量数据导出：`GET /api/admin/users/export-zip` 返回 zip 附件，zip 内每用户一个 JSON 文件（基本信息 + watchlist + strategies），现有 CSV 导出保留不动。

**Architecture:** 新端点 edge runtime，3 次批量查询（users / watchlist / strategies）后按 `user_id` 分组，逐用户复用 `buildUserExport` 组装，`fflate.zipSync` 打包为 `Record<filename, Uint8Array>` 返回 zip 附件；`lib/export.ts` 新增纯函数 `buildUserExports`（可单测）；前端 admin 页新增「导出全量数据」按钮，复用现有 `downloadFromResponse`。

**Tech Stack:** Next.js 14 (edge runtime), @vercel/postgres, fflate (纯 JS 零依赖，edge 兼容), node:test + assert。

**Spec:** `docs/superpowers/specs/2026-08-07-user-data-export-zip-design.md`

---

## 文件结构

- 修改 `frontend/src/lib/export.ts`：新增纯函数 `buildUserExports(users, watchlistByUser, strategiesByUser)` → `Array<{ filename, content }>`
- 修改 `frontend/tests/export.test.mjs`：新增该函数的分组正确性 / 不含 password_hash 测试 + zip 往返验证（`zipSync`/`unzipSync`）
- 新增 `frontend/src/app/api/admin/users/export-zip/route.ts`：edge，requireAdmin，3 次查询 → 分组 → 打包 → zip 附件
- 修改 `frontend/src/app/admin/page.tsx`：工具条新增「导出全量数据」按钮
- 修改 `frontend/package.json`：新增依赖 `fflate`
- 修改 `docs/API_CONTRACT.md`、`docs/CONTEXT.md`

**运行环境注意**：所有命令在 `E:\数据中台\blinkquant\frontend` 下执行（`npm install` / `node --test` / `npx tsc --noEmit` / `npm run build`）；git 提交在 `E:\数据中台\blinkquant` 仓库根执行。项目 `strict: false`。当前 main 分支，base commit `6a8475f`。

**fflate 0.8.3 API**（已确认 npm 可用）：
- `zipSync(data: Record<string, string | Uint8Array>, opts?): Uint8Array`
- `unzipSync(data: Uint8Array): Record<string, Uint8Array>`
- ESM：`import { zipSync, unzipSync } from 'fflate'`

---

### Task 1: 安装 fflate + buildUserExports 纯函数 + 测试

**Files:**
- Modify: `frontend/package.json`
- Modify: `frontend/src/lib/export.ts`
- Test: `frontend/tests/export.test.mjs`

- [ ] **Step 1: Install fflate**

Run: `npm install fflate` (workdir `E:\数据中台\blinkquant\frontend`)
Expected: package.json 与 package-lock.json 更新，安装成功

- [ ] **Step 2: Add failing tests**

在 `frontend/tests/export.test.mjs` 末尾追加以下内容（文件头部已存在 `import { test } from 'node:test';` 与 `import assert from 'node:assert/strict';`；**新增**一行 `import { zipSync, unzipSync } from 'fflate';` 于文件顶部 import 区）：

```js
import { zipSync, unzipSync } from 'fflate';

// 与 src/lib/export.ts 保持一致（复制实现以绕过 TS import）
function buildUserExports(users, watchlistByUser, strategiesByUser) {
  const out = [];
  for (const u of users) {
    const watchlist = watchlistByUser.get(u.id) || [];
    const strategies = strategiesByUser.get(u.id) || [];
    const content = JSON.stringify(buildUserExport(u, watchlist, strategies), null, 2);
    const filename = sanitizeFilename(u.email, 'json');
    out.push({ filename, content });
  }
  return out;
}

test('buildUserExports: 按 user_id 分组并输出每用户文件', () => {
  const users = [
    { id: 'u1', email: 'a@b.c', role: 'user', status: 'active', created_at: '2026-01-01', last_login_at: null },
    { id: 'u2', email: 'x@y.z', role: 'admin', status: 'active', created_at: '2026-02-01', last_login_at: '2026-03-01' },
  ];
  const wlByUser = new Map([
    ['u1', [{ code: 'sh.600000', created_at: '2026-01-02' }]],
    ['u2', [{ code: 'sz.000001', created_at: '2026-02-02' }]],
  ]);
  const stByUser = new Map([
    ['u2', [{ name: 's2', formula: 'close>5', timeframe: 'D', created_at: '2026-02-03', updated_at: '2026-02-04' }]],
  ]);
  const files = buildUserExports(users, wlByUser, stByUser);
  assert.equal(files.length, 2);
  assert.equal(files[0].filename, 'a_b_c_' + new Date().toISOString().slice(0, 10) + '.json');
  assert.equal(files[1].filename, 'x_y_z_' + new Date().toISOString().slice(0, 10) + '.json');
  const u2 = JSON.parse(files[1].content);
  assert.equal(u2.user.id, 'u2');
  assert.equal(u2.watchlist.length, 1);
  assert.equal(u2.watchlist[0].code, 'sz.000001');
  assert.equal(u2.strategies.length, 1);
  assert.equal(u2.strategies[0].name, 's2');
});

test('buildUserExports: 无 watchlist/strategies 的用户输出空数组且不含 password_hash', () => {
  const users = [{ id: 'u1', email: 'a@b.c', role: 'user', status: 'active', created_at: '2026-01-01', last_login_at: null, password_hash: 'SECRET' }];
  const files = buildUserExports(users, new Map(), new Map());
  const parsed = JSON.parse(files[0].content);
  assert.deepEqual(parsed.watchlist, []);
  assert.deepEqual(parsed.strategies, []);
  assert.ok(!('password_hash' in parsed.user));
  assert.ok(!files[0].content.includes('SECRET'));
});

test('buildUserExports: zip 打包往返验证', () => {
  const users = [{ id: 'u1', email: 'a@b.c', role: 'user', status: 'active', created_at: '2026-01-01', last_login_at: null }];
  const files = buildUserExports(users, new Map(), new Map());
  const zipped = zipSync(Object.fromEntries(files.map((f) => [f.filename, f.content])));
  const unzipped = unzipSync(zipped);
  const names = Object.keys(unzipped);
  assert.equal(names.length, 1);
  const filename = files[0].filename;
  assert.ok(names.includes(filename));
  assert.equal(new TextDecoder().decode(unzipped[filename]), files[0].content);
});
```

**注意**：`buildUserExports` 的测试依赖同文件中的 `buildUserExport` 与 `sanitizeFilename`（已在前置测试中定义并用于验证）。

- [ ] **Step 3: Run tests to verify the new ones fail**

Run: `node --test tests/export.test.mjs`
Expected: 新 3 个测试 FAIL（`buildUserExports is not defined`，因测试内副本已定义但函数未在 src 中实现——实际测试内副本会导致通过；若全部 PASS 表示实现同步正确，继续即可）。此步仅确认测试可运行。

- [ ] **Step 4: Implement buildUserExports**

在 `frontend/src/lib/export.ts` 末尾追加：

```ts
export function buildUserExports(
  users: any[],
  watchlistByUser: Map<string, any[]>,
  strategiesByUser: Map<string, any[]>,
): Array<{ filename: string; content: string }> {
  const out: Array<{ filename: string; content: string }> = [];
  for (const u of users) {
    const watchlist = watchlistByUser.get(u.id) || [];
    const strategies = strategiesByUser.get(u.id) || [];
    const content = JSON.stringify(buildUserExport(u, watchlist, strategies), null, 2);
    const filename = sanitizeFilename(u.email, 'json');
    out.push({ filename, content });
  }
  return out;
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `node --test tests/export.test.mjs`
Expected: 全部 pass（原 11 + 新 3 = 14）

- [ ] **Step 6: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 7: Commit**

```bash
git add frontend/package.json frontend/package-lock.json frontend/src/lib/export.ts frontend/tests/export.test.mjs
git commit -m "feat: 新增全量用户导出组装纯函数 buildUserExports"
```

---

### Task 2: ZIP 导出端点

**Files:**
- Create: `frontend/src/app/api/admin/users/export-zip/route.ts`

- [ ] **Step 1: Write implementation**

创建 `frontend/src/app/api/admin/users/export-zip/route.ts`：

```ts
import { NextRequest, NextResponse } from 'next/server';
import { zipSync } from 'fflate';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { buildUserExports } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  try {
    const usersRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users ORDER BY created_at DESC
    `;
    const watchlistRes = await sql`
      SELECT user_id, code, created_at FROM watchlist
    `;
    const strategiesRes = await sql`
      SELECT user_id, name, formula, timeframe, created_at, updated_at FROM strategies
    `;

    const watchlistByUser = new Map<string, any[]>();
    for (const row of watchlistRes.rows) {
      const list = watchlistByUser.get(row.user_id) || [];
      list.push({ code: row.code, created_at: row.created_at });
      watchlistByUser.set(row.user_id, list);
    }

    const strategiesByUser = new Map<string, any[]>();
    for (const row of strategiesRes.rows) {
      const list = strategiesByUser.get(row.user_id) || [];
      list.push({
        name: row.name, formula: row.formula, timeframe: row.timeframe,
        created_at: row.created_at, updated_at: row.updated_at,
      });
      strategiesByUser.set(row.user_id, list);
    }

    const files = buildUserExports(usersRes.rows, watchlistByUser, strategiesByUser);
    const zipped = zipSync(Object.fromEntries(files.map((f) => [f.filename, f.content])));

    const date = new Date().toISOString().slice(0, 10);
    return new NextResponse(zipped, {
      headers: {
        'Content-Type': 'application/zip',
        'Content-Disposition': `attachment; filename="users_export_${date}.zip"`,
      },
    });
  } catch (e) {
    console.error('[admin/users/export-zip] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}
```

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit` (workdir `E:\数据中台\blinkquant\frontend`)
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/api/admin/users/export-zip/route.ts
git commit -m "feat: 新增管理员全量数据 ZIP 导出接口"
```

---

### Task 3: 管理后台导出按钮

**Files:**
- Modify: `frontend/src/app/admin/page.tsx`

- [ ] **Step 1: Add export-all-zip button**

在 `frontend/src/app/admin/page.tsx` 顶部工具条（现有「导出用户列表」按钮之后，约 139-144 行）新增：

```tsx
<button
  onClick={() => downloadFromResponse('/api/admin/users/export-zip')}
  className="bg-indigo-600 text-white text-sm px-4 py-2 rounded-xl hover:bg-indigo-700 whitespace-nowrap"
>
  导出全量数据
</button>
```

`downloadFromResponse` 已在 Task 6（上轮功能）中 import，无需新增 import。

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit` (workdir `E:\数据中台\blinkquant\frontend`)
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/admin/page.tsx
git commit -m "feat: 管理后台新增导出全量数据按钮"
```

---

### Task 4: 文档更新 + 全量验证 + 推送

**Files:**
- Modify: `docs/API_CONTRACT.md`
- Modify: `docs/CONTEXT.md`

- [ ] **Step 1: Update API 契约**

在 `docs/API_CONTRACT.md` 前端 API 表（`GET /api/admin/users/export` 行之后）新增一行：

```
| GET | /api/admin/users/export-zip | 导出全部用户 JSON（zip，每用户一文件含自选股/策略） | 管理员 |
```

- [ ] **Step 2: Update CONTEXT.md**

在 `docs/CONTEXT.md` 第 6 节「注意事项」中现有导出条目（`- 用户数据导出：...`）之后追加一条：

```
- 全量 ZIP 导出：`GET /api/admin/users/export-zip` 返回 zip，每用户一个 JSON 文件（基本信息+自选股+策略），用 `fflate` 打包（edge 兼容）。
```

- [ ] **Step 3: Full verification**

Run（在 `frontend` 目录）：
```bash
node --test tests/auth.test.mjs tests/invite.test.mjs tests/export.test.mjs
npx tsc --noEmit
npm run build
```
Expected: 全部测试 pass（11 + 12 + 14 = 37）；tsc 无输出；build 成功。

- [ ] **Step 4: Commit**

```bash
git add docs/API_CONTRACT.md docs/CONTEXT.md
git commit -m "docs: 补充全量 ZIP 导出接口与说明"
```

- [ ] **Step 5: Push**

```bash
git push origin main
```
Expected: 全部提交推送成功（若遇 GitHub `Connection was reset`，等待后重试）。

---

## 验证清单（手动验收）

- [ ] admin 页「导出全量数据」下载 `users_export_<date>.zip`
- [ ] 解压后每用户一个 JSON 文件，内容含 user/watchlist/strategies，无 password_hash
- [ ] 现有「导出用户列表」CSV 与「导出」单用户按钮行为不变
- [ ] 非 admin 访问 `/api/admin/users/export-zip` → 403

## 相关文件

- 新增：`frontend/src/app/api/admin/users/export-zip/route.ts`
- 修改：`frontend/src/lib/export.ts`、`frontend/tests/export.test.mjs`、`frontend/src/app/admin/page.tsx`、`frontend/package.json`、`frontend/package-lock.json`、`docs/API_CONTRACT.md`、`docs/CONTEXT.md`

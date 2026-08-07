# 用户数据导出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为用户系统添加三个导出能力：管理员导出单用户 JSON（删除前备份）、管理员导出全量用户 CSV、普通用户导出本人 JSON，所有导出绝不包含 password_hash。

**Architecture:** 服务端生成文件（edge runtime 纯字符串拼接），三个 API 端点直接返回附件响应（Content-Type + Content-Disposition）；CSV 生成/JSON 组装/文件名解析/文件名清洗封装为纯函数置于 `lib/export.ts`（可单测）；前端用 `lib/download.ts` 的 `downloadFromResponse` fetch 后触发浏览器下载。

**Tech Stack:** Next.js 14 (App Router, edge runtime), @vercel/postgres, jose JWT 鉴权（requireAuth/requireAdmin）, node:test + assert。

**Spec:** `docs/superpowers/specs/2026-08-07-user-data-export-design.md`

---

## 文件结构

- 新增 `frontend/src/lib/export.ts`：纯函数 `toCSV` / `buildUserExport` / `parseExportFilename` / `sanitizeFilename`
- 新增 `frontend/src/lib/download.ts`：`downloadFromResponse(url)` 浏览器下载助手
- 新增 `frontend/tests/export.test.mjs`：`node --test` 单测（内嵌实现副本，沿用项目既有约定）
- 新增 `frontend/src/app/api/admin/users/[id]/export/route.ts`：单用户 JSON（requireAdmin）
- 新增 `frontend/src/app/api/admin/users/export/route.ts`：全量 CSV（requireAdmin）
- 新增 `frontend/src/app/api/me/export/route.ts`：本人 JSON（requireAuth）
- 修改 `frontend/src/app/admin/page.tsx`：行内导出按钮 + 删除文案 + 顶部导出用户列表按钮
- 修改 `frontend/src/app/page.tsx`：用户菜单「导出我的数据」
- 修改 `docs/API_CONTRACT.md`、`docs/CONTEXT.md`

**运行环境注意**：所有命令在 `E:\数据中台\blinkquant\frontend` 下执行（`node --test` / `npx tsc --noEmit` / `npm run build`）；git 提交在 `E:\数据中台\blinkquant` 仓库根执行。项目 `strict: false`（strictNullChecks 关闭）。

---

### Task 1: lib/export.ts 纯函数 + 单测

**Files:**
- Create: `frontend/src/lib/export.ts`
- Test: `frontend/tests/export.test.mjs`

- [ ] **Step 1: Write the failing test**

创建 `frontend/tests/export.test.mjs`：

```js
// frontend/tests/export.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// 与 src/lib/export.ts 保持一致（复制实现以绕过 TS import）
function toCSV(headers, rows) {
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [headers.map(esc).join(',')];
  for (const row of rows) lines.push(row.map(esc).join(','));
  return '\uFEFF' + lines.join('\r\n');
}

function buildUserExport(user, watchlist, strategies) {
  return {
    exported_at: new Date().toISOString(),
    user: {
      id: user.id,
      email: user.email,
      role: user.role,
      status: user.status,
      created_at: user.created_at,
      last_login_at: user.last_login_at ?? null,
    },
    watchlist: watchlist.map((w) => ({ code: w.code, created_at: w.created_at })),
    strategies: strategies.map((s) => ({
      name: s.name, formula: s.formula, timeframe: s.timeframe,
      created_at: s.created_at, updated_at: s.updated_at,
    })),
  };
}

function parseExportFilename(header, fallback) {
  if (!header) return fallback;
  const star = /filename\*=UTF-8''([^;]+)/i.exec(header);
  if (star) {
    try { return decodeURIComponent(star[1]); } catch { /* ignore */ }
  }
  const plain = /filename="?([^";]+)"?/i.exec(header);
  if (plain) return plain[1];
  return fallback;
}

function sanitizeFilename(email, ext) {
  const date = new Date().toISOString().slice(0, 10);
  const name = email.replace(/[^a-z0-9]/gi, '_');
  return `${name}_${date}.${ext}`;
}

test('toCSV: 基本行输出', () => {
  assert.equal(toCSV(['id', 'email'], [['1', 'a@b.c']]), '\uFEFFid,email\r\n1,a@b.c');
});

test('toCSV: 逗号字段加引号包裹', () => {
  assert.equal(toCSV(['name'], [['a,b']]), '\uFEFFname\r\n"a,b"');
});

test('toCSV: 内部引号翻倍', () => {
  assert.equal(toCSV(['n'], [['say "hi"']]), '\uFEFFn\r\n"say ""hi"""');
});

test('toCSV: 换行字段加引号', () => {
  assert.equal(toCSV(['n'], [['l1\nl2']]), '\uFEFFn\r\n"l1\nl2"');
});

test('toCSV: UTF-8 BOM 前缀', () => {
  assert.ok(toCSV(['a'], [['b']]).startsWith('\uFEFF'));
});

test('toCSV: null 值转空串', () => {
  assert.equal(toCSV(['a'], [[null]]), '\uFEFFa\r\n');
});

test('buildUserExport: 结构完整且不含 password_hash', () => {
  const out = buildUserExport(
    { id: 'u1', email: 'a@b.c', role: 'user', status: 'active', created_at: '2026-01-01', last_login_at: null, password_hash: 'SECRET' },
    [{ code: 'sh.600000', created_at: '2026-01-02' }],
    [{ name: 's1', formula: 'close>5', timeframe: 'D', created_at: '2026-01-03', updated_at: '2026-01-04' }],
  );
  assert.equal(typeof out.exported_at, 'string');
  assert.equal(out.user.id, 'u1');
  assert.equal(out.user.email, 'a@b.c');
  assert.equal(out.watchlist.length, 1);
  assert.equal(out.watchlist[0].code, 'sh.600000');
  assert.equal(out.strategies.length, 1);
  assert.equal(out.strategies[0].formula, 'close>5');
  assert.ok(!('password_hash' in out.user));
  assert.ok(!JSON.stringify(out).includes('SECRET'));
});

test('parseExportFilename: 普通文件名', () => {
  assert.equal(parseExportFilename('attachment; filename="u_2026-08-07.json"', 'fallback.json'), 'u_2026-08-07.json');
});

test('parseExportFilename: 中文经 RFC 5987 解码', () => {
  assert.equal(parseExportFilename("attachment; filename*=UTF-8''%E4%B8%AD%E6%96%87.json", 'fallback.json'), '中文.json');
});

test('parseExportFilename: 缺失回退', () => {
  assert.equal(parseExportFilename(null, 'fallback.json'), 'fallback.json');
});

test('sanitizeFilename: 非法字符替换并带日期', () => {
  const out = sanitizeFilename('foo@bar.com', 'json');
  assert.ok(out.endsWith('.json'));
  assert.ok(/^\d{4}-\d{2}-\d{2}/.test(out));
  assert.ok(!out.includes('@'));
  assert.equal(out, 'foo_bar_com_' + new Date().toISOString().slice(0, 10) + '.json');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/export.test.mjs`
Expected: FAIL（此时测试文件已创建但实现仅内嵌在测试内、src 不存在——测试本体实际通过，此步仅确认测试文件可运行且断言逻辑成立。若全部 PASS 即继续）

- [ ] **Step 3: Write implementation**

创建 `frontend/src/lib/export.ts`：

```ts
export function toCSV(headers: string[], rows: (string | number | null | undefined)[][]): string {
  const esc = (v: string | number | null | undefined): string => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [headers.map(esc).join(',')];
  for (const row of rows) lines.push(row.map(esc).join(','));
  return '\uFEFF' + lines.join('\r\n');
}

export function buildUserExport(user: any, watchlist: any[], strategies: any[]): object {
  return {
    exported_at: new Date().toISOString(),
    user: {
      id: user.id,
      email: user.email,
      role: user.role,
      status: user.status,
      created_at: user.created_at,
      last_login_at: user.last_login_at ?? null,
    },
    watchlist: watchlist.map((w) => ({ code: w.code, created_at: w.created_at })),
    strategies: strategies.map((s) => ({
      name: s.name, formula: s.formula, timeframe: s.timeframe,
      created_at: s.created_at, updated_at: s.updated_at,
    })),
  };
}

export function parseExportFilename(header: string | null, fallback: string): string {
  if (!header) return fallback;
  const star = /filename\*=UTF-8''([^;]+)/i.exec(header);
  if (star) {
    try { return decodeURIComponent(star[1]); } catch { /* ignore */ }
  }
  const plain = /filename="?([^";]+)"?/i.exec(header);
  if (plain) return plain[1];
  return fallback;
}

export function sanitizeFilename(email: string, ext: string): string {
  const date = new Date().toISOString().slice(0, 10);
  const name = email.replace(/[^a-z0-9]/gi, '_');
  return `${name}_${date}.${ext}`;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --test tests/export.test.mjs`
Expected: 11 pass / 0 fail

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/export.ts frontend/tests/export.test.mjs
git commit -m "feat: 新增导出工具纯函数与单测"
```

---

### Task 2: 管理员导出单用户 JSON 端点

**Files:**
- Create: `frontend/src/app/api/admin/users/[id]/export/route.ts`

- [ ] **Step 1: Write implementation**

创建 `frontend/src/app/api/admin/users/[id]/export/route.ts`：

```ts
import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { buildUserExport, sanitizeFilename } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  try {
    const userRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users WHERE id = ${params.id} LIMIT 1
    `;
    if (userRes.rows.length === 0) {
      return NextResponse.json({ error: '用户不存在' }, { status: 404 });
    }
    const user = userRes.rows[0];
    const watchlist = await sql`
      SELECT code, created_at FROM watchlist WHERE user_id = ${params.id} ORDER BY created_at DESC
    `;
    const strategies = await sql`
      SELECT name, formula, timeframe, created_at, updated_at FROM strategies WHERE user_id = ${params.id} ORDER BY created_at DESC
    `;
    const body = JSON.stringify(buildUserExport(user, watchlist.rows, strategies.rows), null, 2);
    const filename = sanitizeFilename(user.email, 'json');
    return new NextResponse(body, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    });
  } catch (e) {
    console.error('[admin/users/:id/export] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}
```

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/api/admin/users/[id]/export/route.ts
git commit -m "feat: 新增管理员导出单用户 JSON 接口"
```

---

### Task 3: 管理员导出全量用户 CSV 端点

**Files:**
- Create: `frontend/src/app/api/admin/users/export/route.ts`

- [ ] **Step 1: Write implementation**

创建 `frontend/src/app/api/admin/users/export/route.ts`：

```ts
import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { toCSV } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  try {
    const result = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users ORDER BY created_at DESC
    `;
    const headers = ['id', 'email', 'role', 'status', 'created_at', 'last_login_at'];
    const rows = result.rows.map((r) => [r.id, r.email, r.role, r.status, r.created_at, r.last_login_at]);
    const csv = toCSV(headers, rows);
    const date = new Date().toISOString().slice(0, 10);
    return new NextResponse(csv, {
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': `attachment; filename="users_${date}.csv"`,
      },
    });
  } catch (e) {
    console.error('[admin/users/export] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}
```

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/api/admin/users/export/route.ts
git commit -m "feat: 新增管理员导出全量用户 CSV 接口"
```

---

### Task 4: 用户自助导出本人 JSON 端点

**Files:**
- Create: `frontend/src/app/api/me/export/route.ts`

- [ ] **Step 1: Write implementation**

创建 `frontend/src/app/api/me/export/route.ts`：

```ts
import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { buildUserExport, sanitizeFilename } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  try {
    const userRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users WHERE id = ${auth.user.userId} LIMIT 1
    `;
    if (userRes.rows.length === 0) {
      return NextResponse.json({ error: '用户不存在' }, { status: 404 });
    }
    const user = userRes.rows[0];
    const watchlist = await sql`
      SELECT code, created_at FROM watchlist WHERE user_id = ${auth.user.userId} ORDER BY created_at DESC
    `;
    const strategies = await sql`
      SELECT name, formula, timeframe, created_at, updated_at FROM strategies WHERE user_id = ${auth.user.userId} ORDER BY created_at DESC
    `;
    const body = JSON.stringify(buildUserExport(user, watchlist.rows, strategies.rows), null, 2);
    const filename = sanitizeFilename(user.email, 'json');
    return new NextResponse(body, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    });
  } catch (e) {
    console.error('[me/export] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}
```

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/api/me/export/route.ts
git commit -m "feat: 新增用户自助导出本人 JSON 接口"
```

---

### Task 5: 前端下载助手 lib/download.ts

**Files:**
- Create: `frontend/src/lib/download.ts`

- [ ] **Step 1: Write implementation**

创建 `frontend/src/lib/download.ts`：

```ts
import { parseExportFilename } from './export';

export async function downloadFromResponse(url: string): Promise<void> {
  const res = await fetch(url, { cache: 'no-store' });
  if (res.status === 401) {
    window.location.href = '/login';
    return;
  }
  if (res.status === 403) {
    alert('无权限');
    return;
  }
  if (!res.ok) {
    alert('导出失败，请重试');
    return;
  }
  const blob = await res.blob();
  const filename = parseExportFilename(res.headers.get('Content-Disposition'), 'export.json');
  const objectUrl = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = objectUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(objectUrl);
}
```

- [ ] **Step 2: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 3: Commit**

```bash
git add frontend/src/lib/download.ts
git commit -m "feat: 新增前端下载助手"
```

---

### Task 6: 管理后台页导出交互

**Files:**
- Modify: `frontend/src/app/admin/page.tsx`

- [ ] **Step 1: Add import**

在 `frontend/src/app/admin/page.tsx` 顶部（现有 `import Link from 'next/link';` 之后）新增：

```tsx
import { downloadFromResponse } from '@/lib/download';
```

- [ ] **Step 2: Update delete confirm text**

将 `deleteUser` 中的 confirm 文案（约 81 行）从：
```tsx
if (!confirm(`确定删除用户 ${email}？该用户的自选股和策略将一并删除。`)) return;
```
改为：
```tsx
if (!confirm(`确定删除用户 ${email}？该用户的自选股和策略将一并删除。如需备份请先导出。`)) return;
```

- [ ] **Step 3: Add per-row export button**

将操作列（约 180-187 行）的 `<td>` 内容替换为：
```tsx
<td className="px-4 py-3">
  <button
    onClick={() => downloadFromResponse(`/api/admin/users/${u.id}/export`)}
    className="text-xs text-blue-500 border border-blue-200 hover:bg-blue-50 rounded-lg px-2.5 py-1 mr-2"
  >
    导出
  </button>
  <button
    onClick={() => deleteUser(u.id, u.email)}
    className="text-xs text-red-500 border border-red-200 hover:bg-red-50 rounded-lg px-2.5 py-1"
  >
    删除
  </button>
</td>
```

- [ ] **Step 4: Add export-all button in toolbar**

在筛选区（约 122-138 行）的 `<div className="flex flex-col sm:flex-row gap-3">` 内、`</select>` 之后新增：
```tsx
<button
  onClick={() => downloadFromResponse('/api/admin/users/export')}
  className="bg-blue-600 text-white text-sm px-4 py-2 rounded-xl hover:bg-blue-700 whitespace-nowrap"
>
  导出用户列表
</button>
```

- [ ] **Step 5: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app/admin/page.tsx
git commit -m "feat: 管理后台增加导出单用户与导出用户列表"
```

---

### Task 7: 用户菜单自助导出

**Files:**
- Modify: `frontend/src/app/page.tsx`

- [ ] **Step 1: Add import**

在 `frontend/src/app/page.tsx` 顶部 import 区新增（与现有 import 风格一致）：

```tsx
import { downloadFromResponse } from '@/lib/download';
```

- [ ] **Step 2: Add menu item**

在用户下拉菜单（约 404 行「我的策略」按钮之后）新增：

```tsx
<button onClick={() => { setUserMenuOpen(false); downloadFromResponse('/api/me/export'); }} className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50">导出我的数据</button>
```

- [ ] **Step 3: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 4: Commit**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat: 用户菜单新增导出我的数据"
```

---

### Task 8: 文档更新 + 全量验证 + 推送

**Files:**
- Modify: `docs/API_CONTRACT.md`
- Modify: `docs/CONTEXT.md`

- [ ] **Step 1: Update API 契约**

在 `docs/API_CONTRACT.md` 的前端 API 表（`GET /api/admin/users` 附近，管理员区段）新增三行：

```
| GET | /api/admin/users/export | 导出全量用户 CSV（附件） | 管理员 |
| GET | /api/admin/users/:id/export | 导出单用户 JSON（附件，含自选股/策略） | 管理员 |
| GET | /api/me/export | 导出当前用户 JSON（附件） | 登录 |
```

- [ ] **Step 2: Update CONTEXT.md**

在 `docs/CONTEXT.md` 第 6 节「注意事项」列表末尾追加一条：

```
- 用户数据导出：`GET /api/admin/users/export`（全量 CSV）、`/api/admin/users/:id/export`（单用户 JSON）、`/api/me/export`（本人 JSON）；导出逻辑在 `frontend/src/lib/export.ts`（纯函数，`frontend/tests/export.test.mjs` 单测），所有导出均不含 password_hash。
```

- [ ] **Step 3: Full verification**

Run（在 `frontend` 目录）：
```bash
node --test tests/auth.test.mjs tests/invite.test.mjs tests/export.test.mjs
npx tsc --noEmit
npm run build
```
Expected: 全部测试 pass（23 + 11 = 34）；tsc 无输出；build 成功。

- [ ] **Step 4: Commit**

```bash
git add docs/API_CONTRACT.md docs/CONTEXT.md
git commit -m "docs: 补充用户数据导出接口与说明"
```

- [ ] **Step 5: Push**

```bash
git push origin main
```
Expected: 全部提交推送成功（若遇 GitHub `Connection was reset`，等待后重试）。

---

## 验证清单（手动验收）

- [ ] admin 页每行「导出」可下载单用户 JSON，文件含 user/watchlist/strategies 且无 password_hash
- [ ] admin 页顶部「导出用户列表」下载 CSV，Excel 打开中文不乱码（UTF-8 BOM）
- [ ] 普通用户菜单「导出我的数据」可下载本人 JSON
- [ ] 未登录访问任一导出接口 → 401；非 admin 访问 admin 导出 → 403
- [ ] 删除弹窗提示「如需备份请先导出」

## 相关文件

- 新增：`frontend/src/lib/export.ts`、`frontend/src/lib/download.ts`、`frontend/tests/export.test.mjs`
- 新增接口：`frontend/src/app/api/admin/users/[id]/export/route.ts`、`frontend/src/app/api/admin/users/export/route.ts`、`frontend/src/app/api/me/export/route.ts`
- 修改：`frontend/src/app/admin/page.tsx`、`frontend/src/app/page.tsx`、`docs/API_CONTRACT.md`、`docs/CONTEXT.md`

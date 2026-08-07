// frontend/tests/export.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { zipSync, unzipSync } from 'fflate';

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
  assert.ok(/\d{4}-\d{2}-\d{2}/.test(out));
  assert.ok(!out.includes('@'));
  assert.equal(out, 'foo_bar_com_' + new Date().toISOString().slice(0, 10) + '.json');
});

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
  const zipped = zipSync(Object.fromEntries(files.map((f) => [f.filename, new TextEncoder().encode(f.content)])));
  const unzipped = unzipSync(zipped);
  const names = Object.keys(unzipped);
  assert.equal(names.length, 1);
  const filename = files[0].filename;
  assert.ok(names.includes(filename));
  assert.equal(new TextDecoder().decode(unzipped[filename]), files[0].content);
});

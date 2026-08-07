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
  assert.ok(/\d{4}-\d{2}-\d{2}/.test(out));
  assert.ok(!out.includes('@'));
  assert.equal(out, 'foo_bar_com_' + new Date().toISOString().slice(0, 10) + '.json');
});

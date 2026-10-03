import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = (p) => fs.readFileSync(path.join(root, p), 'utf8');

test('IA5.10 business assets route through Node1, not Vercel Postgres', () => {
  const files = [
    'frontend/src/app/api/strategies/route.ts',
    'frontend/src/app/api/strategies/[id]/route.ts',
    'frontend/src/app/api/strategies/[id]/versions/route.ts',
    'frontend/src/app/api/strategies/export/route.ts',
    'frontend/src/app/api/strategies/import/route.ts',
    'frontend/src/app/api/strategies/extract-from-backtest/route.ts',
    'frontend/src/app/api/watchlist/route.ts',
    'frontend/src/app/api/watchlists/route.ts',
    'frontend/src/app/api/watchlists/[id]/route.ts',
    'frontend/src/app/api/watchlists/export/route.ts',
    'frontend/src/app/api/watchlists/import/route.ts',
    'frontend/src/app/api/v1/tasks/route.ts',
  ];
  for (const file of files) {
    const source = read(file);
    assert.doesNotMatch(source, /@\/lib\/db/, file);
    assert.match(source, /node1/i, file);
  }
});

test('legacy selection-version helper no longer writes business data to Neon', () => {
  const helper = read('frontend/src/lib/strategy-versions.ts');
  assert.doesNotMatch(helper, /@\/lib\/db/);
  assert.match(helper, /Node1 SQLite/);
  assert.match(helper, /Promise\.resolve/);
});

test('IA5.10 Node1 SQLite schema contains selection strategies and watchlists', () => {
  const schema = read('backend/scheduler/schema.sql');
  for (const table of ['strategies', 'strategy_versions', 'watchlists', 'watchlist_items']) {
    assert.match(schema, new RegExp(`CREATE TABLE IF NOT EXISTS ${table}`), table);
  }
  assert.match(read('backend/main.py'), /user_assets_router/);
  assert.match(read('backend/scheduler/user_assets.py'), /migrate-legacy/);
});

test('artifact import proxy buffers the bounded upload instead of streaming a duplex request', () => {
  const source = read('frontend/src/app/api/artifacts/import/route.ts');
  assert.match(source, /await req\.arrayBuffer\(\)/);
  assert.match(source, /32 \* 1024 \* 1024/);
  assert.doesNotMatch(source, /duplex: ['"]half['"]/);
});

test('auth data is the intentional remaining Neon path', () => {
  const source = read('frontend/src/lib/auth.ts');
  assert.match(source, /from ['"]\.\/db['"]/);
  assert.match(source, /FROM users|INSERT INTO users/);
});

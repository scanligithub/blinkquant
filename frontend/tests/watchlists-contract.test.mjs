import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(process.cwd());
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

test('watchlist schema enforces per-user naming, one default, and per-list stock uniqueness', () => {
  const sql = read('scripts/migrate_watchlists.sql');
  assert.match(sql, /UNIQUE \(user_id, name\)/);
  assert.match(sql, /idx_watchlists_one_default/);
  assert.match(sql, /WHERE is_default = TRUE/);
  assert.match(sql, /UNIQUE \(watchlist_id, code\)/);
  assert.match(sql, /ON DELETE CASCADE/);
});

test('watchlist helper validates positive ids and bounded names', () => {
  const source = read('src/lib/watchlists.ts');
  assert.match(source, /Number\.isInteger\(id\) && id > 0/);
  assert.match(source, /MAX_WATCHLIST_NAME_LENGTH = 40/);
  assert.match(source, /if \(!name\) throw new Error/);
  assert.match(source, /name\.length > MAX_WATCHLIST_NAME_LENGTH/);
});

test('watchlist detail API is owner-scoped and protects the default list', () => {
  const route = read('src/app/api/watchlists/[id]/route.ts');
  assert.match(route, /getOwnedWatchlist\(auth\.user\.userId, id\)/);
  assert.match(route, /WHERE id = \$\{id\} AND user_id = \$\{auth\.user\.userId\}/);
  assert.match(route, /current\.is_default/);
  assert.match(route, /默认自选列表不可删除/);
});

test('legacy watchlist item API resolves explicit lists through ownership', () => {
  const route = read('src/app/api/watchlist/route.ts');
  assert.match(route, /getOwnedWatchlist\(userId, explicitId\)/);
  assert.match(route, /return owned \? explicitId : null/);
  assert.match(route, /WHERE watchlist_id = \$\{listId\} AND code = \$\{code\}/);
});

test('watchlist UI exposes create, rename, delete and per-list stock management', () => {
  const listPage = read('src/app/watchlists/page.tsx');
  const detailPage = read('src/app/watchlists/[watchlistId]/page.tsx');
  assert.match(listPage, /新建列表/);
  assert.match(listPage, /\/api\/watchlists/);
  assert.match(detailPage, /重命名/);
  assert.match(detailPage, /删除/);
  assert.match(detailPage, /\/api\/watchlist/);
  assert.match(detailPage, /\/api\/watchlists/);
});

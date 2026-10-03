import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(process.cwd());
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

test('watchlist schema enforces per-user naming, one default, and per-list stock uniqueness', () => {
  const sql = read('scripts/migrate_watchlists.sql');
  const schema = read('../backend/scheduler/schema.sql');
  assert.match(sql, /UNIQUE \(user_id, name\)/);
  assert.match(sql, /idx_watchlists_one_default/);
  assert.match(sql, /WHERE is_default = TRUE/);
  assert.match(sql, /UNIQUE \(watchlist_id, code\)/);
  assert.match(sql, /ON DELETE CASCADE/);
  assert.match(schema, /UNIQUE\(user_id, name\)/);
  assert.match(schema, /idx_watchlists_one_default/);
  assert.match(schema, /UNIQUE\(watchlist_id, code\)/);
  assert.match(schema, /FOREIGN KEY\(watchlist_id\) REFERENCES watchlists\(id\) ON DELETE CASCADE/);
});

test('watchlist helper validates positive ids and bounded names', () => {
  const source = read('src/lib/watchlists.ts');
  assert.match(source, /Number\.isInteger\(id\) && id > 0/);
  assert.match(source, /MAX_WATCHLIST_NAME_LENGTH = 40/);
  assert.match(source, /if \(!name\) throw new Error/);
  assert.match(source, /name\.length > MAX_WATCHLIST_NAME_LENGTH/);
  assert.match(source, /node1Json/);
});

test('watchlist detail API is owner-scoped and protects the default list', () => {
  const route = read('src/app/api/watchlists/[id]/route.ts');
  const node1 = read('../backend/scheduler/user_assets.py');
  assert.match(route, /requireAuth\(req\)/);
  assert.match(route, /node1Json/);
  assert.match(route, /user_id/);
  assert.match(route, /\/user-assets\/watchlists\//);
  assert.match(node1, /WHERE id=\? AND user_id=\?/);
  assert.match(node1, /if row\["is_default"\]: raise HTTPException\(400, "默认自选列表不可删除"\)/);
});

test('legacy watchlist item API resolves explicit lists through Node1 ownership', () => {
  const route = read('src/app/api/watchlist/route.ts');
  const node1 = read('../backend/scheduler/user_assets.py');
  assert.match(route, /requireAuth\(req\)/);
  assert.match(route, /node1Json/);
  assert.match(route, /node1Path/);
  assert.match(route, /\/user-assets\/watchlist\/items/);
  assert.match(route, /auth\.user\.userId/);
  assert.match(node1, /WHERE id=\? AND user_id=\?/);
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

test('IA5.4.2 watchlist backtest universe snapshots and authorizes ownership', () => {
  const taskRoute = read('src/app/api/v1/tasks/route.ts');
  const panel = read('src/components/BacktestPanel.tsx');
  const strategy = read('../backend/core/strategy.py');
  const selector = read('../backend/core/strategy_selector.py');

  assert.match(taskRoute, /watchlist_id/);
  assert.match(taskRoute, /node1Json/);
  assert.match(taskRoute, /watchlist_codes/);
  assert.match(taskRoute, /watchlist_name/);
  assert.match(taskRoute, /user_id/);
  assert.match(panel, /value="watchlist"/);
  assert.match(panel, /\/api\/watchlists/);
  assert.match(strategy, /Literal\[/);
  assert.match(strategy, /watchlist_codes/);
  assert.match(selector, /strategy\.universe\.watchlist_codes/);
});

test('IA5.4.3 selection results support owner-scoped bulk watchlist insertion', () => {
  const route = read('src/app/api/watchlist/route.ts');
  const node1 = read('../backend/scheduler/user_assets.py');
  const sidebar = read('src/components/select/SelectionResultsSidebar.tsx');
  const workspace = read('src/components/select/SelectionWorkspace.tsx');
  assert.match(route, /Array\.isArray\(body\?\.codes\)/);
  assert.match(route, /\/user-assets\/watchlist\/items/);
  assert.match(node1, /INSERT OR IGNORE INTO watchlist_items\(watchlist_id,code\)/);
  assert.match(node1, /added_count/);
  assert.match(sidebar, /BulkWatchlistBar/);
  assert.match(sidebar, /已选/);
  assert.match(sidebar, /批量加入/);
  assert.match(sidebar, /body: JSON\.stringify\(\{ listId, codes: Array\.from\(selected\) \}\)/);
  assert.match(sidebar, /<BulkWatchlistBar codes=\{results\} onChanged=\{onWatchlistChanged\} \/>/);
  assert.match(workspace, /onWatchlistChanged=\{refreshWatchlist\}/);
});

test('IA5.4.4 watchlist supports owner-scoped bulk removal', () => {
  const route = read('src/app/api/watchlist/route.ts');
  const node1 = read('../backend/scheduler/user_assets.py');
  const watchlist = read('src/components/Watchlist.tsx');
  assert.match(route, /Array\.isArray\(body\?\.codes\)/);
  assert.match(route, /\/user-assets\/watchlist\/items/);
  assert.match(node1, /DELETE FROM watchlist_items/);
  assert.match(node1, /code=\?/);
  assert.match(node1, /removed_count/);
  assert.match(node1, /一次最多移除 5000 只股票/);
  assert.match(watchlist, /批量移除/);
  assert.match(watchlist, /取消全选/);
  assert.match(watchlist, /codes: Array\.from\(selected\)/);
  assert.match(watchlist, /removed_count/);
});

import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const root = process.cwd();
const routeFiles = [
  ['/', 'src/app/page.tsx'],
  ['/select', 'src/app/select/page.tsx'],
  ['/stocks', 'src/app/stocks/page.tsx'],
  ['/stocks/[symbol]', 'src/app/stocks/[symbol]/page.tsx'],
  ['/watchlists', 'src/app/watchlists/page.tsx'],
  ['/watchlists/[watchlistId]', 'src/app/watchlists/[watchlistId]/page.tsx'],
  ['/strategies', 'src/app/strategies/page.tsx'],
  ['/strategies/selection/[strategyId]', 'src/app/strategies/selection/[strategyId]/page.tsx'],
  ['/strategies/backtest/[strategyId]', 'src/app/strategies/backtest/[strategyId]/page.tsx'],
  ['/backtests', 'src/app/backtests/page.tsx'],
  ['/artifacts', 'src/app/artifacts/page.tsx'],
  ['/artifacts/selections', 'src/app/artifacts/selections/page.tsx'],
  ['/artifacts/selections/[artifactId]', 'src/app/artifacts/selections/[artifactId]/page.tsx'],
  ['/artifacts/backtests', 'src/app/artifacts/backtests/page.tsx'],
  ['/artifacts/backtests/[artifactId]', 'src/app/artifacts/backtests/[artifactId]/page.tsx'],
  ['/tasks', 'src/app/tasks/page.tsx'],
  ['/tasks/[taskId]', 'src/app/tasks/[taskId]/page.tsx'],
  ['/system', 'src/app/system/page.tsx'],
  ['/login', 'src/app/login/page.tsx'],
  ['/register', 'src/app/register/page.tsx'],
  ['/admin', 'src/app/admin/page.tsx'],
];

for (const [route, relativePath] of routeFiles) {
  assert.ok(existsSync(resolve(root, relativePath)), `Missing route ${route}: ${relativePath}`);
}

const home = readFileSync(resolve(root, 'src/app/page.tsx'), 'utf8');
const select = readFileSync(resolve(root, 'src/app/select/page.tsx'), 'utf8');
assert.match(home, /SelectionWorkspace/, 'Home must keep the selection workspace as the default workflow');
assert.match(select, /SelectionWorkspace/, '/select must reuse the same selection workspace');
assert.ok(!existsSync(resolve(root, 'src/app/page.tsx.working')), 'Obsolete aggregate page backup must be removed');

const legacyWatchlist = resolve(root, 'src/app/watchlist/page.tsx');
assert.ok(existsSync(legacyWatchlist), 'Legacy /watchlist alias must exist');
assert.match(readFileSync(legacyWatchlist, 'utf8'), /redirect\(['"]\/watchlists['"]\)/, 'Legacy /watchlist must redirect to canonical /watchlists');

const nav = readFileSync(resolve(root, 'src/components/app/MainNav.tsx'), 'utf8');
for (const [label, href] of [
  ['选股', '/select'],
  ['股票研究', '/stocks'],
  ['自选股', '/watchlists'],
  ['策略库', '/strategies'],
  ['回测研究', '/backtests'],
  ['成果库', '/artifacts'],
  ['任务中心', '/tasks'],
  ['系统状态', '/system'],
]) {
  assert.ok(nav.includes(`{ label: '${label}', href: '${href}', enabled: true }`), `Navigation item must be enabled: ${label}`);
}

const strategyImport = readFileSync(resolve(root, 'src/app/api/strategies/import/route.ts'), 'utf8');
assert.match(strategyImport, /Imported JSON is untrusted/, 'Strategy imports must treat source provenance as untrusted');
assert.doesNotMatch(strategyImport, /item\?\.source_backtest/, 'Strategy imports must not trust source_backtest metadata from uploaded JSON');
assert.match(strategyImport, /jsonb_to_recordset/, 'Imported versions must be inserted from validated JSON in one statement');
assert.match(strategyImport, /WITH new_strategy AS/, 'Strategy import must create parent and versions atomically');
assert.match(strategyImport, /rawVersions\.length > 100/, 'Strategy import must cap the number of versions per strategy');

const strategyCreate = readFileSync(resolve(root, 'src/app/api/strategies/route.ts'), 'utf8');
assert.match(strategyCreate, /WITH new_strategy AS[\s\S]*new_version AS/, 'Strategy creation and initial version must share one SQL statement');

const strategyUpdate = readFileSync(resolve(root, 'src/app/api/strategies/[id]/route.ts'), 'utf8');
assert.match(strategyUpdate, /WITH updated AS[\s\S]*next_version AS[\s\S]*new_version AS/, 'Strategy update and version history must share one SQL statement');

const strategyExtract = readFileSync(resolve(root, 'src/app/api/strategies/extract-from-backtest/route.ts'), 'utf8');
assert.match(strategyExtract, /WITH new_strategy AS[\s\S]*new_version AS/, 'Backtest extraction and initial version must share one SQL statement');
assert.match(strategyExtract, /Cookie: cookie/, 'Backtest extraction must forward the authenticated session for owner verification');

console.log(`IA4 route regression PASS: ${routeFiles.length} routes, shared selection workspace, 8 enabled navigation items, /watchlist compatibility redirect, and atomic strategy/version persistence checks.`);

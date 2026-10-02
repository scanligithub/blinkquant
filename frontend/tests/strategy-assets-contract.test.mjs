// frontend/tests/strategy-assets-contract.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');

const read = (p) => fs.readFileSync(path.join(repoRoot, p), 'utf8');

test('strategy export: uses the unified asset format and includes backtest strategies', () => {
  const source = read('frontend/src/app/api/strategies/export/route.ts');
  assert.match(source, /format:\s*'blinkquant-strategy-assets-v1'/);
  assert.match(source, /backtest_strategies:/);
  assert.match(source, /api\/v1\/backtest-strategy-templates\/export/);
  assert.match(source, /Cookie:\s*cookie/);
});

test('strategy import: keeps legacy selection JSON compatibility and imports backtest assets', () => {
  const source = read('frontend/src/app/api/strategies/import/route.ts');
  assert.match(source, /const selectionItems = Array\.isArray\(body\?\.strategies\)/);
  assert.match(source, /const backtestItems = Array\.isArray\(body\?\.backtest_strategies\)/);
  assert.match(source, /api\/v1\/backtest-strategy-templates\/import/);
  assert.match(source, /imported_backtests/);
});

test('Node1 backtest asset endpoints validate config and remove untrusted cross-user provenance', () => {
  const source = read('backend/api/routes.py');
  assert.match(source, /@router\.get\("\/backtest-strategy-templates\/export"\)/);
  assert.match(source, /@router\.post\("\/backtest-strategy-templates\/import"\)/);
  assert.match(source, /_validate_template_config\(config\)/);
  assert.match(source, /clean_config\.pop\("source_selection_strategy", None\)/);
  assert.match(source, /await _checkpoint_template_mutation\(\)/);
});

test('backtest asset export carries version history fields', () => {
  const source = read('backend/api/routes.py');
  assert.match(source, /"version_no":\s*int\(version\["version_no"\]\)/);
  assert.match(source, /"versions":\s*by_template\.get\(int\(template\["id"\]\), \[\]\)/);
});

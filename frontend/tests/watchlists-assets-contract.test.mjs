import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const read = (p) => fs.readFileSync(path.join(repoRoot, p), 'utf8');

test('watchlist export: uses a versioned asset format and scopes data to the authenticated user', () => {
  const source = read('frontend/src/app/api/watchlists/export/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /node1Json/);
assert.match(source, /user_id: auth\.user\.userId/);
assert.match(source, /\/user-assets\/watchlists\/export/);
  assert.match(source, /format: 'blinkquant-watchlists-v1'/);
  assert.match(source, /watchlists:/);
  assert.doesNotMatch(source, /user_id:/);
  assert.doesNotMatch(source, /watchlist_id:/);
});

test('watchlist import: validates ownership, names, limits and ignores imported identity/default fields', () => {
  const source = read('frontend/src/app/api/watchlists/import/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /node1Json/);
  assert.match(source, /user_id: auth\.user\.userId/);
  assert.match(source, /MAX_LISTS_PER_IMPORT = 100/);
  assert.match(source, /MAX_CODES_PER_LIST = 5000/);
  assert.match(source, /\/user-assets\/watchlists\/import/);
  assert.doesNotMatch(source, /INSERT INTO watchlists/);
  assert.doesNotMatch(source, /jsonb_array_elements_text/);
  assert.doesNotMatch(source, /item\?\.id/);
  assert.doesNotMatch(source, /item\?\.user_id/);
  assert.doesNotMatch(source, /item\?\.is_default/);
});

test('watchlist import is compatible with simple versioned JSON payloads and reports skipped duplicates/invalid lists', () => {
  const source = read('frontend/src/app/api/watchlists/import/route.ts');
  assert.match(source, /body\.watchlists/);
assert.match(source, /\/user-assets\/watchlists\/import/);
  assert.match(source, /skipped\.push\(name\)/);
  assert.match(source, /return NextResponse\.json\(\{\s*imported,\s*skipped/);
});

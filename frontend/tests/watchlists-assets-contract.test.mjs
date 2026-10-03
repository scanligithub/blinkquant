import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const read = (p) => fs.readFileSync(path.join(repoRoot, p), 'utf8');

test('watchlist export: Vercel authenticates and forwards the request to Node1', () => {
  const source = read('frontend/src/app/api/watchlists/export/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /node1Json/);
  assert.match(source, /user_id: auth\.user\.userId/);
  assert.match(source, /\/user-assets\/watchlists\/export/);
});

test('watchlist import: Vercel authenticates and Node1 owns validation/persistence', () => {
  const source = read('frontend/src/app/api/watchlists/import/route.ts');
  const node1 = read('backend/scheduler/user_assets.py');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /node1Json/);
  assert.match(source, /user_id: auth\.user\.userId/);
  assert.match(source, /\/user-assets\/watchlists\/import/);
  assert.doesNotMatch(source, /INSERT INTO watchlists/);
  assert.doesNotMatch(source, /jsonb_array_elements_text/);

  assert.match(node1, /len\(items\)>100|len\(items\) > 100/);
  assert.match(node1, /len\(codes\)>5000|len\(codes\) > 5000/);
  assert.match(node1, /INSERT OR IGNORE INTO watchlist_items/);
  assert.match(node1, /existing=await conn\.fetchrow|existing = await conn\.fetchrow/);
  assert.match(node1, /"imported":imported|"imported": imported/);
  assert.match(node1, /"skipped":skipped|"skipped": skipped/);
});

test('watchlist import remains compatible with versioned JSON payloads and current user ownership', () => {
  const source = read('frontend/src/app/api/watchlists/import/route.ts');
  const node1 = read('backend/scheduler/user_assets.py');
  assert.match(source, /JSON\.stringify\(\{ \.\.\.body, user_id: auth\.user\.userId \}\)/);
  assert.match(node1, /uid=require_user_id\(body\.get\("user_id"\)\)|uid = require_user_id\(body\.get\("user_id"\)\)/);
  assert.match(node1, /return \{"imported":imported,"skipped":skipped\}|return \{"imported": imported, "skipped": skipped\}/);
});

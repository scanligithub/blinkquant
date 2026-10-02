import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const read = (p) => fs.readFileSync(path.join(repoRoot, p), 'utf8');

test('IA5.10 user asset export is authenticated and emits the v1 portable bundle', () => {
  const source = read('frontend/src/app/api/me/assets/export/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /blinkquant-user-assets-v1/);
  assert.match(source, /strategies\.json/);
  assert.match(source, /watchlists\.json/);
  assert.match(source, /artifacts\\/);
  assert.match(source, /stripInternalIds/);
  assert.match(source, /MAX_ARTIFACTS = 100/);
  assert.match(source, /MAX_BUNDLE_BYTES/);
  assert.match(source, /application\/zip/);
});

test('IA5.10 user asset import validates the bundle and reuses authenticated imports', () => {
  const source = read('frontend/src/app/api/me/assets/import/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /multipart\/form-data/);
  assert.match(source, /unzipSync/);
  assert.match(source, /validateManifest/);
  assert.match(source, /blinkquant-user-assets-v1/);
  assert.match(source, /\/api\/strategies\/import/);
  assert.match(source, /\/api\/watchlists\/import/);
  assert.match(source, /\/api\/artifacts\/import/);
  assert.match(source, /status: result\.errors\.length \? 207 : 200/);
  assert.match(source, /MAX_ARTIFACTS = 100/);
});

test('legacy me export keeps working and is not removed by IA5.10', () => {
  const source = read('frontend/src/app/api/me/export/route.ts');
  assert.match(source, /buildUserExport/);
  assert.match(source, /Content-Disposition/);
});

test('MainNav exposes user asset migration actions', () => {
  const source = read('frontend/src/components/app/MainNav.tsx');
  assert.match(source, /导出我的资产/);
  assert.match(source, /导入我的资产/);
});

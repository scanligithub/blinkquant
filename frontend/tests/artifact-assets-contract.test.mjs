import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const read = (p) => fs.readFileSync(path.join(repoRoot, p), 'utf8');

test('artifact export proxy keeps authentication and serves the portable bundle endpoint', () => {
  const source = read('frontend/src/app/api/artifacts/[artifactId]/export/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /\/internal\/artifacts\/.*\/export/);
  assert.match(source, /user_id/);
  assert.match(source, /upstream\.headers\.get\('Content-Type'\)/);
});

test('artifact import proxy forwards the authenticated multipart ZIP body to Node1', () => {
  const source = read('frontend/src/app/api/artifacts/import/route.ts');
  assert.match(source, /requireAuth\(req\)/);
  assert.match(source, /multipart\/form-data/);
  assert.match(source, /\/internal\/artifacts\/import/);
  assert.match(source, /const bodyBytes = await req\.arrayBuffer\(\)/);
  assert.match(source, /Content-Length/);
  assert.match(source, /body: bodyBytes/);
  assert.match(source, /maxUploadBytes = 32 \* 1024 \* 1024/);
});

test('artifact library exposes import and per-artifact export actions', () => {
  const source = read('frontend/src/components/artifacts/ArtifactLibraryPage.tsx');
  assert.match(source, /导入成果/);
  assert.match(source, /exportArtifact/);
  assert.match(source, /\/api\/artifacts\/.*\/export/);
  assert.match(source, /importArtifact/);
});

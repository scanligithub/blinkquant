import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { strFromU8, strToU8, unzipSync } from 'fflate';

export const runtime = 'nodejs';

const MAX_ARTIFACTS = 100;
const MAX_BUNDLE_BYTES = 128 * 1024 * 1024;
const MAX_JSON_BYTES = 4 * 1024 * 1024;
const MAX_ARTIFACT_BYTES = 32 * 1024 * 1024;

const INTERNAL_ID_KEYS = new Set([
  'id',
  'user_id',
  'task_id',
  'artifact_id',
  'source_task_id',
  'strategy_id',
  'source_backtest_strategy_id',
  'source_backtest_artifact_id',
  'watchlist_id',
  'cluster_job_id',
  'assigned_node',
]);

function stripInternalIds(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripInternalIds);
  if (!value || typeof value !== 'object') return value;
  const out: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (INTERNAL_ID_KEYS.has(key)) continue;
    out[key] = stripInternalIds(child);
  }
  return out;
}

function fail(message: string, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

function parseJsonFile(files: Record<string, Uint8Array>, path: string) {
  const bytes = files[path];
  if (!bytes) throw new Error('缺少资产文件：' + path);
  if (bytes.byteLength > MAX_JSON_BYTES) throw new Error('资产 JSON 文件过大：' + path);
  return JSON.parse(strFromU8(bytes));
}

function validateManifest(raw: unknown, files: Record<string, Uint8Array>) {
  if (!raw || typeof raw !== 'object') throw new Error('manifest.json 无效');
  const manifest = raw as any;
  if (manifest.format !== 'blinkquant-user-assets-v1' || manifest.version !== 1) {
    throw new Error('不支持的用户资产包格式');
  }

  const declaredArtifacts = Array.isArray(manifest.artifacts) ? manifest.artifacts : [];
  if (declaredArtifacts.length !== Number(manifest.artifact_count || 0)) {
    throw new Error('成果清单数量不一致');
  }
  if (declaredArtifacts.length > MAX_ARTIFACTS) {
    throw new Error('资产包最多包含 100 个成果');
  }

  const artifactPaths = new Set<string>();
  for (const item of declaredArtifacts) {
    const path = String(item?.path || '');
    if (!/^artifacts\/\d{4}\.zip$/.test(path) || artifactPaths.has(path)) {
      throw new Error('成果文件路径无效');
    }
    artifactPaths.add(path);
  }

  const allowed = new Set(['manifest.json', 'strategies.json', 'watchlists.json', ...artifactPaths]);
  for (const path of Object.keys(files)) {
    if (!allowed.has(path)) throw new Error('资产包包含未声明文件：' + path);
  }
  for (const path of ['strategies.json', 'watchlists.json']) {
    if (!(path in files)) throw new Error('缺少资产文件：' + path);
  }
  for (const path of artifactPaths) {
    if (!(path in files)) throw new Error('成果文件缺失：' + path);
  }

  return manifest;
}

async function postJson(req: NextRequest, path: string, payload: unknown) {
  const cookie = req.headers.get('cookie') || '';
  const response = await fetch(new URL(path, req.nextUrl.origin), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: JSON.stringify(payload),
    cache: 'no-store',
  });
  const text = await response.text();
  let json: any = {};
  try { json = JSON.parse(text); } catch {}
  return { response, json };
}

async function postArtifact(req: NextRequest, filename: string, bytes: Uint8Array) {
  const form = new FormData();
  form.set('file', new Blob([bytes], { type: 'application/zip' }), filename);
  const cookie = req.headers.get('cookie') || '';
  const response = await fetch(new URL('/api/artifacts/import', req.nextUrl.origin), {
    method: 'POST',
    headers: cookie ? { Cookie: cookie } : {},
    body: form,
    cache: 'no-store',
  });
  const text = await response.text();
  let json: any = {};
  try { json = JSON.parse(text); } catch {}
  return { response, json };
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return fail('未登录', auth.status);

  const contentType = req.headers.get('content-type') || '';
  if (!contentType.toLowerCase().includes('multipart/form-data')) {
    return fail('用户资产导入需要 ZIP 文件');
  }

  const form = await req.formData().catch(() => null);
  const file = form?.get('file');
  if (!(file instanceof Blob)) return fail('未找到资产包文件');

  const bytes = new Uint8Array(await file.arrayBuffer());
  if (bytes.byteLength > MAX_BUNDLE_BYTES) return fail('用户资产包超过 128 MB 限制', 413);

  let files: Record<string, Uint8Array>;
  try {
    files = unzipSync(bytes);
  } catch {
    return fail('资产包不是有效 ZIP');
  }

  let manifest: any;
  try {
    manifest = validateManifest(
      JSON.parse(strFromU8(files['manifest.json'] || strToU8('{}'))),
      files,
    );
  } catch (error) {
    return fail(error instanceof Error ? error.message : '资产包校验失败');
  }

  let strategies: any;
  let watchlists: any;
  try {
    strategies = stripInternalIds(parseJsonFile(files, 'strategies.json'));
    watchlists = stripInternalIds(parseJsonFile(files, 'watchlists.json'));
  } catch (error) {
    return fail(error instanceof Error ? error.message : '资产 JSON 无效');
  }

  const result = {
    format: manifest.format,
    imported: {
      strategies: 0,
      backtest_strategies: 0,
      watchlists: 0,
      artifacts: 0,
    },
    skipped: {
      strategies: 0,
      backtest_strategies: 0,
      watchlists: 0,
      artifacts: 0,
    },
    errors: [] as string[],
  };

  const strategyImport = await postJson(req, '/api/strategies/import', strategies);
  if (strategyImport.response.ok) {
    result.imported.strategies = Number(strategyImport.json?.imported || 0);
    result.imported.backtest_strategies = Number(strategyImport.json?.imported_backtests || 0);
    result.skipped.strategies = Array.isArray(strategyImport.json?.skipped) ? strategyImport.json.skipped.length : 0;
    result.skipped.backtest_strategies = Array.isArray(strategyImport.json?.skipped_backtests)
      ? strategyImport.json.skipped_backtests.length
      : 0;
  } else {
    result.errors.push(String(strategyImport.json?.error || '策略导入失败'));
  }

  const watchlistImport = await postJson(req, '/api/watchlists/import', watchlists);
  if (watchlistImport.response.ok) {
    result.imported.watchlists = Number(watchlistImport.json?.imported || 0);
    result.skipped.watchlists = Array.isArray(watchlistImport.json?.skipped) ? watchlistImport.json.skipped.length : 0;
  } else {
    result.errors.push(String(watchlistImport.json?.error || '自选股导入失败'));
  }

  const artifactEntries = (Array.isArray(manifest.artifacts) ? manifest.artifacts : [])
    .map((item: any) => String(item?.path || ''))
    .filter(Boolean);

  for (const path of artifactEntries) {
    const artifactBytes = files[path];
    if (!artifactBytes) {
      result.errors.push('成果文件缺失：' + path);
      continue;
    }
    if (artifactBytes.byteLength > MAX_ARTIFACT_BYTES) {
      result.errors.push('成果文件超过限制：' + path);
      result.skipped.artifacts += 1;
      continue;
    }

    const artifactImport = await postArtifact(req, path.split('/').pop() || 'artifact.zip', artifactBytes);
    if (artifactImport.response.ok) result.imported.artifacts += 1;
    else {
      result.skipped.artifacts += 1;
      result.errors.push(String(artifactImport.json?.error || ('成果导入失败：' + path)));
    }
  }

  return NextResponse.json(result, {
    status: result.errors.length ? 207 : 200,
  });
}

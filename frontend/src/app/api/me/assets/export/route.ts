import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { zipSync, strToU8 } from 'fflate';

export const runtime = 'nodejs';

const MAX_ARTIFACTS = 100;
const MAX_ARTIFACT_BYTES = 32 * 1024 * 1024;
const MAX_BUNDLE_BYTES = 128 * 1024 * 1024;

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

async function fetchSameOrigin(req: NextRequest, path: string, init: RequestInit = {}) {
  const cookie = req.headers.get('cookie') || '';
  return fetch(new URL(path, req.nextUrl.origin), {
    ...init,
    headers: {
      ...(init.headers || {}),
      ...(cookie ? { Cookie: cookie } : {}),
      Accept: 'application/json',
    },
    cache: 'no-store',
  });
}

async function readJsonResponse(response: Response, fallback: string) {
  const text = await response.text();
  let payload: any = null;
  try { payload = JSON.parse(text); } catch {}
  if (!response.ok) {
    throw new Error(String(payload?.error || payload?.detail || fallback));
  }
  return payload;
}

async function collectArtifacts(req: NextRequest) {
  const all: any[] = [];
  let offset = 0;
  const pageSize = 200;

  while (true) {
    const response = await fetchSameOrigin(
      req,
      '/api/artifacts?' + new URLSearchParams({ limit: String(pageSize), offset: String(offset) }),
    );
    const payload = await readJsonResponse(response, '成果列表读取失败');
    const page = Array.isArray(payload?.artifacts) ? payload.artifacts : [];
    all.push(...page);
    if (page.length < pageSize) break;
    offset += page.length;
    if (all.length > MAX_ARTIFACTS) {
      throw new Error('资产包最多包含 100 个成果');
    }
  }

  return all;
}

async function exportArtifact(req: NextRequest, artifactId: number) {
  const response = await fetchSameOrigin(
    req,
    '/api/artifacts/' + artifactId + '/export',
    { headers: { Accept: 'application/zip' } },
  );
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    let message = '成果导出失败';
    try {
      const payload = JSON.parse(text);
      message = String(payload?.error || payload?.detail || message);
    } catch {}
    throw new Error(message);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_ARTIFACT_BYTES) {
    throw new Error('单个成果超过统一资产包大小限制');
  }
  return bytes;
}

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  try {
    const [strategiesResponse, watchlistsResponse, artifacts] = await Promise.all([
      fetchSameOrigin(req, '/api/strategies/export'),
      fetchSameOrigin(req, '/api/watchlists/export'),
      collectArtifacts(req),
    ]);

    const strategiesPayload = stripInternalIds(
      await readJsonResponse(strategiesResponse, '策略导出失败'),
    );
    const watchlistsPayload = await readJsonResponse(watchlistsResponse, '自选股导出失败');

    const archive: Record<string, Uint8Array> = {
      'strategies.json': strToU8(JSON.stringify(strategiesPayload, null, 2)),
      'watchlists.json': strToU8(JSON.stringify(watchlistsPayload, null, 2)),
    };

    const manifestArtifacts: Array<{
      path: string;
      artifact_type: string;
      title: string;
      created_at: unknown;
    }> = [];

    for (let i = 0; i < artifacts.length; i += 1) {
      const source = artifacts[i];
      const artifactId = Number(source?.id ?? source?.artifact_id);
      if (!Number.isInteger(artifactId) || artifactId <= 0) {
        throw new Error('成果列表包含无效 ID');
      }

      const bytes = await exportArtifact(req, artifactId);
      const ordinal = String(i + 1).padStart(4, '0');
      const path = 'artifacts/' + ordinal + '.zip';
      archive[path] = bytes;
      manifestArtifacts.push({
        path,
        artifact_type: String(source?.artifact_type || source?.task_type || ''),
        title: String(source?.title || ''),
        created_at: source?.created_at ?? null,
      });
    }

    const manifest = {
      format: 'blinkquant-user-assets-v1',
      version: 1,
      exported_at: new Date().toISOString(),
      contents: ['strategies.json', 'watchlists.json', 'artifacts/'],
      artifact_count: manifestArtifacts.length,
      artifacts: manifestArtifacts,
    };
    archive['manifest.json'] = strToU8(JSON.stringify(manifest, null, 2));

    const bundle = zipSync(archive, { level: 6 });
    if (bundle.byteLength > MAX_BUNDLE_BYTES) {
      return NextResponse.json({ error: '统一资产包超过 128 MB 限制，请分别导出成果' }, { status: 413 });
    }

    return new NextResponse(bundle, {
      headers: {
        'Content-Type': 'application/zip',
        'Content-Disposition': `attachment; filename="blinkquant_user_assets_${new Date().toISOString().slice(0, 10)}.zip"`,
        'Cache-Control': 'no-store',
      },
    });
  } catch (error) {
    console.error('[me/assets/export] error:', error);
    return NextResponse.json({
      error: error instanceof Error ? error.message : '统一资产导出失败',
    }, { status: 500 });
  }
}

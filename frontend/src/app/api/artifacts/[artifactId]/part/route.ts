import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
export const runtime = 'nodejs';
const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';
const ALLOWED = new Set(['equity_curve', 'trades', 'positions_daily', 'result', 'selection_result', 'signal_trace']);
export async function GET(req: NextRequest, { params }: { params: Promise<{ artifactId: string }> }) {
  const auth = await requireAuth(req);
  if (auth.status !== 200 || !auth.user?.userId) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  const id = Number((await params).artifactId);
  if (!Number.isInteger(id) || id <= 0) return NextResponse.json({ error: 'Invalid artifact ID' }, { status: 400 });
  const sp = req.nextUrl.searchParams;
  const name = sp.get('name') || 'equity_curve';
  if (!ALLOWED.has(name)) return NextResponse.json({ error: 'Invalid artifact name' }, { status: 400 });

  const authQs = new URLSearchParams({ user_id: auth.user.userId });
  if (auth.user.role) authQs.set('role', auth.user.role);
  const artifactResponse = await fetch(NODE1_URL + '/internal/artifacts/' + id + '?' + authQs.toString(), {
    headers: { Authorization: 'Bearer ' + INTERNAL_TOKEN }, cache: 'no-store',
  });
  if (!artifactResponse.ok) {
    const text = await artifactResponse.text().catch(() => '');
    return NextResponse.json({ error: text || ('upstream ' + artifactResponse.status) }, { status: artifactResponse.status });
  }
  const artifact = await artifactResponse.json().catch(() => null);
  const taskId = Number(artifact?.task_id);
  if (!Number.isInteger(taskId) || taskId <= 0) {
    return NextResponse.json({ error: 'Artifact task ID unavailable' }, { status: 404 });
  }

  const qs = new URLSearchParams(sp);
  qs.set('name', name); qs.set('user_id', auth.user.userId);
  if (auth.user.role) qs.set('role', auth.user.role);
  const upstream = await fetch(NODE1_URL + '/internal/tasks/' + taskId + '/artifact?' + qs.toString(), {
    headers: { Authorization: 'Bearer ' + INTERNAL_TOKEN }, cache: 'no-store',
  });
  if (!upstream.ok) {
    const text = await upstream.text().catch(() => '');
    return NextResponse.json({ error: text || ('upstream ' + upstream.status) }, { status: upstream.status });
  }
  const ct = upstream.headers.get('Content-Type') || '';
  if (ct.includes('application/json')) return NextResponse.json(await upstream.json());
  if (!upstream.body) return NextResponse.json({ error: 'Artifact download stream unavailable' }, { status: 502 });

  const headers: Record<string, string> = {
    'Content-Type': ct || 'application/octet-stream',
    'Content-Disposition':
      upstream.headers.get('Content-Disposition') ||
      ('attachment; filename="artifact_' + id + '_' + name + '.parquet"'),
    'Cache-Control': 'no-store',
  };
  const contentLength = upstream.headers.get('Content-Length');
  if (contentLength) headers['Content-Length'] = contentLength;
  return new NextResponse(upstream.body, { status: 200, headers });
}

import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';

const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (auth.status !== 200 || !auth.user?.userId) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const contentType = req.headers.get('content-type');
  if (!contentType || !contentType.toLowerCase().includes('multipart/form-data')) {
    return NextResponse.json({ error: 'Artifact import requires a ZIP upload' }, { status: 400 });
  }

  const qs = new URLSearchParams({ user_id: auth.user.userId });
  if (auth.user.role) qs.set('role', auth.user.role);
  const bodyBytes = await req.arrayBuffer();
  const maxUploadBytes = 32 * 1024 * 1024;
  if (bodyBytes.byteLength > maxUploadBytes) {
    return NextResponse.json({ error: '成果导入 ZIP 超过 32 MB 限制' }, { status: 413 });
  }

  const upstream = await fetch(NODE1_URL + '/internal/artifacts/import?' + qs.toString(), {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + INTERNAL_TOKEN,
      'Content-Type': contentType,
      'Content-Length': String(bodyBytes.byteLength),
    },
    body: bodyBytes,
    cache: 'no-store',
  });

  const body = await upstream.arrayBuffer();
  return new NextResponse(body, {
    status: upstream.status,
    headers: { 'Content-Type': upstream.headers.get('Content-Type') || 'application/json', 'Cache-Control': 'no-store' },
  });
}

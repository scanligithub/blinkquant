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
  const upstream = await fetch(NODE1_URL + '/internal/artifacts/import?' + qs.toString(), {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + INTERNAL_TOKEN,
      'Content-Type': contentType,
    },
    body: req.body,
    // Node's fetch requires duplex for streaming request bodies.
    duplex: 'half',
    cache: 'no-store',
  } as RequestInit & { duplex: 'half' });

  const body = await upstream.arrayBuffer();
  return new NextResponse(body, {
    status: upstream.status,
    headers: { 'Content-Type': upstream.headers.get('Content-Type') || 'application/json', 'Cache-Control': 'no-store' },
  });
}

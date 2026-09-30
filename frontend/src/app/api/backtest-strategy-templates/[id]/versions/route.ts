import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: auth.status });
  }

  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return NextResponse.json({ error: 'invalid template id' }, { status: 400 });
  }

  const headers: Record<string, string> = { Accept: 'application/json' };
  const cookie = req.headers.get('cookie');
  if (cookie) headers.Cookie = cookie;

  const response = await fetch(
    new URL('/api/v1/backtest-strategy-templates/' + id + '/versions', BACKTEST_NODE),
    { headers, cache: 'no-store', signal: AbortSignal.timeout(10000) },
  );
  const text = await response.text();

  let payload: unknown;
  try {
    payload = JSON.parse(text);
  } catch {
    payload = { error: text.slice(0, 500) };
  }
  return NextResponse.json(payload, { status: response.status });
}

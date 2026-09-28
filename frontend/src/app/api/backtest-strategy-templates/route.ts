import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

async function auth(req: NextRequest) {
  return requireAuth(req);
}

async function forward(req: NextRequest, method: string) {
  const result = await auth(req);
  if (!result.user) return NextResponse.json({ error: 'Unauthorized' }, { status: result.status });

  const body = method === 'GET' || method === 'DELETE' ? undefined : await req.text();
  const url = new URL('/api/v1/backtest-strategy-templates', BACKTEST_NODE);
  if (method === 'PUT' || method === 'DELETE') {
    const id = req.nextUrl.searchParams.get('id');
    if (id) url.searchParams.set('id', id);
  }

  // Node1 re-validates the browser session through Vercel /api/auth/session.
  // Do not pass a client-controlled user id; the authenticated identity must
  // come from the same signed session cookie that requireAuth() accepted here.
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  const cookie = req.headers.get('cookie');
  if (cookie) headers.Cookie = cookie;

  const response = await fetch(url, {
    method,
    headers,
    body,
    signal: AbortSignal.timeout(10000),
  });
  const text = await response.text();
  let payload: unknown;
  try { payload = JSON.parse(text); } catch { payload = { error: text.slice(0, 500) }; }
  return NextResponse.json(payload, { status: response.status });
}

export async function GET(req: NextRequest) { return forward(req, 'GET'); }
export async function POST(req: NextRequest) { return forward(req, 'POST'); }
export async function PUT(req: NextRequest) { return forward(req, 'PUT'); }
export async function DELETE(req: NextRequest) { return forward(req, 'DELETE'); }

import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';
export const maxDuration = 60;

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

export async function POST(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const body = await req.text();
  const res = await fetch(`${BACKTEST_NODE}/api/v1/backtest/async`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
    signal: AbortSignal.timeout(15000),
  });
  const text = await res.text();

  if (!res.ok) {
    console.error('[backtest-async] Node1 failed:', text.slice(0, 500));
    return NextResponse.json(
      { error: 'Backtest node failed', detail: text.slice(0, 500) },
      { status: 502 }
    );
  }
  return NextResponse.json(JSON.parse(text));
}

export async function GET(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const jobId = req.nextUrl.searchParams.get('jobId');
  if (!jobId) {
    return NextResponse.json({ error: 'Missing jobId' }, { status: 400 });
  }

  const res = await fetch(`${BACKTEST_NODE}/api/v1/backtest/async/${jobId}`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(10000),
  });
  const text = await res.text();

  if (!res.ok) {
    return NextResponse.json(
      { error: 'Backtest node failed', detail: text.slice(0, 500) },
      { status: res.status }
    );
  }
  return NextResponse.json(JSON.parse(text));
}

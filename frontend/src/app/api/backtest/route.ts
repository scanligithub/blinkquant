import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';
export const maxDuration = 60;

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

export async function POST(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const body = await req.text();

  // 并行请求所有节点，取最快的成功响应
  const results = await Promise.allSettled(
    NODES.map((node) =>
      fetch(`${node}/api/v1/backtest/async`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: AbortSignal.timeout(15000),
      }).then(async (res) => {
        const text = await res.text();
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
        return JSON.parse(text);
      })
    )
  );

  const success = results.find((r) => r.status === 'fulfilled');
  if (success && success.status === 'fulfilled') {
    return NextResponse.json(success.value);
  }

  const errors = results.map((r, i) =>
    r.status === 'rejected' ? `${NODES[i]}: ${r.reason?.message || r.reason}` : 'ok'
  );
  console.error('[backtest-async] All nodes failed:', errors);
  return NextResponse.json({ error: 'All nodes failed' }, { status: 502 });
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

  // 并行轮询所有节点，取最快的成功响应
  const results = await Promise.allSettled(
    NODES.map((node) =>
      fetch(`${node}/api/v1/backtest/async/${jobId}`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(10000),
      }).then(async (res) => {
        const text = await res.text();
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
        return JSON.parse(text);
      })
    )
  );

  const success = results.find((r) => r.status === 'fulfilled');
  if (success && success.status === 'fulfilled') {
    return NextResponse.json(success.value);
  }

  return NextResponse.json({ error: 'All nodes failed' }, { status: 502 });
}

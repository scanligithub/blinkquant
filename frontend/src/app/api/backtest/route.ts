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

  for (const node of NODES) {
    try {
      const res = await fetch(`${node}/api/v1/backtest/async`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: AbortSignal.timeout(30000),
      });

      const text = await res.text();

      if (!res.ok) {
        console.error(`[backtest-async] ${node} returned ${res.status}: ${text.slice(0, 200)}`);
        continue;
      }

      try {
        const data = JSON.parse(text);
        return NextResponse.json(data);
      } catch {
        console.error(`[backtest-async] ${node} returned non-JSON: ${text.slice(0, 200)}`);
        continue;
      }
    } catch (e: any) {
      console.error(`[backtest-async] ${node} error: ${e.message}`);
      continue;
    }
  }

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

  for (const node of NODES) {
    try {
      const res = await fetch(`${node}/api/v1/backtest/async/${jobId}`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(15000),
      });

      const text = await res.text();

      if (!res.ok) {
        console.error(`[backtest-poll] ${node} returned ${res.status}: ${text.slice(0, 200)}`);
        continue;
      }

      try {
        const data = JSON.parse(text);
        return NextResponse.json(data);
      } catch {
        console.error(`[backtest-poll] ${node} returned non-JSON: ${text.slice(0, 200)}`);
        continue;
      }
    } catch (e: any) {
      console.error(`[backtest-poll] ${node} error: ${e.message}`);
      continue;
    }
  }

  return NextResponse.json({ error: 'All nodes failed' }, { status: 502 });
}
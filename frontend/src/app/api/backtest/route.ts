import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';
export const maxDuration = 60;

// v1: force deploy
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
      const res = await fetch(`${node}/api/v1/backtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: AbortSignal.timeout(55000),
      });

      const text = await res.text();

      if (!res.ok) {
        console.error(`[backtest] ${node} returned ${res.status}: ${text.slice(0, 200)}`);
        continue;
      }

      try {
        const data = JSON.parse(text);
        return NextResponse.json(data);
      } catch {
        console.error(`[backtest] ${node} returned non-JSON: ${text.slice(0, 200)}`);
        continue;
      }
    } catch (e: any) {
      console.error(`[backtest] ${node} error: ${e.message}`);
      continue;
    }
  }

  return NextResponse.json({ error: 'All nodes failed' }, { status: 502 });
}
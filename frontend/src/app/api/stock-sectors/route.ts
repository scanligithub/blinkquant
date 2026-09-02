import { NextResponse, NextRequest } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space'
];

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  if (!code) {
    return NextResponse.json({ error: 'Stock code is required' }, { status: 400 });
  }

  try {
    const result = await Promise.any(
      NODES.map(async (nodeUrl) => {
        const res = await fetch(`${nodeUrl}/api/v1/stock-sectors?code=${encodeURIComponent(code)}`, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) throw new Error(`Node responded with ${res.status}`);
        return res.json();
      })
    );
    return NextResponse.json(result, { headers: { 'Cache-Control': 'no-store' } });
  } catch (error) {
    console.error('Failed to fetch stock sectors:', error);
    return NextResponse.json({ error: 'Failed to fetch stock sectors' }, { status: 503 });
  }
}

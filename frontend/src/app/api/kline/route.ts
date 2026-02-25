import { NextResponse, NextRequest } from 'next/server';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space'
];

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';

  if (!code) {
    return NextResponse.json({ error: 'Stock code is required' }, { status: 400 });
  }

  try {
    const result = await Promise.any(
      NODES.map(async (nodeUrl, i) => {
        const url = `${nodeUrl}/api/v1/kline?code=${code}&timeframe=${timeframe}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });

        if (!res.ok) throw new Error('Not on this node');

        const json = await res.json();
        if (!json.data || (Array.isArray(json.data) && json.data.length === 0)) {
            throw new Error('Empty data');
        }
        return json;
      })
    );
    return NextResponse.json(result, {
      headers: {
        'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=60'
      }
    });

  } catch (error: any) {
    console.error("Error fetching kline data:", error);
    if (error instanceof AggregateError) {
        return NextResponse.json({ error: 'Stock not found in cluster' }, { status: 404 });
    }
    return NextResponse.json({ error: 'Failed to fetch kline data' }, { status: 500 });
  }
}

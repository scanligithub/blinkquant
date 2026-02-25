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
    return new NextResponse(JSON.stringify({ error: 'Stock code is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  }

  try {
    const resultBuffer = await Promise.any(
      NODES.map(async (nodeUrl, i) => {
        const url = `${nodeUrl}/api/v1/kline?code=${code}&timeframe=${timeframe}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });

        if (!res.ok) {
          const errorText = await res.text();
          console.error(`Backend node ${nodeUrl} responded with status ${res.status}: ${errorText}`);
          throw new Error(`Not on this node or backend error: ${errorText}`);
        }

        const arrayBuffer = await res.arrayBuffer();
        if (!arrayBuffer || arrayBuffer.byteLength < 100) { // Parquet files have a magic number PAR1 at start, min size
            throw new Error('Empty or invalid Parquet data received');
        }
        return arrayBuffer;
      })
    );

    return new NextResponse(resultBuffer, {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=60'
      }
    });

  } catch (error: any) {
    console.error("Error fetching kline data:", error);
    if (error instanceof AggregateError) {
        return new NextResponse(JSON.stringify({ error: 'Stock not found in cluster or data unavailable' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
    }
    return new NextResponse(JSON.stringify({ error: 'Failed to fetch kline data' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}

import { NextResponse } from 'next/server';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/stock-list',
  'https://scanli-blinkquant-node2.hf.space/api/v1/stock-list',
  'https://scanli-blinkquant-node3.hf.space/api/v1/stock-list'
];

export async function GET() {
  try {
    // 并发请求所有节点，使用 Promise.any 获取最快响应
    const result = await Promise.any(
      NODES.map(url =>
        fetch(url, { signal: AbortSignal.timeout(10000) })
          .then(res => {
            if (!res.ok) throw new Error(`Node responded with ${res.status}`);
            return res.json();
          })
      )
    );

    return NextResponse.json(result, {
      headers: {
        'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=300'
      }
    });
  } catch (error) {
    console.error('Failed to fetch stock list:', error);
    return NextResponse.json({ error: 'Failed to fetch stock list' }, { status: 503 });
  }
}

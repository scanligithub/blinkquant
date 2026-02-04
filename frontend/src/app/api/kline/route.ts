import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';

const NODE_URLS = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/kline',
  'https://scanli-blinkquant-node2.hf.space/api/v1/kline',
  'https://scanli-blinkquant-node3.hf.space/api/v1/kline'
];

// 必须与 Python 端的简单 hash 逻辑匹配 (或者直接通过 Promise.any 并发尝试)
export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';

  if (!code) return NextResponse.json({ error: 'Code is required' }, { status: 400 });

  // 策略：并发请求所有节点，谁先返回 200 就用谁 (最简单有效的分布式路由)
  try {
    const fetchPromises = NODE_URLS.map(url => 
      fetch(`${url}?code=${code}&timeframe=${timeframe}`, { signal: AbortSignal.timeout(5000) })
        .then(res => {
          if (res.ok) return res.json();
          throw new Error('Not found');
        })
    );

    const data = await Promise.any(fetchPromises);
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: 'Stock not found across cluster' }, { status: 404 });
  }
}

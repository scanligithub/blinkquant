import { NextResponse } from 'next/server';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/status',
  'https://scanli-blinkquant-node2.hf.space/api/v1/status',
  'https://scanli-blinkquant-node3.hf.space/api/v1/status'
];

export async function GET() {
  try {
    // 并发请求所有节点状态
    const statusPromises = NODES.map(async (url, index) => {
      try {
        const res = await fetch(url, { signal: AbortSignal.timeout(3000) });
        if (!res.ok) throw new Error('Down');
        const data = await res.json();
        return { 
            id: index, 
            online: true, 
            ...data 
        };
      } catch (err) {
        return { 
            id: index, 
            online: false, 
            node: String(index), 
            status: 'offline',
            error: 'Timeout/Down' 
        };
      }
    });

    const results = await Promise.all(statusPromises);

    // 计算汇总数据（用于缓存头或简略信息）
    const healthyCount = results.filter(n => n.online).length;

    return NextResponse.json({
      timestamp: new Date().toISOString(),
      cluster_health: `${healthyCount}/${NODES.length}`,
      nodes: results
    }, {
      headers: {
        // 设置 5秒 的短缓存，避免频繁刷新导致前端卡顿
        'Cache-Control': 'public, s-maxage=5, stale-while-revalidate=5'
      }
    });

  } catch (err) {
    return NextResponse.json({ error: 'Monitor Error' }, { status: 500 });
  }
}

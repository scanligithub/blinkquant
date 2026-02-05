import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/kline',
  'https://scanli-blinkquant-node2.hf.space/api/v1/kline',
  'https://scanli-blinkquant-node3.hf.space/api/v1/kline'
];

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';

  if (!code) return NextResponse.json({ error: 'Code is required' }, { status: 400 });

  try {
    // 1. 竞速路由 (Smart Routing)
    // 同时询问三个节点，谁先返回 200 OK 且包含数据，就采用谁的结果
    const data = await Promise.any(
      NODES.map(async (url) => {
        const res = await fetch(`${url}?code=${code}&timeframe=${timeframe}`, {
           // 设置 5秒 超时，快速失败
           signal: AbortSignal.timeout(5000) 
        });
        
        if (!res.ok) throw new Error('Not on this node');
        
        const json = await res.json();
        // 确保数据有效
        if (!json.data || (Array.isArray(json.data) && json.data.length === 0)) {
            throw new Error('Empty data');
        }
        return json;
      })
    );

    // 2. 添加 CDN 缓存头 (Cache-Control)
    // s-maxage=3600: 在 Vercel 边缘节点缓存 1 小时
    // stale-while-revalidate=60: 允许短暂返回旧数据以保持极致速度
    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=60'
      }
    });

  } catch (err) {
    // 所有节点都失败（Promise.any 抛出 AggregateError）
    return NextResponse.json({ error: 'Stock not found in cluster' }, { status: 404 });
  }
}

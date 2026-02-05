import { NextRequest, NextResponse } from 'next/server';

// 启用 Edge Runtime 以获得最低延迟和最高并发性能
export const runtime = 'edge';

// 后端节点地址池
const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { formula, timeframe } = body;

    if (!formula) {
      return NextResponse.json({ error: 'Formula is required' }, { status: 400 });
    }

    // 1. 并发请求所有算力节点
    const promises = NODES.map(url => 
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        // 设置 30秒超时，防止网关被长尾请求挂起
        signal: AbortSignal.timeout(30000)
      })
      .then(async res => {
        if (!res.ok) return { results: [] };
        return res.json();
      })
      .catch(err => {
        console.error(`Node failure ${url}:`, err);
        return { results: [] }; // 容错：单个节点失败不影响整体
      })
    );

    const responses = await Promise.all(promises);

    // 2. 聚合结果
    const allCodes = responses.flatMap(r => r.results || []);
    
    // 3. 去重与排序
    const uniqueCodes = Array.from(new Set(allCodes)).sort();

    // 4. 生成元数据
    const meta = {
        total_hits: uniqueCodes.length,
        nodes_responding: responses.filter(r => r.results !== undefined).length
    };

    return NextResponse.json({ 
        success: true, 
        data: uniqueCodes,
        meta 
    });

  } catch (err) {
    return NextResponse.json({ error: 'Gateway Internal Error' }, { status: 500 });
  }
}

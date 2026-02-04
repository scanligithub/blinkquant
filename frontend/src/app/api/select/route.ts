import { NextRequest, NextResponse } from 'next/server';

// 强制使用 Edge Runtime，获得最高并发性能和最低延迟
export const runtime = 'edge';

// 定义 3 个算力节点的原生地址 (HF 内部地址最稳定)
const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

export async function POST(req: NextRequest) {
  try {
    const { formula, timeframe } = await req.json();

    if (!formula) {
      return NextResponse.json({ error: 'Formula is required' }, { status: 400 });
    }

    console.log(`Aggregating selection for: ${formula} [${timeframe}]`);

    // 1. 并发请求 3 个节点 (Promise.all)
    const requests = NODES.map(nodeUrl => 
      fetch(nodeUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        // 设置 10 秒超时，防止某节点挂掉拖累全局
        signal: AbortSignal.timeout(10000) 
      }).then(res => {
        if (!res.ok) throw new Error(`Node ${nodeUrl} error: ${res.status}`);
        return res.json();
      })
    );

    // 2. 等待所有节点返回结果
    const responses = await Promise.allSettled(requests);

    let globalResults: string[] = [];
    let errors: string[] = [];
    let nodesOnline = 0;

    // 3. 聚合结果集
    responses.forEach((res, index) => {
      if (res.status === 'fulfilled') {
        globalResults.push(...(res.value.results || []));
        nodesOnline++;
      } else {
        errors.push(`Node ${index} failed: ${res.reason}`);
      }
    });

    // 4. 去重并排序 (尽管分片已理论去重，但合并时保持有序是好习惯)
    const finalResults = Array.from(new Set(globalResults)).sort();

    // 5. 返回全局结果
    return NextResponse.json({
      success: true,
      count: finalResults.length,
      data: finalResults,
      meta: {
        nodes_responding: nodesOnline,
        total_nodes: NODES.length,
        errors: errors.length > 0 ? errors : undefined
      }
    });

  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

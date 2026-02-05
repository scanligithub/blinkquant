import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@vercel/postgres';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

/**
 * 从公式中提取指标特征
 * 例子: "CLOSE > MA(CLOSE, 250) & ROC(CLOSE, 12) > 5" 
 * 提取结果: ["MA_CLOSE_250", "ROC_CLOSE_12"]
 */
function extractMetrics(formula: string): string[] {
  const metrics = new Set<string>();
  // 匹配模式: 算子(字段, 参数) -> 如 MA(CLOSE, 20)
  const regex = /(\bMA\b|\bEMA\b|\bSTD\b|\bROC\b)\s*\(\s*(\bCLOSE\b|\bOPEN\b|\bHIGH\b|\bLOW\b|\bVOL\b|\bS_CLOSE\b)\s*,\s*(\d+)\s*\)/gi;
  
  let match;
  while ((match = regex.exec(formula)) !== null) {
    const func = match[1].toUpperCase();
    const field = match[2].toUpperCase();
    const param = match[3];
    metrics.add(`${func}_${field}_${param}`);
  }
  return Array.from(metrics);
}

/**
 * 异步更新指标热度统计 (Fire and Forget)
 */
async function trackMetricUsage(formula: string) {
  const keys = extractMetrics(formula);
  if (keys.length === 0) return;

  try {
    for (const key of keys) {
      // 执行原子化 UPSERT: 存在则增加计数，不存在则插入
      await sql`
        INSERT INTO metrics_stats (metric_key, usage_count, last_used)
        VALUES (${key}, 1, NOW())
        ON CONFLICT (metric_key)
        DO UPDATE SET 
          usage_count = metrics_stats.usage_count + 1,
          last_used = NOW();
      `;
    }
    console.log(`Self-Evolution: Tracked ${keys.length} metrics from formula.`);
  } catch (err) {
    console.error('Database tracking failed:', err);
  }
}

export async function POST(req: NextRequest) {
  try {
    const { formula, timeframe } = await req.json();

    if (!formula) {
      return NextResponse.json({ error: 'Formula is required' }, { status: 400 });
    }

    // --- 核心逻辑 A: 分布式选股计算 (并行请求 3 个节点) ---
    const requests = NODES.map(nodeUrl => 
      fetch(nodeUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        signal: AbortSignal.timeout(10000) 
      }).then(res => {
        if (!res.ok) throw new Error(`Node error: ${res.status}`);
        return res.json();
      })
    );

    // --- 核心逻辑 B: 异步热度统计 (不阻塞选股结果返回) ---
    // 在 Edge Runtime 中，我们不 await trackMetricUsage，让它在后台运行
    // 这样用户可以第一时间拿到选股结果，而数据库统计在毫秒后悄悄完成
    const trackingTask = trackMetricUsage(formula);

    // --- 核心逻辑 C: 聚合结果 ---
    const responses = await Promise.allSettled(requests);

    let globalResults: string[] = [];
    let nodesOnline = 0;

    responses.forEach((res) => {
      if (res.status === 'fulfilled') {
        globalResults.push(...(res.value.results || []));
        nodesOnline++;
      }
    });

    const finalResults = Array.from(new Set(globalResults)).sort();

    // 确保异步统计任务在边缘函数生命周期内完成 (Vercel 特有优化)
    // 如果是标准 Edge 环境，建议使用 event.waitUntil(trackingTask)
    // 在 Next.js Route Handlers 中，不 await 也会在微任务队列执行
    
    return NextResponse.json({
      success: true,
      count: finalResults.length,
      data: finalResults,
      meta: {
        nodes_responding: nodesOnline,
        total_nodes: NODES.length
      }
    });

  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

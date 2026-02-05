import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@vercel/postgres';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

/**
 * 核心：公式特征提取器
 * 逻辑：扫描公式中的指标名、字段名和参数，生成标准化 Key (例如 MA_CLOSE_199)
 */
function extractMetrics(formula: string): string[] {
  const metrics = new Set<string>();
  // 更加鲁棒的正则：支持不同空格排版，不区分大小写
  const regex = /(\bMA\b|\bEMA\b|\bSTD\b|\bROC\b)\s*\(\s*(\bCLOSE\b|\bOPEN\b|\bHIGH\b|\bLOW\b|\bVOL\b|\bAMOUNT\b|\bS_CLOSE\b|\bS_PCT_CHG\b)\s*,\s*(\d+)\s*\)/gi;
  
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
 * 核心：异步数据库计数任务
 * 确保即使选股失败，只要公式合法，热度依然会被记录
 */
async function trackMetricUsage(keys: string[]) {
  if (keys.length === 0) return;

  try {
    // 串行或并行执行 Upsert 操作
    const tasks = keys.map(key => sql`
      INSERT INTO metrics_stats (metric_key, usage_count, last_used)
      VALUES (${key}, 1, NOW())
      ON CONFLICT (metric_key)
      DO UPDATE SET 
        usage_count = metrics_stats.usage_count + 1,
        last_used = NOW();
    `);
    await Promise.all(tasks);
    console.log(`Blink-Evolution: Successfully tracked [${keys.join(', ')}]`);
  } catch (err) {
    console.error('Blink-Evolution Error: Database write failed.', err);
  }
}

export async function POST(req: NextRequest) {
  try {
    const { formula, timeframe } = await req.json();

    if (!formula) {
      return NextResponse.json({ error: 'Formula is required' }, { status: 400 });
    }

    // 1. 提取指标特征
    const metricKeys = extractMetrics(formula);

    // 2. 准备并发任务队列
    // A. 算力集群请求
    const nodeRequests = NODES.map(nodeUrl => 
      fetch(nodeUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        signal: AbortSignal.timeout(15000) // 15秒超时，给现算逻辑留足时间
      }).then(res => {
        if (!res.ok) throw new Error(`Node ${nodeUrl} returned ${res.status}`);
        return res.json();
      })
    );

    // B. 数据库计数任务 (必须在这里产生 Promise)
    const dbTask = trackMetricUsage(metricKeys);

    // 3. 并行执行：选股与计数同步进行
    // 我们 await dbTask 以确保在 Edge 函数结束前写入完成
    const [responses] = await Promise.all([
      Promise.allSettled(nodeRequests),
      dbTask 
    ]);

    // 4. 聚合分布式结果
    let globalResults: string[] = [];
    let nodesOnline = 0;
    let nodeErrors: string[] = [];

    responses.forEach((res, idx) => {
      if (res.status === 'fulfilled') {
        globalResults.push(...(res.value.results || []));
        nodesOnline++;
      } else {
        nodeErrors.push(`Node ${idx} Error: ${res.reason}`);
      }
    });

    // 5. 数据清洗：去重、排序
    const finalResults = Array.from(new Set(globalResults)).sort();

    // 6. 返回最终响应
    return NextResponse.json({
      success: true,
      count: finalResults.length,
      data: finalResults,
      meta: {
        nodes_responding: nodesOnline,
        total_nodes: NODES.length,
        metrics_captured: metricKeys,
        errors: nodeErrors.length > 0 ? nodeErrors : undefined
      }
    });

  } catch (err: any) {
    console.error('Gateway Error:', err.message);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

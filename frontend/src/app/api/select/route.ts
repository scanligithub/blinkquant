import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@vercel/postgres';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

/**
 * 强化版：公式特征提取
 * 能够识别：MA(CLOSE,199), MA ( CLOSE , 199 ), ma(close,199) 等各种变体
 */
function extractMetrics(formula: string): string[] {
  const metrics = new Set<string>();
  // 更加宽松的正则，专注于抓取 (函数名, 字段名, 数字)
  const regex = /(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT|S_CLOSE|S_PCT_CHG)\s*,\s*(\d+)\s*\)/gi;
  
  let match;
  // 每次运行前重置正则索引，防止状态残留
  regex.lastIndex = 0; 
  
  while ((match = regex.exec(formula)) !== null) {
    const func = match[1].toUpperCase();
    const field = match[2].toUpperCase();
    const param = match[3];
    metrics.add(`${func}_${field}_${param}`);
  }
  
  const results = Array.from(metrics);
  console.log('DEBUG: Extracted Metric Keys ->', results); // 在 Vercel Logs 中查看
  return results;
}

export async function POST(req: NextRequest) {
  try {
    const { formula, timeframe } = await req.json();

    if (!formula) return NextResponse.json({ error: 'Formula is required' }, { status: 400 });

    const metricKeys = extractMetrics(formula);

    // 1. 发起选股请求
    const nodeRequests = NODES.map(nodeUrl => 
      fetch(nodeUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        signal: AbortSignal.timeout(12000) 
      }).then(res => res.json())
    );

    // 2. 发起数据库写入请求
    // 我们将其包装成一个 Promise，确保它能被 await
    const dbTask = (async () => {
      if (metricKeys.length === 0) return;
      try {
        for (const key of metricKeys) {
          await sql`
            INSERT INTO metrics_stats (metric_key, usage_count, last_used)
            VALUES (${key}, 1, NOW())
            ON CONFLICT (metric_key)
            DO UPDATE SET 
              usage_count = metrics_stats.usage_count + 1,
              last_used = NOW();
          `;
        }
        console.log('DEBUG: DB Update Success for', metricKeys);
      } catch (dbErr: any) {
        console.error('DEBUG: DB Update Failed ->', dbErr.message);
      }
    })();

    // 3. 同时等待：选股结果 + 数据库写入
    // 只有当两者都（尝试）完成后，才返回响应，防止 Edge 函数过早退出
    const [responses] = await Promise.all([
      Promise.allSettled(nodeRequests),
      dbTask
    ]);

    // 4. 聚合逻辑
    let globalResults: string[] = [];
    let nodesOnline = 0;
    responses.forEach((res: any) => {
      if (res.status === 'fulfilled' && res.value.results) {
        globalResults.push(...res.value.results);
        nodesOnline++;
      }
    });

    const finalResults = Array.from(new Set(globalResults)).sort();

    return NextResponse.json({
      success: true,
      count: finalResults.length,
      data: finalResults,
      meta: {
        nodes_responding: nodesOnline,
        metrics_found: metricKeys // 返回给前端，方便观察正则是否抓到了
      }
    });

  } catch (err: any) {
    console.error('Gateway Error:', err.message);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}

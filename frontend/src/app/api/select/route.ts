import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@vercel/postgres';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

function extractMetrics(formula: string): string[] {
  const metrics = new Set<string>();
  // 这里的正则极其重要：
  const regex = /(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT|S_CLOSE|S_PCT_CHG)\s*,\s*(\d+)\s*\)/gi;
  let match;
  regex.lastIndex = 0; 
  while ((match = regex.exec(formula)) !== null) {
    metrics.add(`${match[1].toUpperCase()}_${match[2].toUpperCase()}_${match[3]}`);
  }
  return Array.from(metrics);
}

export async function POST(req: NextRequest) {
  let debugMetrics: string[] = [];
  try {
    const { formula, timeframe } = await req.json();
    if (!formula) return NextResponse.json({ error: 'No formula' }, { status: 400 });

    // 1. 抓取指标
    debugMetrics = extractMetrics(formula);

    // 2. 数据库任务
    const dbTask = (async () => {
      if (debugMetrics.length === 0) return "No metrics found";
      try {
        for (const key of debugMetrics) {
          await sql`
            INSERT INTO metrics_stats (metric_key, usage_count, last_used)
            VALUES (${key}, 1, NOW())
            ON CONFLICT (metric_key)
            DO UPDATE SET usage_count = metrics_stats.usage_count + 1, last_used = NOW();
          `;
        }
        return "DB Update OK";
      } catch (e: any) {
        return `DB Error: ${e.message}`;
      }
    })();

    // 3. 选股任务
    const nodeRequests = NODES.map(url => 
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe }),
        signal: AbortSignal.timeout(12000)
      }).then(res => res.json())
    );

    // 4. 强制等待两者完成 (Edge 生命周期保护)
    const [responses, dbStatus] = await Promise.all([
      Promise.allSettled(nodeRequests),
      dbTask
    ]);

    // 5. 聚合结果
    let globalResults: string[] = [];
    responses.forEach((res: any) => {
      if (res.status === 'fulfilled' && res.value.results) {
        globalResults.push(...res.value.results);
      }
    });

    // 6. 返回结果，同时带上调试信息
    return NextResponse.json({
      success: true,
      count: new Set(globalResults).size,
      data: Array.from(new Set(globalResults)).sort(),
      debug: {
        captured_keys: debugMetrics, // 如果这里是空的，说明正则没配上
        database_status: dbStatus   // 这里会告诉你数据库写入的结果
      }
    });

  } catch (err: any) {
    return NextResponse.json({ error: err.message, keys: debugMetrics }, { status: 500 });
  }
}

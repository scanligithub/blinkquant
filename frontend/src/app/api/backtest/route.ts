import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';
export const maxDuration = 60;

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

export async function POST(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const body = await req.text();

  // 启动后台任务，立即返回 jobId
  const jobId = crypto.randomUUID();
  
  // 使用 setTimeout 模拟后台任务（实际应用中可用队列）
  setImmediate(async () => {
    for (const node of NODES) {
      try {
        const res = await fetch(`${node}/api/v1/backtest`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body,
          signal: AbortSignal.timeout(280000),
        });

        const text = await res.text();

        if (!res.ok) {
          console.error(`[backtest] ${node} returned ${res.status}: ${text.slice(0, 200)}`);
          continue;
        }

        try {
          const data = JSON.parse(text);
          // 存储结果供轮询获取（生产环境用 Redis/数据库，这里用内存 map）
          if (!globalThis.__backtestResults) {
            globalThis.__backtestResults = new Map();
          }
          globalThis.__backtestResults.set(jobId, { status: 'done', data });
          return;
        } catch {
          console.error(`[backtest] ${node} returned non-JSON: ${text.slice(0, 200)}`);
          continue;
        }
      } catch (e: any) {
        console.error(`[backtest] ${node} error: ${e.message}`);
        continue;
      }
    }
    // 所有节点失败
    if (!globalThis.__backtestResults) {
      globalThis.__backtestResults = new Map();
    }
    globalThis.__backtestResults.set(jobId, { status: 'error', error: 'All nodes failed' });
  });

  return NextResponse.json({ jobId, status: 'pending' });
}

export async function GET(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const jobId = req.nextUrl.searchParams.get('jobId');
  if (!jobId) {
    return NextResponse.json({ error: 'Missing jobId' }, { status: 400 });
  }

  if (!globalThis.__backtestResults) {
    return NextResponse.json({ status: 'not_found' }, { status: 404 });
  }

  const result = globalThis.__backtestResults.get(jobId);
  if (!result) {
    return NextResponse.json({ status: 'not_found' }, { status: 404 });
  }

  return NextResponse.json(result);
}
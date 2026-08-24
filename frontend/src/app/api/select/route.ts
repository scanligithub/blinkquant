import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

// 启用 Edge Runtime 以获得最低延迟和最高并发性能
export const runtime = 'edge';

// 后端节点地址池
const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/select',
  'https://scanli-blinkquant-node2.hf.space/api/v1/select',
  'https://scanli-blinkquant-node3.hf.space/api/v1/select'
];

interface NodeOutcome {
  node: number;
  ok: boolean;
  count: number;
  error: string | null;
  date: string | null;
  results: string[];
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  try {
    const body = await req.json();
    const { formula, timeframe, date } = body;

    if (!formula) {
      return NextResponse.json({ error: 'Formula is required' }, { status: 400 });
    }

    // 1. 并发请求所有算力节点（date 可选：指定交易日，后端回退到最近交易日）
    const promises = NODES.map((url, i): Promise<NodeOutcome> =>
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe, ...(date ? { date } : {}) }),
        // 设置 30秒超时，防止网关被长尾请求挂起
        signal: AbortSignal.timeout(30000)
      })
      .then(async (res): Promise<NodeOutcome> => {
        if (!res.ok) return { node: i + 1, ok: false, count: 0, error: `HTTP ${res.status}`, date: null, results: [] };
        const json = await res.json();
        return {
          node: i + 1,
          ok: true,
          count: typeof json.count === 'number' ? json.count : (json.results?.length ?? 0),
          error: null,
          date: typeof json.date === 'string' ? json.date : null,
          results: Array.isArray(json.results) ? json.results : [],
        };
      })
      .catch((err): NodeOutcome => {
        console.error(`Node failure ${url}:`, err);
        // 容错：单个节点失败不阻塞整体，但必须向上暴露（degraded），避免静默丢数据被误读为"涨停数量少"
        const reason = err?.name === 'TimeoutError' || err?.name === 'AbortError' ? 'timeout' : String(err?.message || err);
        return { node: i + 1, ok: false, count: 0, error: reason, date: null, results: [] };
      })
    );

    const responses = await Promise.all(promises);

    // 2. 聚合结果
    const allCodes = responses.flatMap(r => r.results || []);

    // 3. 去重与排序
    const uniqueCodes = Array.from(new Set(allCodes)).sort();

    // 4. 生成元数据：逐节点状态 + 整体降级标志（任一节点失败即 true）
    const failed = responses.filter(r => !r.ok);
    const meta = {
        total_hits: uniqueCodes.length,
        nodes_responding: responses.length - failed.length,
        degraded: failed.length > 0,
        nodes: responses.map(({ node, ok, count, error }) => ({ node, ok, count, error })),
    };

    // 各节点分片互斥且同数据集，生效交易日取任一成功节点的返回
    const effectiveDate = responses.find(r => r.ok && r.date)?.date ?? null;

    return NextResponse.json({
        success: true,
        data: uniqueCodes,
        date: effectiveDate,
        meta
    });

  } catch (err) {
    return NextResponse.json({ error: 'Gateway Internal Error' }, { status: 500 });
  }
}

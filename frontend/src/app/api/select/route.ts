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
  /** 节点返回 4xx 时的响应体摘要（如 FastAPI 的 detail），用于确定性错误透传 */
  detail: string | null;
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
        if (!res.ok) {
          const text = await res.text().catch(() => '');
          return {
            node: i + 1,
            ok: false,
            count: 0,
            error: `HTTP ${res.status}`,
            detail: text.slice(0, 300),
            date: null,
            results: [],
          };
        }
        const json = await res.json();
        const codes = Array.isArray(json.codes) ? json.codes : (Array.isArray(json.results) ? json.results : []);
        const signalDate = typeof json.signal_date === 'string' ? json.signal_date : (typeof json.date === 'string' ? json.date : null);
        return {
          node: i + 1,
          ok: true,
          count: codes.length,
          error: null,
          detail: null,
          date: signalDate,
          results: codes,
        };
      })
      .catch((err): NodeOutcome => {
        console.error(`Node failure ${url}:`, err);
        // 容错：单个节点失败不阻塞整体，但必须向上暴露（degraded），避免静默丢数据被误读为"涨停数量少"
        const reason = err?.name === 'TimeoutError' || err?.name === 'AbortError' ? 'timeout' : String(err?.message || err);
        return { node: i + 1, ok: false, count: 0, error: reason, detail: null, date: null, results: [] };
      })
    );

    const responses = await Promise.all(promises);

    // 1.5 确定性错误透传：全部节点一致返回 4xx 说明是请求本身非法（公式错误/日期越界），
    // 而非节点故障——此时不应降级为空结果，应把后端的错误说明原样给到前端。
    if (responses.every(r => !r.ok && r.error !== null && /^HTTP 4/.test(r.error))) {
      let message = '请求被计算节点拒绝';
      try {
        message = JSON.parse(responses[0].detail || '{}').detail || message;
      } catch { /* 非 JSON 响应体则用默认文案 */ }
      return NextResponse.json(
        { success: false, error: String(message), meta: { degraded: true } },
        { status: responses[0].error === 'HTTP 400' ? 400 : 422 }
      );
    }

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

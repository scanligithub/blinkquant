import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const cookie = req.headers.get('cookie') || '';
  const [selectionResult, backtestResponse] = await Promise.all([
    node1Json(node1Path('/user-assets/strategies/export', { user_id: auth.user.userId })),
    fetch(new URL('/api/v1/backtest-strategy-templates/export', BACKTEST_NODE), {
      headers: { Accept: 'application/json', Cookie: cookie },
      cache: 'no-store',
      signal: AbortSignal.timeout(10000),
    }),
  ]);

  if (!selectionResult.response.ok) {
    return NextResponse.json(
      { error: String(selectionResult.data?.detail || selectionResult.data?.error || '选股策略导出失败') },
      { status: selectionResult.response.status },
    );
  }
  if (!backtestResponse.ok) {
    const text = await backtestResponse.text().catch(() => '');
    let message = '回测策略导出失败';
    try { const payload = JSON.parse(text); message = String(payload?.detail || payload?.error || message); } catch {}
    return NextResponse.json({ error: message }, { status: backtestResponse.status === 401 ? 401 : 503 });
  }

  const backtestPayload = await backtestResponse.json();
  const payload = {
    format: 'blinkquant-strategy-assets-v1',
    exported_at: new Date().toISOString(),
    strategies: Array.isArray(selectionResult.data?.strategies) ? selectionResult.data.strategies : [],
    backtest_strategies: Array.isArray(backtestPayload?.templates) ? backtestPayload.templates : [],
  };

  return new NextResponse(JSON.stringify(payload, null, 2), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="blinkquant_strategy_assets_${new Date().toISOString().slice(0, 10)}.json"`,
    },
  });
}

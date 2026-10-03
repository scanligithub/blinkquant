import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json } from '@/lib/node1Internal';

export const runtime = 'edge';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  let body: any;
  try { body = await req.json(); } catch {
    return NextResponse.json({ error: '导入文件格式无效' }, { status: 400 });
  }

  const selectionItems = Array.isArray(body?.strategies) ? body.strategies : [];
  const backtestItems = Array.isArray(body?.backtest_strategies) ? body.backtest_strategies : [];
  if (selectionItems.length === 0 && backtestItems.length === 0) {
    return NextResponse.json({ error: '没有可导入的策略' }, { status: 400 });
  }
  if (selectionItems.length > 100 || backtestItems.length > 100) {
    return NextResponse.json({ error: '单次每种类型最多导入 100 个策略' }, { status: 400 });
  }

  const selectionResult = await node1Json('/user-assets/strategies/import', {
    method: 'POST',
    body: JSON.stringify({ user_id: auth.user.userId, strategies: selectionItems }),
  });
  if (!selectionResult.response.ok) {
    return NextResponse.json({
      error: String(selectionResult.data?.detail || selectionResult.data?.error || '选股策略导入失败'),
      imported: Number(selectionResult.data?.imported || 0),
      imported_backtests: 0,
      skipped: Array.isArray(selectionResult.data?.skipped) ? selectionResult.data.skipped : [],
      skipped_backtests: backtestItems.map((item: any) => String(item?.name || '(未命名)')),
    }, { status: selectionResult.response.status });
  }

  let importedBacktests = 0;
  let skippedBacktests: string[] = [];
  if (backtestItems.length > 0) {
    const response = await fetch(new URL('/api/v1/backtest-strategy-templates/import', BACKTEST_NODE), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', Cookie: req.headers.get('cookie') || '' },
      body: JSON.stringify({ templates: backtestItems }),
      cache: 'no-store',
      signal: AbortSignal.timeout(10000),
    });
    const text = await response.text();
    let payload: any = {};
    try { payload = JSON.parse(text); } catch {}
    if (!response.ok) {
      return NextResponse.json({
        error: String(payload?.detail || payload?.error || '回测策略导入失败'),
        imported: selectionResult.data.imported,
        imported_backtests: 0,
        skipped: selectionResult.data.skipped || [],
        skipped_backtests: backtestItems.map((item: any) => String(item?.name || '(未命名)')),
      }, { status: response.status === 401 ? 401 : 503 });
    }
    importedBacktests = Number(payload?.imported || 0);
    skippedBacktests = Array.isArray(payload?.skipped) ? payload.skipped.map(String) : [];
  }

  return NextResponse.json({
    imported: Number(selectionResult.data?.imported || 0),
    imported_backtests: importedBacktests,
    skipped: Array.isArray(selectionResult.data?.skipped) ? selectionResult.data.skipped : [],
    skipped_backtests: skippedBacktests,
  });
}

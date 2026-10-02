import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'edge';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  const cookie = req.headers.get('cookie') || '';
  const [strategies, versions, backtestResponse] = await Promise.all([
    sql`
      SELECT id, name, formula, timeframe,
             source_backtest_strategy_id, source_backtest_strategy_version,
             source_backtest_strategy_name, source_backtest_strategy_trigger,
             created_at, updated_at
      FROM strategies WHERE user_id = ${auth.user.userId}
      ORDER BY id ASC
    `,
    sql`
      SELECT strategy_id, version_no, name, formula, timeframe,
             source_backtest_strategy_id, source_backtest_strategy_version,
             source_backtest_strategy_name, source_backtest_strategy_trigger,
             created_at
      FROM strategy_versions
      WHERE strategy_id IN (SELECT id FROM strategies WHERE user_id = ${auth.user.userId})
      ORDER BY strategy_id ASC, version_no ASC
    `,
    fetch(new URL('/api/v1/backtest-strategy-templates/export', BACKTEST_NODE), {
      headers: { Accept: 'application/json', Cookie: cookie },
      cache: 'no-store',
      signal: AbortSignal.timeout(10000),
    }),
  ]);

  if (!backtestResponse.ok) {
    const text = await backtestResponse.text().catch(() => '');
    let message = '回测策略导出失败';
    try {
      const payload = JSON.parse(text);
      message = String(payload?.detail || payload?.error || message);
    } catch {}
    return NextResponse.json({ error: message }, { status: backtestResponse.status === 401 ? 401 : 503 });
  }

  const backtestPayload = await backtestResponse.json();
  const selectionStrategies = strategies.rows.map((strategy) => ({
    name: strategy.name,
    formula: strategy.formula,
    timeframe: strategy.timeframe,
    created_at: strategy.created_at,
    updated_at: strategy.updated_at,
    source_backtest: strategy.source_backtest_strategy_id != null ? {
      strategy_id: strategy.source_backtest_strategy_id,
      version_no: strategy.source_backtest_strategy_version,
      name: strategy.source_backtest_strategy_name,
      trigger: strategy.source_backtest_strategy_trigger,
    } : null,
    versions: versions.rows
      .filter((version) => String(version.strategy_id) === String(strategy.id))
      .map(({ version_no, name, formula, timeframe,
               source_backtest_strategy_id, source_backtest_strategy_version,
               source_backtest_strategy_name, source_backtest_strategy_trigger,
               created_at }) => ({
        version_no, name, formula, timeframe, created_at,
        source_backtest: source_backtest_strategy_id != null ? {
          strategy_id: source_backtest_strategy_id,
          version_no: source_backtest_strategy_version,
          name: source_backtest_strategy_name,
          trigger: source_backtest_strategy_trigger,
        } : null,
      })),
  }));

  const payload = {
    format: 'blinkquant-strategy-assets-v1',
    exported_at: new Date().toISOString(),
    strategies: selectionStrategies,
    backtest_strategies: Array.isArray(backtestPayload?.templates) ? backtestPayload.templates : [],
  };

  return new NextResponse(JSON.stringify(payload, null, 2), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="blinkquant_strategy_assets_${new Date().toISOString().slice(0, 10)}.json"`,
    },
  });
}

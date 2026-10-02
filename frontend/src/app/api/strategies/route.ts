import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  const result = await sql`
    SELECT s.id, s.name, s.formula, s.timeframe, s.created_at, s.updated_at,
      s.source_backtest_strategy_id, s.source_backtest_strategy_version,
      s.source_backtest_strategy_name, s.source_backtest_strategy_trigger,
      s.source_backtest_artifact_id, s.source_backtest_artifact_title,
      COALESCE((SELECT MAX(v.version_no) FROM strategy_versions v WHERE v.strategy_id = s.id), 1)::int AS version_no
    FROM strategies s
    WHERE s.user_id = ${auth.user.userId}
    ORDER BY s.updated_at DESC
  `;
  return NextResponse.json({ strategies: result.rows });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  const body = await req.json();
  const name = String(body?.name || '').trim();
  const formula = String(body?.formula || '').trim();
  const timeframe = String(body?.timeframe || 'D').trim();

  if (!name || !formula) {
    return NextResponse.json({ error: '名称和公式必填' }, { status: 400 });
  }
  if (!['D', 'W', 'M'].includes(timeframe)) {
    return NextResponse.json({ error: '无效的时间周期' }, { status: 400 });
  }

  try {
    // Keep the strategy row and its initial version in one PostgreSQL statement:
    // if version creation fails, the strategy insert rolls back with the statement.
    const result = await sql`
      WITH new_strategy AS (
        INSERT INTO strategies (user_id, name, formula, timeframe)
        VALUES (${auth.user.userId}, ${name}, ${formula}, ${timeframe})
        RETURNING id, name, formula, timeframe, created_at, updated_at
      ),
      new_version AS (
        INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe)
        SELECT id, 1, name, formula, timeframe FROM new_strategy
        RETURNING strategy_id, version_no
      )
      SELECT s.id, s.name, s.formula, s.timeframe, s.created_at, s.updated_at, v.version_no
      FROM new_strategy s
      JOIN new_version v ON v.strategy_id = s.id
    `;
    const strategy = result.rows[0];
    if (!strategy) throw new Error('strategy and initial version insert returned no row');
    return NextResponse.json({ strategy }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '已存在同名策略' }, { status: 409 });
    console.error('[strategies] POST error:', error);
    return NextResponse.json({ error: '保存策略失败' }, { status: 500 });
  }
}

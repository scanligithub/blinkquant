import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const [strategies, versions] = await Promise.all([
    sql`
      SELECT id, name, formula, timeframe, created_at, updated_at
      FROM strategies WHERE user_id = ${auth.user.userId}
      ORDER BY id ASC
    `,
    sql`
      SELECT strategy_id, version_no, name, formula, timeframe, created_at
      FROM strategy_versions
      WHERE strategy_id IN (SELECT id FROM strategies WHERE user_id = ${auth.user.userId})
      ORDER BY strategy_id ASC, version_no ASC
    `,
  ]);

  const payload = {
    format: 'blinkquant-strategies-v1',
    exported_at: new Date().toISOString(),
    strategies: strategies.rows.map((strategy) => ({
      name: strategy.name,
      formula: strategy.formula,
      timeframe: strategy.timeframe,
      created_at: strategy.created_at,
      updated_at: strategy.updated_at,
      versions: versions.rows
        .filter((version) => String(version.strategy_id) === String(strategy.id))
        .map(({ version_no, name, formula, timeframe, created_at }) => ({
          version_no, name, formula, timeframe, created_at,
        })),
    })),
  };

  return new NextResponse(JSON.stringify(payload, null, 2), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="blinkquant_strategies_${new Date().toISOString().slice(0, 10)}.json"`,
    },
  });
}

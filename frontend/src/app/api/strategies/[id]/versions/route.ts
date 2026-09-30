import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'edge';

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return NextResponse.json({ error: '无效的策略 ID' }, { status: 400 });
  }

  const owned = await sql`
    SELECT id FROM strategies
    WHERE id = ${id} AND user_id = ${auth.user.userId}
    LIMIT 1
  `;
  if (!owned.rows[0]) return NextResponse.json({ error: '策略不存在' }, { status: 404 });

  const result = await sql`
    SELECT id, strategy_id, version_no, name, formula, timeframe, created_at
    FROM strategy_versions
    WHERE strategy_id = ${id}
    ORDER BY version_no DESC
  `;
  return NextResponse.json({ versions: result.rows });
}

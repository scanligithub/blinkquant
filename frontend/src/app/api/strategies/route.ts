import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const result = await sql`
    SELECT id, name, formula, timeframe, created_at, updated_at
    FROM strategies
    WHERE user_id = ${auth.user.userId}
    ORDER BY updated_at DESC
  `;
  return NextResponse.json({ strategies: result.rows });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

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
    const inserted = await sql`
      INSERT INTO strategies (user_id, name, formula, timeframe)
      VALUES (${auth.user.userId}, ${name}, ${formula}, ${timeframe})
      RETURNING id, name, formula, timeframe, created_at, updated_at
    `;
    const strategy = inserted.rows[0];
    if (!strategy) throw new Error('strategy insert returned no row');
    await sql`
      INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe)
      VALUES (${strategy.id}, 1, ${name}, ${formula}, ${timeframe})
    `;
    return NextResponse.json({ strategy: { ...strategy, version_no: 1 } }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '已存在同名策略' }, { status: 409 });
    console.error('[strategies] POST error:', error);
    return NextResponse.json({ error: '保存策略失败' }, { status: 500 });
  }
}

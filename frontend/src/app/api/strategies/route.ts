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

  const inserted = await sql`
    INSERT INTO strategies (user_id, name, formula, timeframe)
    VALUES (${auth.user.userId}, ${name}, ${formula}, ${timeframe})
    RETURNING id, name, formula, timeframe, created_at, updated_at
  `;
  return NextResponse.json({ strategy: inserted.rows[0] }, { status: 201 });
}

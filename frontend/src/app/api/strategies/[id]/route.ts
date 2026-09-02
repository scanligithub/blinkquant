import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

export async function PUT(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return NextResponse.json({ error: '无效的策略 ID' }, { status: 400 });
  }

  const body = await req.json();

  const existing = await sql`
    SELECT id, name, formula, timeframe FROM strategies
    WHERE id = ${id} AND user_id = ${auth.user.userId} LIMIT 1
  `;
  if (existing.rows.length === 0) {
    return NextResponse.json({ error: '策略不存在' }, { status: 404 });
  }
  const current = existing.rows[0];

  const name = body?.name !== undefined ? String(body.name).trim() : current.name;
  const formula = body?.formula !== undefined ? String(body.formula).trim() : current.formula;
  const timeframe = body?.timeframe !== undefined ? String(body.timeframe).trim() : current.timeframe;

  if (!name) {
    return NextResponse.json({ error: '名称不能为空' }, { status: 400 });
  }
  if (!formula) {
    return NextResponse.json({ error: '公式不能为空' }, { status: 400 });
  }
  if (!['D', 'W', 'M'].includes(timeframe)) {
    return NextResponse.json({ error: '无效的时间周期' }, { status: 400 });
  }

  const updated = await sql`
    UPDATE strategies
    SET name = ${name}, formula = ${formula}, timeframe = ${timeframe}, updated_at = NOW()
    WHERE id = ${id} AND user_id = ${auth.user.userId}
    RETURNING id, name, formula, timeframe, created_at, updated_at
  `;
  return NextResponse.json({ strategy: updated.rows[0] });
}

export async function DELETE(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return NextResponse.json({ error: '无效的策略 ID' }, { status: 400 });
  }

  const result = await sql`
    DELETE FROM strategies WHERE id = ${id} AND user_id = ${auth.user.userId}
  `;
  if (result.rowCount === 0) {
    return NextResponse.json({ error: '策略不存在' }, { status: 404 });
  }
  return NextResponse.json({ success: true });
}

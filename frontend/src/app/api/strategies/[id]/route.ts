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

  const result = await sql`
    SELECT s.id, s.name, s.formula, s.timeframe, s.created_at, s.updated_at,
      COALESCE((SELECT MAX(v.version_no) FROM strategy_versions v WHERE v.strategy_id = s.id), 1)::int AS version_no
    FROM strategies s
    WHERE s.id = ${id} AND s.user_id = ${auth.user.userId}
    LIMIT 1
  `;
  if (!result.rows[0]) return NextResponse.json({ error: '策略不存在' }, { status: 404 });
  return NextResponse.json({ strategy: result.rows[0] });
}

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

  try {
    const versionRow = await sql`
      SELECT COALESCE(MAX(version_no), 0)::int AS max_version
      FROM strategy_versions
      WHERE strategy_id = ${id}
    `;
    const nextVersion = Number(versionRow.rows[0]?.max_version || 0) + 1;

    const updated = await sql`
      UPDATE strategies
      SET name = ${name}, formula = ${formula}, timeframe = ${timeframe}, updated_at = NOW()
      WHERE id = ${id} AND user_id = ${auth.user.userId}
      RETURNING id, name, formula, timeframe, created_at, updated_at
    `;
    const strategy = updated.rows[0];
    if (!strategy) return NextResponse.json({ error: '策略不存在' }, { status: 404 });

    await sql`
      INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe)
      VALUES (${id}, ${nextVersion}, ${name}, ${formula}, ${timeframe})
    `;

    return NextResponse.json({ strategy: { ...strategy, version_no: nextVersion } });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '保存版本失败：策略版本冲突，请重试' }, { status: 409 });
    console.error('[strategies/:id] PUT error:', error);
    return NextResponse.json({ error: '更新策略失败' }, { status: 500 });
  }
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

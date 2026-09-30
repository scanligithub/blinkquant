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
      s.source_backtest_strategy_id, s.source_backtest_strategy_version,
      s.source_backtest_strategy_name, s.source_backtest_strategy_trigger,
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
  await ensureSelectionStrategyVersions();

  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return NextResponse.json({ error: '无效的策略 ID' }, { status: 400 });
  }

  let body: any;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
  }

  const existing = await sql`
    SELECT id, name, formula, timeframe, source_backtest_strategy_id,
           source_backtest_strategy_version, source_backtest_strategy_name,
           source_backtest_strategy_trigger FROM strategies
    WHERE id = ${id} AND user_id = ${auth.user.userId} LIMIT 1
  `;
  if (existing.rows.length === 0) {
    return NextResponse.json({ error: '策略不存在' }, { status: 404 });
  }
  const current = existing.rows[0];

  const name = body?.name !== undefined ? String(body.name).trim() : current.name;
  const formula = body?.formula !== undefined ? String(body.formula).trim() : current.formula;
  const timeframe = body?.timeframe !== undefined ? String(body.timeframe).trim() : current.timeframe;

  if (!name) return NextResponse.json({ error: '名称不能为空' }, { status: 400 });
  if (!formula) return NextResponse.json({ error: '公式不能为空' }, { status: 400 });
  if (!['D', 'W', 'M'].includes(timeframe)) {
    return NextResponse.json({ error: '无效的时间周期' }, { status: 400 });
  }

  try {
    // One statement makes the strategy update and its version record atomic.
    // A concurrent version-number collision aborts the whole statement, including UPDATE.
    const result = await sql`
      WITH updated AS (
        UPDATE strategies
        SET name = ${name}, formula = ${formula}, timeframe = ${timeframe}, updated_at = NOW()
        WHERE id = ${id} AND user_id = ${auth.user.userId}
        RETURNING id, name, formula, timeframe, created_at, updated_at
      ),
      next_version AS (
        SELECT updated.id,
          (COALESCE((SELECT MAX(v.version_no) FROM strategy_versions v
                     WHERE v.strategy_id = updated.id), 0) + 1)::int AS version_no
        FROM updated
      ),
      new_version AS (
        INSERT INTO strategy_versions (
          strategy_id, version_no, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger
        )
        SELECT updated.id, next_version.version_no, updated.name, updated.formula, updated.timeframe,
          ${current.source_backtest_strategy_id ?? null},
          ${current.source_backtest_strategy_version ?? null},
          ${current.source_backtest_strategy_name ?? null},
          ${current.source_backtest_strategy_trigger ?? null}
        FROM updated
        JOIN next_version ON next_version.id = updated.id
        RETURNING strategy_id, version_no
      )
      SELECT updated.id, updated.name, updated.formula, updated.timeframe,
             updated.created_at, updated.updated_at, new_version.version_no
      FROM updated
      JOIN new_version ON new_version.strategy_id = updated.id
    `;
    const strategy = result.rows[0];
    if (!strategy) return NextResponse.json({ error: '策略不存在' }, { status: 404 });
    return NextResponse.json({ strategy });
  } catch (error: any) {
    if (error?.code === '23505') {
      return NextResponse.json({ error: '保存版本失败：策略版本冲突，请重试' }, { status: 409 });
    }
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

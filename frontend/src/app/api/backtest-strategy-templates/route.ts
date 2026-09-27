import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

function validConfig(config: any) {
  return !!config && typeof config === 'object' && !!config.strategy && !!config.fee_policy && !!config.benchmark &&
    typeof config.min_listing_days === 'number' && config.min_listing_days >= 0 && typeof config.exclude_st === 'boolean';
}

function normalize(body: any) {
  if (!validConfig(body?.config)) return null;
  const c = body.config;
  return { strategy: c.strategy, fee_policy: c.fee_policy, benchmark: c.benchmark, min_listing_days: Math.trunc(c.min_listing_days), exclude_st: c.exclude_st };
}

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const result = await sql`SELECT id, name, description, config, created_at, updated_at FROM backtest_strategy_templates WHERE user_id = ${auth.user.userId} ORDER BY updated_at DESC`;
  return NextResponse.json({ templates: result.rows });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const body = await req.json();
  const name = String(body?.name || '').trim();
  const description = body?.description == null ? null : String(body.description).trim();
  const config = normalize(body);
  if (!name) return NextResponse.json({ error: '模板名称不能为空' }, { status: 400 });
  if (name.length > 100) return NextResponse.json({ error: '模板名称过长' }, { status: 400 });
  if (!config) return NextResponse.json({ error: '无效的回测策略模板配置' }, { status: 400 });
  try {
    const result = await sql.query(
      'INSERT INTO backtest_strategy_templates (user_id, name, description, config) VALUES ($1, $2, $3, $4::jsonb) RETURNING id, name, description, config, created_at, updated_at',
      [auth.user.userId, name, description, JSON.stringify(config)]
    );
    return NextResponse.json({ template: result.rows[0] }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '同名模板已存在' }, { status: 409 });
    throw error;
  }
}

export async function PUT(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const body = await req.json();
  const id = Number(body?.id);
  const name = String(body?.name || '').trim();
  const description = body?.description == null ? null : String(body.description).trim();
  const config = normalize(body);
  if (!Number.isInteger(id) || id <= 0) return NextResponse.json({ error: '无效的模板 ID' }, { status: 400 });
  if (!name || !config) return NextResponse.json({ error: '无效的模板数据' }, { status: 400 });
  try {
    const result = await sql`UPDATE backtest_strategy_templates SET name=${name}, description=${description}, config=${JSON.stringify(config)}::jsonb, updated_at=NOW() WHERE id=${id} AND user_id=${auth.user.userId} RETURNING id, name, description, config, created_at, updated_at`;
    if (!result.rows.length) return NextResponse.json({ error: '模板不存在' }, { status: 404 });
    return NextResponse.json({ template: result.rows[0] });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '同名模板已存在' }, { status: 409 });
    throw error;
  }
}

export async function DELETE(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const id = Number(req.nextUrl.searchParams.get('id'));
  if (!Number.isInteger(id) || id <= 0) return NextResponse.json({ error: '无效的模板 ID' }, { status: 400 });
  const result = await sql`DELETE FROM backtest_strategy_templates WHERE id=${id} AND user_id=${auth.user.userId}`;
  if (!result.rowCount) return NextResponse.json({ error: '模板不存在' }, { status: 404 });
  return NextResponse.json({ success: true });
}
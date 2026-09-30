import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { getOwnedWatchlist, parseWatchlistId, validateWatchlistName } from '@/lib/watchlists';

export const runtime = 'edge';

async function loadList(userId: string, id: number) {
  const list = await getOwnedWatchlist(userId, id);
  if (!list) return null;
  const items = await sql`SELECT code, created_at FROM watchlist_items WHERE watchlist_id = ${id} ORDER BY created_at ASC`;
  return { ...list, item_count: items.rows.length, codes: items.rows.map((row) => row.code) };
}

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const id = parseWatchlistId(params.id);
  if (!id) return NextResponse.json({ error: '无效的自选股列表 ID' }, { status: 400 });
  try {
    const watchlist = await loadList(auth.user.userId, id);
    if (!watchlist) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
    return NextResponse.json({ watchlist });
  } catch (error) {
    console.error('[watchlists/:id] GET error:', error);
    return NextResponse.json({ error: '加载自选股列表失败' }, { status: 500 });
  }
}

export async function PATCH(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const id = parseWatchlistId(params.id);
  if (!id) return NextResponse.json({ error: '无效的自选股列表 ID' }, { status: 400 });
  if (!(await getOwnedWatchlist(auth.user.userId, id))) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  let name: string;
  try {
    const body = await req.json();
    name = validateWatchlistName(body?.name);
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : '无效的自选股列表名称' }, { status: 400 });
  }
  try {
    const updated = await sql`
      UPDATE watchlists SET name = ${name}, updated_at = NOW()
      WHERE id = ${id} AND user_id = ${auth.user.userId}
      RETURNING id, name, is_default, created_at, updated_at
    `;
    return NextResponse.json({ watchlist: updated.rows[0] });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '已存在同名的自选股列表' }, { status: 409 });
    console.error('[watchlists/:id] PATCH error:', error);
    return NextResponse.json({ error: '修改自选股列表失败' }, { status: 500 });
  }
}

export async function DELETE(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const id = parseWatchlistId(params.id);
  if (!id) return NextResponse.json({ error: '无效的自选股列表 ID' }, { status: 400 });
  const current = await getOwnedWatchlist(auth.user.userId, id);
  if (!current) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  if (current.is_default) return NextResponse.json({ error: '默认自选列表不可删除' }, { status: 400 });
  const deleted = await sql`DELETE FROM watchlists WHERE id = ${id} AND user_id = ${auth.user.userId}`;
  if (deleted.rowCount === 0) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  return NextResponse.json({ success: true });
}

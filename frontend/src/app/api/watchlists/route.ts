import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureDefaultWatchlist, validateWatchlistName } from '@/lib/watchlists';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  try {
    await ensureDefaultWatchlist(auth.user.userId);
    const { searchParams } = new URL(req.url);
    const code = String(searchParams.get('code') || '').trim();
    const result = await sql`
      SELECT w.id, w.name, w.is_default, w.created_at, w.updated_at,
        COUNT(wi.code)::int AS item_count,
        CASE WHEN ${code} = '' THEN FALSE ELSE EXISTS (
          SELECT 1 FROM watchlist_items wx WHERE wx.watchlist_id = w.id AND wx.code = ${code}
        ) END AS contains
      FROM watchlists w
      LEFT JOIN watchlist_items wi ON wi.watchlist_id = w.id
      WHERE w.user_id = ${auth.user.userId}
      GROUP BY w.id
      ORDER BY w.is_default DESC, w.created_at ASC
    `;
    return NextResponse.json({
      watchlists: result.rows.map((row) => ({ ...row, item_count: Number(row.item_count || 0), contains: !!row.contains })),
    });
  } catch (error) {
    console.error('[watchlists] GET error:', error);
    return NextResponse.json({ error: '加载自选股列表失败' }, { status: 500 });
  }
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  let name: string;
  try {
    const body = await req.json();
    name = validateWatchlistName(body?.name);
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : '无效的自选股列表名称' }, { status: 400 });
  }
  try {
    const inserted = await sql`
      INSERT INTO watchlists (user_id, name, is_default)
      VALUES (${auth.user.userId}, ${name}, FALSE)
      RETURNING id, name, is_default, created_at, updated_at
    `;
    return NextResponse.json({ watchlist: { ...inserted.rows[0], item_count: 0 } }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '已存在同名的自选股列表' }, { status: 409 });
    console.error('[watchlists] POST error:', error);
    return NextResponse.json({ error: '创建自选股列表失败' }, { status: 500 });
  }
}

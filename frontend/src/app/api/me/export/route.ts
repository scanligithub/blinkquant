import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { buildUserExport, sanitizeFilename } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  try {
    const userRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users WHERE id = ${auth.user.userId} LIMIT 1
    `;
    if (userRes.rows.length === 0) {
      return NextResponse.json({ error: '用户不存在' }, { status: 404 });
    }
    const user = userRes.rows[0];
    const watchlist = await sql`
      SELECT code, created_at FROM watchlist WHERE user_id = ${auth.user.userId} ORDER BY created_at DESC
    `;
    const watchlistsRes = await sql`
      SELECT id, name, is_default, created_at, updated_at
      FROM watchlists WHERE user_id = ${auth.user.userId}
      ORDER BY is_default DESC, created_at ASC
    `;
    const watchlistItems = await sql`
      SELECT watchlist_id, code, created_at
      FROM watchlist_items
      WHERE watchlist_id IN (SELECT id FROM watchlists WHERE user_id = ${auth.user.userId})
      ORDER BY created_at ASC
    `;
    const watchlists = watchlistsRes.rows.map((list) => ({
      ...list,
      items: watchlistItems.rows.filter((item) => String(item.watchlist_id) === String(list.id)),
    }));
    const strategies = await sql`
      SELECT name, formula, timeframe, created_at, updated_at FROM strategies WHERE user_id = ${auth.user.userId} ORDER BY created_at DESC
    `;
    const body = JSON.stringify(buildUserExport(user, watchlist.rows, strategies.rows, watchlists), null, 2);
    const filename = sanitizeFilename(user.email, 'json');
    return new NextResponse(body, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    });
  } catch (e) {
    console.error('[me/export] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}

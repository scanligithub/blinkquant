import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { buildUserExport, sanitizeFilename } from '@/lib/export';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  try {
    const userRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users WHERE id = ${auth.user.userId} LIMIT 1
    `;
    if (userRes.rows.length === 0) return NextResponse.json({ error: '用户不存在' }, { status: 404 });

    const [watchlistsResult, strategiesResult] = await Promise.all([
      node1Json(node1Path('/user-assets/watchlists/export', { user_id: auth.user.userId })),
      node1Json(node1Path('/user-assets/strategies/export', { user_id: auth.user.userId })),
    ]);
    if (!watchlistsResult.response.ok) throw new Error(String(watchlistsResult.data?.detail || watchlistsResult.data?.error || '自选股读取失败'));
    if (!strategiesResult.response.ok) throw new Error(String(strategiesResult.data?.detail || strategiesResult.data?.error || '策略读取失败'));

    const watchlists = Array.isArray(watchlistsResult.data?.watchlists)
      ? watchlistsResult.data.watchlists
      : [];
    const defaultList = watchlists.find((item: any) => item?.is_default) || watchlists[0] || null;
    const watchlist = defaultList && Array.isArray(defaultList.codes)
      ? defaultList.codes.map((code: string) => ({ code, created_at: defaultList.created_at }))
      : [];
    const strategies = Array.isArray(strategiesResult.data?.strategies)
      ? strategiesResult.data.strategies
      : [];

    const body = JSON.stringify(
      buildUserExport(userRes.rows[0], watchlist, strategies, watchlists),
      null,
      2,
    );
    const filename = sanitizeFilename(userRes.rows[0].email, 'json');
    return new NextResponse(body, {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    });
  } catch (error) {
    console.error('[me/export] error:', error);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}

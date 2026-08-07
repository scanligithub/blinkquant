import { NextRequest, NextResponse } from 'next/server';
import { zipSync } from 'fflate';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { buildUserExports } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  try {
    const usersRes = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users ORDER BY created_at DESC
    `;
    const watchlistRes = await sql`
      SELECT user_id, code, created_at FROM watchlist
    `;
    const strategiesRes = await sql`
      SELECT user_id, name, formula, timeframe, created_at, updated_at FROM strategies
    `;

    const watchlistByUser = new Map<string, any[]>();
    for (const row of watchlistRes.rows) {
      const list = watchlistByUser.get(row.user_id) || [];
      list.push({ code: row.code, created_at: row.created_at });
      watchlistByUser.set(row.user_id, list);
    }

    const strategiesByUser = new Map<string, any[]>();
    for (const row of strategiesRes.rows) {
      const list = strategiesByUser.get(row.user_id) || [];
      list.push({
        name: row.name, formula: row.formula, timeframe: row.timeframe,
        created_at: row.created_at, updated_at: row.updated_at,
      });
      strategiesByUser.set(row.user_id, list);
    }

    const files = buildUserExports(usersRes.rows, watchlistByUser, strategiesByUser);
    const encoder = new TextEncoder();
    const zipped = zipSync(
      Object.fromEntries(files.map((f) => [f.filename, encoder.encode(f.content)])),
    );

    const date = new Date().toISOString().slice(0, 10);
    return new NextResponse(zipped, {
      headers: {
        'Content-Type': 'application/zip',
        'Content-Disposition': `attachment; filename="users_export_${date}.zip"`,
      },
    });
  } catch (e) {
    console.error('[admin/users/export-zip] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}

import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  const { searchParams } = new URL(req.url);
  const keyword = searchParams.get('keyword') || '';
  const statusFilter = searchParams.get('status') || '';
  const page = Math.max(1, Number(searchParams.get('page')) || 1);
  const pageSize = Math.min(100, Math.max(1, Number(searchParams.get('pageSize')) || 20));
  const offset = (page - 1) * pageSize;

  const kw = keyword ? `%${keyword.toLowerCase()}%` : '%';
  const st = statusFilter === 'active' || statusFilter === 'disabled' ? statusFilter : null;

  try {
    const list = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users
      WHERE LOWER(email) LIKE ${kw}
        AND (${st} IS NULL OR status = ${st})
      ORDER BY created_at DESC
      LIMIT ${pageSize} OFFSET ${offset}
    `;
    const count = await sql`
      SELECT COUNT(*)::int AS total FROM users
      WHERE LOWER(email) LIKE ${kw}
        AND (${st} IS NULL OR status = ${st})
    `;

    return NextResponse.json({
      users: list.rows,
      total: count.rows[0]?.total ?? 0,
      page,
      pageSize,
    });
  } catch (e) {
    console.error('[admin/users] error:', e);
    return NextResponse.json({ error: String((e as Error)?.message || e) }, { status: 500 });
  }
}

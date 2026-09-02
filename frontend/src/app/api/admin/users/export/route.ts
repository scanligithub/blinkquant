import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { toCSV } from '@/lib/export';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  try {
    const result = await sql`
      SELECT id, email, role, status, created_at, last_login_at
      FROM users ORDER BY created_at DESC
    `;
    const headers = ['id', 'email', 'role', 'status', 'created_at', 'last_login_at'];
    const rows = result.rows.map((r) => [r.id, r.email, r.role, r.status, r.created_at, r.last_login_at]);
    const csv = toCSV(headers, rows);
    const date = new Date().toISOString().slice(0, 10);
    return new NextResponse(csv, {
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': `attachment; filename="users_${date}.csv"`,
      },
    });
  } catch (e) {
    console.error('[admin/users/export] error:', e);
    return NextResponse.json({ error: '导出失败' }, { status: 500 });
  }
}

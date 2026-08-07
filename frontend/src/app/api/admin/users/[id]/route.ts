import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';

export const runtime = 'edge';

export async function PATCH(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  const body = await req.json();
  const role = body?.role;
  const status = body?.status;

  if (role !== undefined && !['user', 'admin'].includes(role)) {
    return NextResponse.json({ error: '无效的角色' }, { status: 400 });
  }
  if (status !== undefined && !['active', 'disabled'].includes(status)) {
    return NextResponse.json({ error: '无效的状态' }, { status: 400 });
  }
  if (role === undefined && status === undefined) {
    return NextResponse.json({ error: '没有需要更新的字段' }, { status: 400 });
  }

  if (params.id === auth.user.userId && status === 'disabled') {
    return NextResponse.json({ error: '不能禁用自己' }, { status: 400 });
  }
  if (params.id === auth.user.userId && role === 'user') {
    return NextResponse.json({ error: '不能降级自己的管理员角色' }, { status: 400 });
  }

  try {
    const updated = await sql`
      UPDATE users
      SET role = COALESCE(${role ?? null}, role),
          status = COALESCE(${status ?? null}, status)
      WHERE id = ${params.id}
      RETURNING id, email, role, status, created_at, last_login_at
    `;
    if (updated.rows.length === 0) {
      return NextResponse.json({ error: '用户不存在' }, { status: 404 });
    }
    return NextResponse.json({ user: updated.rows[0] });
  } catch (e) {
    console.error('[admin/users/:id] error:', e);
    return NextResponse.json({ error: String((e as Error)?.message || e) }, { status: 500 });
  }
}

export async function DELETE(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAdmin(req);
  if (!auth.user) {
    return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });
  }

  if (params.id === auth.user.userId) {
    return NextResponse.json({ error: '不能删除自己' }, { status: 400 });
  }

  const result = await sql`DELETE FROM users WHERE id = ${params.id}`;
  if (result.rowCount === 0) {
    return NextResponse.json({ error: '用户不存在' }, { status: 404 });
  }
  return NextResponse.json({ success: true });
}

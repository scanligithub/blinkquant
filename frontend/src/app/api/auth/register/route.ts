import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { signToken, setAuthCookie, isValidEmail, isValidPassword } from '@/lib/auth';

export const runtime = 'edge';

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const email = String(body?.email || '').trim().toLowerCase();
    const password = String(body?.password || '');

    if (!isValidEmail(email)) {
      return NextResponse.json({ error: '邮箱格式不正确' }, { status: 400 });
    }
    if (!isValidPassword(password)) {
      return NextResponse.json({ error: '密码长度至少 8 位' }, { status: 400 });
    }

    const bcrypt = (await import('bcryptjs')).default;
    const passwordHash = await bcrypt.hash(password, 10);

    const existing = await sql`SELECT id FROM users WHERE email = ${email} LIMIT 1`;
    if (existing.rows.length > 0) {
      return NextResponse.json({ error: '该邮箱已注册' }, { status: 409 });
    }

    const inserted = await sql`
      INSERT INTO users (id, email, password_hash, role, status)
      VALUES (gen_random_uuid(), ${email}, ${passwordHash}, 'user', 'active')
      RETURNING id, email, role, status
    `;
    const user = inserted.rows[0];
    if (!user) {
      return NextResponse.json({ error: '注册失败' }, { status: 500 });
    }

    const token = await signToken({ userId: user.id, email: user.email, role: user.role });
    const res = NextResponse.json({ user: { id: user.id, email: user.email, role: user.role } }, { status: 201 });
    return setAuthCookie(res, token);
  } catch (e) {
    console.error('[register] error:', e);
    return NextResponse.json({ error: '服务器内部错误' }, { status: 500 });
  }
}

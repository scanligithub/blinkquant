import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { signToken, setAuthCookie, isValidEmail, ensureAdmin } from '@/lib/auth';

export const runtime = 'edge';

export async function POST(req: NextRequest) {
  try {
    await ensureAdmin();

    const body = await req.json();
    const email = String(body?.email || '').trim().toLowerCase();
    const password = String(body?.password || '');

    if (!isValidEmail(email) || !password) {
      return NextResponse.json({ error: '邮箱或密码错误' }, { status: 401 });
    }

    const result = await sql`
      SELECT id, email, password_hash, role, status
      FROM users WHERE email = ${email} LIMIT 1
    `;
    const user = result.rows[0];
    if (!user) {
      return NextResponse.json({ error: '邮箱或密码错误' }, { status: 401 });
    }
    if (user.status !== 'active') {
      return NextResponse.json({ error: '账号已被禁用' }, { status: 403 });
    }

    const bcrypt = (await import('bcryptjs')).default;
    const valid = await bcrypt.compare(password, user.password_hash);
    if (!valid) {
      return NextResponse.json({ error: '邮箱或密码错误' }, { status: 401 });
    }

    await sql`UPDATE users SET last_login_at = NOW() WHERE id = ${user.id}`;

    const token = await signToken({ userId: user.id, email: user.email, role: user.role });
    const res = NextResponse.json({ user: { id: user.id, email: user.email, role: user.role } });
    return setAuthCookie(res, token);
  } catch (e) {
    console.error('[login] error:', e);
    return NextResponse.json({ error: '服务器内部错误' }, { status: 500 });
  }
}

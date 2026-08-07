import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const result = await sql`
    SELECT code, created_at FROM watchlist
    WHERE user_id = ${auth.user.userId}
    ORDER BY created_at ASC
  `;
  return NextResponse.json({ codes: result.rows.map(r => r.code) });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const body = await req.json();
  const code = String(body?.code || '').trim();
  if (!code) {
    return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  }

  await sql`
    INSERT INTO watchlist (user_id, code)
    VALUES (${auth.user.userId}, ${code})
    ON CONFLICT (user_id, code) DO NOTHING
  `;
  return NextResponse.json({ success: true });
}

export async function DELETE(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  if (!code) {
    return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  }

  await sql`
    DELETE FROM watchlist WHERE user_id = ${auth.user.userId} AND code = ${code}
  `;
  return NextResponse.json({ success: true });
}

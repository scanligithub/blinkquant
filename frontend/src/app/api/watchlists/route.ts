import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const code = String(req.nextUrl.searchParams.get('code') || '').trim();
  const { response, data } = await node1Json(node1Path('/user-assets/watchlists', { user_id: auth.user.userId, code }));
  return NextResponse.json(data, { status: response.status });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  try {
    const body = await req.json();
    const { response, data } = await node1Json('/user-assets/watchlists', {
      method: 'POST',
      body: JSON.stringify({ ...body, user_id: auth.user.userId }),
    });
    return NextResponse.json(data, { status: response.status });
  } catch {
    return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
  }
}

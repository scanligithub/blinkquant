import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { response, data } = await node1Json('/user-assets/watchlists/' + params.id + '?user_id=' + encodeURIComponent(auth.user.userId));
  return NextResponse.json(data, { status: response.status });
}

export async function PATCH(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  try {
    const body = await req.json();
    const { response, data } = await node1Json('/user-assets/watchlists/' + params.id, {
      method: 'PATCH', body: JSON.stringify({ ...body, user_id: auth.user.userId }),
    });
    return NextResponse.json(data, { status: response.status });
  } catch {
    return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
  }
}

export async function DELETE(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { response, data } = await node1Json(
    '/user-assets/watchlists/' + params.id + '?user_id=' + encodeURIComponent(auth.user.userId),
    { method: 'DELETE' },
  );
  return NextResponse.json(data, { status: response.status });
}

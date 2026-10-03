import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const listId = req.nextUrl.searchParams.get('listId');
  const { response, data } = await node1Json(node1Path('/user-assets/watchlist', { user_id: auth.user.userId, listId: listId ? Number(listId) : undefined }));
  return NextResponse.json(
    data?.watchlist ? { codes: data.watchlist.codes || [], listId: data.watchlist.id } : data,
    { status: response.status },
  );
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  try {
    const body = await req.json();
    const { response, data } = await node1Json('/user-assets/watchlist/items', {
      method: 'POST',
      body: JSON.stringify({ ...body, user_id: auth.user.userId }),
    });
    return NextResponse.json(data, { status: response.status });
  } catch {
    return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
  }
}

export async function DELETE(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  let body: any = {};
  try { body = await req.json(); } catch {}
  const listId = body?.listId ?? req.nextUrl.searchParams.get('listId');
  const codes = Array.isArray(body?.codes) ? body.codes : undefined;
  const code = body?.code ?? req.nextUrl.searchParams.get('code');
  const path = node1Path('/user-assets/watchlist/items', {
    user_id: auth.user.userId,
    listId: listId ? Number(listId) : undefined,
    code: codes?.length ? undefined : (code ? String(code) : undefined),
    codes: codes?.length ? codes.map(String).join(',') : undefined,
  });
  const { response, data } = await node1Json(path, { method: 'DELETE' });
  return NextResponse.json(data, { status: response.status });
}

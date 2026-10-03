import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  let body: any;
  try { body = await req.json(); } catch {
    return NextResponse.json({ error: '导入文件格式无效' }, { status: 400 });
  }
  const { response, data } = await node1Json('/user-assets/watchlists/import', {
    method: 'POST',
    body: JSON.stringify({ ...body, user_id: auth.user.userId }),
  });
  return NextResponse.json(data, { status: response.status });
}

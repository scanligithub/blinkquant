import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const id = Number(params.id);
  if (!Number.isInteger(id) || id <= 0) return NextResponse.json({ error: '无效的策略 ID' }, { status: 400 });
  const { response, data } = await node1Json(node1Path('/user-assets/strategies/' + id + '/versions', { user_id: auth.user.userId }));
  return NextResponse.json(data, { status: response.status });
}

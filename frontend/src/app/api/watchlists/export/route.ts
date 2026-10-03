import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { node1Json, node1Path } from '@/lib/node1Internal';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { response, data } = await node1Json(node1Path('/user-assets/watchlists/export', { user_id: auth.user.userId }));
  if (!response.ok) return NextResponse.json({ error: String(data?.detail || data?.error || '导出自选股失败') }, { status: response.status });
  return new NextResponse(JSON.stringify(data, null, 2), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="blinkquant_watchlists_${new Date().toISOString().slice(0, 10)}.json"`,
    },
  });
}

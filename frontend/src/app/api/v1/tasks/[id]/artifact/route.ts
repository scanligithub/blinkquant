import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';

const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';

const ALLOWED = new Set(['equity_curve', 'trades', 'positions_daily']);

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const auth = await requireAuth(req);
  if (auth.status !== 200 || !auth.user?.userId) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { id } = await params;
  const taskId = parseInt(id, 10);
  if (Number.isNaN(taskId)) {
    return NextResponse.json({ error: 'Invalid task ID' }, { status: 400 });
  }

  const name = req.nextUrl.searchParams.get('name') || 'equity_curve';
  if (!ALLOWED.has(name)) {
    return NextResponse.json({ error: 'Invalid artifact name' }, { status: 400 });
  }

  const url =
    `${NODE1_URL}/internal/tasks/${taskId}/artifact?name=${encodeURIComponent(name)}`;

  const upstream = await fetch(url, {
    headers: { Authorization: `Bearer ${INTERNAL_TOKEN}` },
    cache: 'no-store',
  });

  if (!upstream.ok) {
    const text = await upstream.text().catch(() => '');
    return NextResponse.json(
      { error: text || `upstream ${upstream.status}` },
      { status: upstream.status }
    );
  }

  const buf = await upstream.arrayBuffer();
  return new NextResponse(buf, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': `attachment; filename="task_${taskId}_${name}.parquet"`,
      'Cache-Control': 'no-store',
    },
  });
}

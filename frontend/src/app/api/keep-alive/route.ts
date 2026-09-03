import { NextRequest, NextResponse } from 'next/server';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space/api/v1/health',
  'https://scanli-blinkquant-node2.hf.space/api/v1/health',
  'https://scanli-blinkquant-node3.hf.space/api/v1/health',
];

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  // Verify this is a Vercel cron request
  const authHeader = req.headers.get('authorization');
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const results = await Promise.allSettled(
    NODES.map(async (url) => {
      try {
        const r = await fetch(url, { method: 'GET', signal: AbortSignal.timeout(30000) });
        return { url, status: r.status, ok: r.ok };
      } catch (e) {
        return { url, error: String(e) };
      }
    })
  );

  return NextResponse.json({ results });
}
import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
export const runtime = 'nodejs';
const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';
export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (auth.status !== 200 || !auth.user?.userId) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  const qs = new URLSearchParams(req.nextUrl.searchParams);
  qs.set('user_id', auth.user.userId);
  if (auth.user.role) qs.set('role', auth.user.role);
  const response = await fetch(NODE1_URL + '/internal/artifacts?' + qs.toString(), {
    headers: { Authorization: 'Bearer ' + INTERNAL_TOKEN }, cache: 'no-store',
  });
  const data = await response.json().catch(() => ({}));
  return NextResponse.json(data, { status: response.status });
}

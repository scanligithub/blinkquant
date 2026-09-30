import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureDefaultWatchlist, getOwnedWatchlist, parseWatchlistId } from '@/lib/watchlists';

export const runtime = 'edge';

async function resolveListId(userId: string, requestedId: string | null | undefined): Promise<number | null> {
  const explicitId = parseWatchlistId(requestedId);
  if (explicitId) {
    const owned = await getOwnedWatchlist(userId, explicitId);
    return owned ? explicitId : null;
  }
  const fallback = await ensureDefaultWatchlist(userId);
  return fallback ? Number(fallback.id) : null;
}

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { searchParams } = new URL(req.url);
  const listId = await resolveListId(auth.user.userId, searchParams.get('listId'));
  if (!listId) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  const result = await sql`SELECT code, created_at FROM watchlist_items WHERE watchlist_id = ${listId} ORDER BY created_at ASC`;
  return NextResponse.json({ codes: result.rows.map((r) => r.code), listId });
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const body = await req.json();
  const code = String(body?.code || '').trim();
  if (!code) return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  const listId = await resolveListId(auth.user.userId, body?.listId);
  if (!listId) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  await sql`INSERT INTO watchlist_items (watchlist_id, code) VALUES (${listId}, ${code}) ON CONFLICT (watchlist_id, code) DO NOTHING`;
  return NextResponse.json({ success: true, listId });
}

export async function DELETE(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  if (!code) return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  const listId = await resolveListId(auth.user.userId, searchParams.get('listId'));
  if (!listId) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  await sql`DELETE FROM watchlist_items WHERE watchlist_id = ${listId} AND code = ${code}`;
  return NextResponse.json({ success: true, listId });
}

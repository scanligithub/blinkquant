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
  const listId = await resolveListId(auth.user.userId, body?.listId);
  if (!listId) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });

  const requestedCodes = Array.isArray(body?.codes)
    ? body.codes.map((v: unknown) => String(v || '').trim()).filter(Boolean)
    : [String(body?.code || '').trim()].filter(Boolean);

  const codes = Array.from(new Set<string>(requestedCodes));
  if (!codes.length) return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  if (codes.length > 5000) return NextResponse.json({ error: '一次最多加入 5000 只股票' }, { status: 400 });

  let addedCount = 0;
  for (let i = 0; i < codes.length; i += 50) {
    const chunk = codes.slice(i, i + 50);
    const results = await Promise.all(
      chunk.map((code) => sql`
        INSERT INTO watchlist_items (watchlist_id, code)
        VALUES (${listId}, ${code})
        ON CONFLICT (watchlist_id, code) DO NOTHING
        RETURNING code
      `)
    );
    addedCount += results.filter((r) => r.rows.length > 0).length;
  }
  await sql`UPDATE watchlists SET updated_at = NOW() WHERE id = ${listId} AND user_id = ${auth.user.userId}`;
  return NextResponse.json({ success: true, listId, requested_count: codes.length, added_count: addedCount });
}

export async function DELETE(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  const { searchParams } = new URL(req.url);
  const listId = await resolveListId(auth.user.userId, searchParams.get('listId'));

  let body: { code?: unknown; codes?: unknown[] } = {};
  try {
    body = await req.json();
  } catch {
    // Legacy query-parameter deletion remains supported.
  }

  const requestedCodes = Array.isArray(body.codes)
    ? body.codes.map((value) => String(value || '').trim()).filter(Boolean)
    : [String(body.code || searchParams.get('code') || '').trim()].filter(Boolean);
  const codes = Array.from(new Set(requestedCodes));

  if (!listId) return NextResponse.json({ error: '自选股列表不存在' }, { status: 404 });
  if (!codes.length) return NextResponse.json({ error: '缺少股票代码' }, { status: 400 });
  if (codes.length > 5000) return NextResponse.json({ error: '一次最多移除 5000 只股票' }, { status: 400 });

  let removedCount = 0;
  for (let i = 0; i < codes.length; i += 50) {
    const chunk = codes.slice(i, i + 50);
    const result = await sql`
      DELETE FROM watchlist_items
      WHERE watchlist_id = ${listId}
        AND code = ANY(${chunk})
      RETURNING code
    `;
    removedCount += result.rows.length;
  }
  if (removedCount > 0) {
    await sql`UPDATE watchlists SET updated_at = NOW() WHERE id = ${listId} AND user_id = ${auth.user.userId}`;
  }
  return NextResponse.json({ success: true, listId, requested_count: codes.length, removed_count: removedCount });
}

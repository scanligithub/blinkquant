import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureDefaultWatchlist } from '@/lib/watchlists';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  try {
    await ensureDefaultWatchlist(auth.user.userId);
    const result = await sql`
      SELECT
        w.id,
        w.name,
        w.is_default,
        w.created_at,
        w.updated_at,
        wi.code,
        wi.created_at AS item_created_at
      FROM watchlists w
      LEFT JOIN watchlist_items wi ON wi.watchlist_id = w.id
      WHERE w.user_id = ${auth.user.userId}
      ORDER BY w.is_default DESC, w.created_at ASC, wi.created_at ASC, wi.code ASC
    `;

    const byList = new Map<number, {
      name: string;
      is_default: boolean;
      created_at: string;
      updated_at: string;
      codes: string[];
    }>();

    for (const row of result.rows) {
      const id = Number(row.id);
      let item = byList.get(id);
      if (!item) {
        item = {
          name: String(row.name),
          is_default: !!row.is_default,
          created_at: row.created_at,
          updated_at: row.updated_at,
          codes: [],
        };
        byList.set(id, item);
      }
      if (row.code != null) item.codes.push(String(row.code));
    }

    return new NextResponse(JSON.stringify({
      format: 'blinkquant-watchlists-v1',
      exported_at: new Date().toISOString(),
      watchlists: Array.from(byList.values()).map((list) => ({
        name: list.name,
        is_default: list.is_default,
        created_at: list.created_at,
        updated_at: list.updated_at,
        codes: list.codes,
      })),
    }, null, 2), {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="blinkquant_watchlists_${new Date().toISOString().slice(0, 10)}.json"`,
      },
    });
  } catch (error) {
    console.error('[watchlists/export] GET error:', error);
    return NextResponse.json({ error: '导出自选股失败' }, { status: 500 });
  }
}

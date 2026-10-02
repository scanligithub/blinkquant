import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureDefaultWatchlist, validateWatchlistName } from '@/lib/watchlists';

export const runtime = 'edge';

const MAX_LISTS_PER_IMPORT = 100;
const MAX_CODES_PER_LIST = 5000;

function normalizeCodes(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return Array.from(new Set(
    value.map((code) => String(code ?? '').trim()).filter(Boolean)
  ));
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  await ensureDefaultWatchlist(auth.user.userId);

  let body: any;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '导入文件格式无效' }, { status: 400 });
  }

  const items = Array.isArray(body?.watchlists) ? body.watchlists : [];
  if (!items.length) return NextResponse.json({ error: '没有可导入的自选股列表' }, { status: 400 });
  if (items.length > MAX_LISTS_PER_IMPORT) {
    return NextResponse.json({ error: `单次最多导入 ${MAX_LISTS_PER_IMPORT} 个自选股列表` }, { status: 400 });
  }

  let imported = 0;
  const skipped: string[] = [];

  for (const item of items) {
    let name = '';
    try {
      name = validateWatchlistName(item?.name);
    } catch {
      skipped.push(String(item?.name || '(未命名)'));
      continue;
    }

    const codes = normalizeCodes(item?.codes);
    if (codes.length > MAX_CODES_PER_LIST) {
      skipped.push(name);
      continue;
    }

    try {
      const result = await sql`
        WITH new_watchlist AS (
          INSERT INTO watchlists (user_id, name, is_default)
          VALUES (${auth.user.userId}, ${name}, FALSE)
          ON CONFLICT (user_id, name) DO NOTHING
          RETURNING id
        ),
        inserted_items AS (
          INSERT INTO watchlist_items (watchlist_id, code)
          SELECT nw.id, codes.code
          FROM new_watchlist nw
          CROSS JOIN LATERAL jsonb_array_elements_text(${JSON.stringify(codes)}::jsonb) AS codes(code)
          ON CONFLICT (watchlist_id, code) DO NOTHING
          RETURNING code
        )
        SELECT
          (SELECT id FROM new_watchlist) AS id,
          (SELECT COUNT(*) FROM inserted_items)::int AS item_count
      `;

      if (result.rows[0]?.id != null) {
        imported += 1;
      } else {
        skipped.push(name);
      }
    } catch (error) {
      console.error('[watchlists/import] item error:', error);
      skipped.push(name);
    }
  }

  return NextResponse.json({
    imported,
    skipped,
  });
}

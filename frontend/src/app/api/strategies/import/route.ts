import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

const validTimeframe = (value: unknown) => ['D', 'W', 'M'].includes(String(value));

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });

  let body: any;
  try { body = await req.json(); } catch {
    return NextResponse.json({ error: '导入文件格式无效' }, { status: 400 });
  }

  const items = Array.isArray(body?.strategies) ? body.strategies : [];
  if (items.length === 0) return NextResponse.json({ error: '没有可导入的策略' }, { status: 400 });
  if (items.length > 100) return NextResponse.json({ error: '单次最多导入 100 个策略' }, { status: 400 });

  let imported = 0;
  const skipped: string[] = [];

  for (const item of items) {
    const name = String(item?.name || '').trim();
    const formula = String(item?.formula || '').trim();
    const timeframe = String(item?.timeframe || 'D').trim();
    if (!name || !formula || !validTimeframe(timeframe)) {
      skipped.push(name || '(未命名)');
      continue;
    }

    const existing = await sql`
      SELECT id FROM strategies
      WHERE user_id = ${auth.user.userId} AND name = ${name}
      LIMIT 1
    `;
    if (existing.rows[0]) {
      skipped.push(name);
      continue;
    }

    try {
      const inserted = await sql`
        INSERT INTO strategies (user_id, name, formula, timeframe)
        VALUES (${auth.user.userId}, ${name}, ${formula}, ${timeframe})
        RETURNING id
      `;
      const strategyId = inserted.rows[0]?.id;
      if (!strategyId) throw new Error('strategy insert failed');

      const versions = Array.isArray(item?.versions) ? item.versions : [];
      const cleanVersions = versions
        .map((version: any) => ({
          version_no: Number(version?.version_no),
          name: String(version?.name || name).trim(),
          formula: String(version?.formula || formula).trim(),
          timeframe: String(version?.timeframe || timeframe).trim(),
        }))
        .filter((version: any) =>
          Number.isInteger(version.version_no) && version.version_no > 0 &&
          version.name && version.formula && validTimeframe(version.timeframe)
        )
        .sort((a: any, b: any) => a.version_no - b.version_no);

      if (cleanVersions.length > 0) {
        for (const version of cleanVersions) {
          await sql`
            INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe)
            VALUES (${strategyId}, ${version.version_no}, ${version.name}, ${version.formula}, ${version.timeframe})
            ON CONFLICT (strategy_id, version_no) DO NOTHING
          `;
        }
      } else {
        await sql`
          INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe)
          VALUES (${strategyId}, 1, ${name}, ${formula}, ${timeframe})
        `;
      }
      imported += 1;
    } catch (error) {
      console.error('[strategies/import] item error:', error);
      skipped.push(name);
    }
  }

  return NextResponse.json({ imported, skipped });
}

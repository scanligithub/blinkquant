import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'edge';

const validTimeframe = (value: unknown) => ['D', 'W', 'M'].includes(String(value));

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

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

    // Imported JSON is untrusted: source_backtest fields are descriptive metadata,
    // not proof that the referenced backtest strategy/version belongs to this user.
    // Never persist provenance unless it has been verified by the backtest API.
    const rawVersions = Array.isArray(item?.versions) ? item.versions : [];
    if (rawVersions.length > 100) {
      skipped.push(name);
      continue;
    }

    const byVersion = new Map<number, { version_no: number; name: string; formula: string; timeframe: string }>();
    for (const version of rawVersions) {
      const versionNo = Number(version?.version_no);
      const versionName = String(version?.name || name).trim();
      const versionFormula = String(version?.formula || formula).trim();
      const versionTimeframe = String(version?.timeframe || timeframe).trim();
      if (!Number.isInteger(versionNo) || versionNo <= 0 || !versionName ||
          !versionFormula || !validTimeframe(versionTimeframe) || byVersion.has(versionNo)) {
        continue;
      }
      byVersion.set(versionNo, {
        version_no: versionNo,
        name: versionName,
        formula: versionFormula,
        timeframe: versionTimeframe,
      });
    }

    const cleanVersions = Array.from(byVersion.values()).sort((a, b) => a.version_no - b.version_no);
    if (cleanVersions.length === 0) {
      cleanVersions.push({ version_no: 1, name, formula, timeframe });
    }
    const versionsJson = JSON.stringify(cleanVersions);

    try {
      // A single statement creates the parent strategy and every imported version.
      // If any insert fails, PostgreSQL rolls back the whole statement for this item.
      const result = await sql`
        WITH new_strategy AS (
          INSERT INTO strategies (user_id, name, formula, timeframe)
          SELECT ${auth.user.userId}, ${name}, ${formula}, ${timeframe}
          WHERE NOT EXISTS (
            SELECT 1 FROM strategies
            WHERE user_id = ${auth.user.userId} AND name = ${name}
          )
          RETURNING id, name, formula, timeframe
        ),
        version_input AS (
          SELECT version_no, name, formula, timeframe
          FROM jsonb_to_recordset(${versionsJson}::jsonb)
            AS v(version_no integer, name text, formula text, timeframe text)
        ),
        new_versions AS (
          INSERT INTO strategy_versions (
            strategy_id, version_no, name, formula, timeframe,
            source_backtest_strategy_id, source_backtest_strategy_version,
            source_backtest_strategy_name, source_backtest_strategy_trigger
          )
          SELECT s.id, v.version_no, v.name, v.formula, v.timeframe,
                 NULL, NULL, NULL, NULL
          FROM new_strategy s
          CROSS JOIN version_input v
          RETURNING strategy_id, version_no
        )
        SELECT s.id, COUNT(v.version_no)::int AS version_count
        FROM new_strategy s
        JOIN new_versions v ON v.strategy_id = s.id
        GROUP BY s.id
      `;

      if (result.rows[0]) {
        imported += 1;
      } else {
        skipped.push(name);
      }
    } catch (error) {
      console.error('[strategies/import] item error:', error);
      skipped.push(name);
    }
  }

  return NextResponse.json({ imported, skipped });
}

import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'edge';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

const validTimeframe = (value: unknown) => ['D', 'W', 'M'].includes(String(value));

async function importSelectionStrategies(userId: string, items: unknown[]) {
  if (items.length > 100) throw new Error('单次最多导入 100 个选股策略');

  let imported = 0;
  const skipped: string[] = [];

  for (const item of items) {
    const name = String((item as any)?.name || '').trim();
    const formula = String((item as any)?.formula || '').trim();
    const timeframe = String((item as any)?.timeframe || 'D').trim();
    if (!name || !formula || !validTimeframe(timeframe)) {
      skipped.push(name || '(未命名)');
      continue;
    }

    const rawVersions = Array.isArray((item as any)?.versions) ? (item as any).versions : [];
    if (rawVersions.length > 100) {
      skipped.push(name);
      continue;
    }

    const byVersion = new Map<number, { version_no: number; name: string; formula: string; timeframe: string }>();
    for (const version of rawVersions) {
      const versionNo = Number((version as any)?.version_no);
      const versionName = String((version as any)?.name || name).trim();
      const versionFormula = String((version as any)?.formula || formula).trim();
      const versionTimeframe = String((version as any)?.timeframe || timeframe).trim();
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

    try {
      const result = await sql`
        WITH new_strategy AS (
          INSERT INTO strategies (user_id, name, formula, timeframe)
          SELECT ${userId}, ${name}, ${formula}, ${timeframe}
          WHERE NOT EXISTS (
            SELECT 1 FROM strategies
            WHERE user_id = ${userId} AND name = ${name}
          )
          RETURNING id
        ),
        version_input AS (
          SELECT version_no, name, formula, timeframe
          FROM jsonb_to_recordset(${JSON.stringify(cleanVersions)}::jsonb)
            AS v(version_no integer, name text, formula text, timeframe text)
        ),
        new_versions AS (
          // Imported JSON is untrusted: never carry uploaded provenance into persisted versions.
          INSERT INTO strategy_versions (
            strategy_id, version_no, name, formula, timeframe,
            source_backtest_strategy_id, source_backtest_strategy_version,
            source_backtest_strategy_name, source_backtest_strategy_trigger
          )
          SELECT s.id, v.version_no, v.name, v.formula, v.timeframe,
                 NULL, NULL, NULL, NULL
          FROM new_strategy s
          CROSS JOIN version_input v
          RETURNING strategy_id
        )
        SELECT s.id, COUNT(v.strategy_id)::int AS version_count
        FROM new_strategy s
        JOIN new_versions v ON v.strategy_id = s.id
        GROUP BY s.id
      `;
      if (result.rows[0]) imported += 1;
      else skipped.push(name);
    } catch (error) {
      console.error('[strategies/import] item error:', error);
      skipped.push(name);
    }
  }

  return { imported, skipped };
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  let body: any;
  try { body = await req.json(); } catch {
    return NextResponse.json({ error: '导入文件格式无效' }, { status: 400 });
  }

  // Backward compatibility: old files use "strategies" for selection strategies.
  const selectionItems = Array.isArray(body?.strategies) ? body.strategies : [];
  const backtestItems = Array.isArray(body?.backtest_strategies) ? body.backtest_strategies : [];

  if (selectionItems.length === 0 && backtestItems.length === 0) {
    return NextResponse.json({ error: '没有可导入的策略' }, { status: 400 });
  }
  if (selectionItems.length > 100 || backtestItems.length > 100) {
    return NextResponse.json({ error: '单次每种类型最多导入 100 个策略' }, { status: 400 });
  }

  const selection = await importSelectionStrategies(auth.user.userId, selectionItems);

  let importedBacktests = 0;
  let skippedBacktests: string[] = [];
  if (backtestItems.length > 0) {
    const cookie = req.headers.get('cookie') || '';
    const response = await fetch(new URL('/api/v1/backtest-strategy-templates/import', BACKTEST_NODE), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Cookie: cookie,
      },
      body: JSON.stringify({ templates: backtestItems }),
      cache: 'no-store',
      signal: AbortSignal.timeout(10000),
    });

    const text = await response.text();
    let payload: any = {};
    try { payload = JSON.parse(text); } catch {}

    if (!response.ok) {
      const message = String(payload?.detail || payload?.error || '回测策略导入失败');
      return NextResponse.json({
        error: message,
        imported: selection.imported,
        imported_backtests: 0,
        skipped: selection.skipped,
        skipped_backtests: backtestItems.map((item: any) => String(item?.name || '(未命名)')),
      }, { status: response.status === 401 ? 401 : 503 });
    }

    importedBacktests = Number(payload?.imported || 0);
    skippedBacktests = Array.isArray(payload?.skipped)
      ? payload.skipped.map((item: unknown) => String(item))
      : [];
  }

  return NextResponse.json({
    imported: selection.imported,
    imported_backtests: importedBacktests,
    skipped: selection.skipped,
    skipped_backtests: skippedBacktests,
  });
}

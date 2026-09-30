import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAuth } from '@/lib/auth';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'nodejs';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

function validTimeframe(value: unknown): value is 'D' | 'W' | 'M' {
  return value === 'D' || value === 'W' || value === 'M';
}

function validTrigger(value: unknown): string {
  return typeof value === 'string' &&
    ['condition', 'cross_above', 'cross_below'].includes(value)
    ? value
    : 'condition';
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) return NextResponse.json({ error: '未登录' }, { status: auth.status });
  await ensureSelectionStrategyVersions();

  let body: any;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
  }

  const strategyId = Number(body?.backtest_strategy_id);
  const requestedVersion = body?.version_no == null ? null : Number(body.version_no);
  if (!Number.isInteger(strategyId) || strategyId <= 0) {
    return NextResponse.json({ error: '无效的回测策略 ID' }, { status: 400 });
  }
  if (requestedVersion != null && (!Number.isInteger(requestedVersion) || requestedVersion <= 0)) {
    return NextResponse.json({ error: '无效的回测策略版本' }, { status: 400 });
  }

  const cookie = req.headers.get('cookie');
  if (!cookie) return NextResponse.json({ error: '未登录' }, { status: 401 });

  try {
    const response = await fetch(
      new URL('/api/v1/backtest-strategy-templates/' + strategyId + '/versions', BACKTEST_NODE),
      {
        headers: { Accept: 'application/json', Cookie: cookie },
        signal: AbortSignal.timeout(10000),
      },
    );
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return NextResponse.json(
        { error: payload?.detail || payload?.error || '无法读取回测策略版本' },
        { status: response.status },
      );
    }

    const versions = Array.isArray(payload?.versions) ? payload.versions : [];
    const version = requestedVersion == null
      ? versions[0]
      : versions.find((item: any) => Number(item.version_no) === requestedVersion);

    if (!version) {
      return NextResponse.json({ error: '回测策略版本不存在或无权访问' }, { status: 404 });
    }

    const entry = version?.config?.strategy?.entry;
    const formula = typeof entry?.condition === 'string' ? entry.condition.trim() : '';
    const timeframe = entry?.timeframe;
    const trigger = validTrigger(entry?.trigger);
    if (!formula || !validTimeframe(timeframe)) {
      return NextResponse.json({ error: '该回测策略没有可提取的有效 Entry 条件' }, { status: 400 });
    }

    const requestedName = typeof body?.name === 'string' ? body.name.trim() : '';
    const name = requestedName || (String(version.name || '回测策略') + ' · Entry选股');
    if (!name || name.length > 80) {
      return NextResponse.json({ error: '选股策略名称无效（最多 80 个字符）' }, { status: 400 });
    }

    // The source version is owner-verified by Node1 above. Create the strategy and
    // its first immutable version in one statement to prevent partial records.
    const result = await sql`
      WITH new_strategy AS (
        INSERT INTO strategies (
          user_id, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger
        )
        VALUES (
          ${auth.user.userId}, ${name}, ${formula}, ${timeframe},
          ${strategyId}, ${Number(version.version_no)},
          ${String(version.name || '')}, ${trigger}
        )
        RETURNING id, name, formula, timeframe, created_at, updated_at,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger
      ),
      new_version AS (
        INSERT INTO strategy_versions (
          strategy_id, version_no, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger
        )
        SELECT id, 1, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger
        FROM new_strategy
        RETURNING strategy_id, version_no
      )
      SELECT s.*, v.version_no
      FROM new_strategy s
      JOIN new_version v ON v.strategy_id = s.id
    `;

    const strategy = result.rows[0];
    if (!strategy) throw new Error('strategy and initial version insert returned no row');

    return NextResponse.json({
      strategy,
      source: {
        backtest_strategy_id: strategyId,
        backtest_strategy_version: Number(version.version_no),
        backtest_strategy_name: String(version.name || ''),
        entry_trigger: trigger,
      },
    }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') {
      return NextResponse.json({ error: '已存在同名选股策略' }, { status: 409 });
    }
    console.error('[strategies/extract-from-backtest] error:', error);
    return NextResponse.json({ error: '提取选股策略失败' }, { status: 500 });
  }
}

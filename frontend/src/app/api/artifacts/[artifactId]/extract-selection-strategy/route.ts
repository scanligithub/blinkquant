import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { sql } from '@/lib/db';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'nodejs';

const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';

export async function POST(req: NextRequest, { params }: { params: Promise<{ artifactId: string }> }) {
  const auth = await requireAuth(req);
  if (auth.status !== 200 || !auth.user?.userId) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  const artifactId = Number((await params).artifactId);
  if (!Number.isInteger(artifactId) || artifactId <= 0) return NextResponse.json({ error: 'Invalid artifact ID' }, { status: 400 });
  await ensureSelectionStrategyVersions();

  const qs = new URLSearchParams({ user_id: auth.user.userId });
  if (auth.user.role) qs.set('role', auth.user.role);
  const response = await fetch(NODE1_URL + '/internal/artifacts/' + artifactId + '?' + qs.toString(), {
    headers: { Authorization: 'Bearer ' + INTERNAL_TOKEN }, cache: 'no-store',
  });
  const artifact = await response.json().catch(() => ({}));
  if (!response.ok) return NextResponse.json(artifact, { status: response.status });
  if (artifact.artifact_type !== 'backtest') return NextResponse.json({ error: '只有回测成果可以提取为选股策略' }, { status: 400 });

  const payload = artifact.payload && typeof artifact.payload === 'object' ? artifact.payload : {};
  const strategy = payload.strategy && typeof payload.strategy === 'object' ? payload.strategy : {};
  const entry = strategy.entry && typeof strategy.entry === 'object' ? strategy.entry : {};
  const formula = String(entry.condition || '').trim();
  const timeframe = String(entry.timeframe || 'D').trim();
  if (!formula) return NextResponse.json({ error: '该回测成果没有可提取的入场条件公式' }, { status: 400 });
  if (!['D', 'W', 'M'].includes(timeframe)) return NextResponse.json({ error: '回测入场周期无法作为选股策略周期' }, { status: 400 });

  let body: any = {};
  try { body = await req.json(); } catch { /* optional body */ }
  const requestedName = String(body?.name || '').trim();
  const name = requestedName || (String(artifact.title || ('回测成果 #' + artifactId)) + ' · 选股策略').slice(0, 80);
  if (name.length > 80) return NextResponse.json({ error: '策略名称不能超过 80 个字符' }, { status: 400 });

  const sourceTemplateId = artifact.strategy_template_id == null ? null : Number(artifact.strategy_template_id);
  const sourceTemplateVersion = artifact.strategy_template_version == null ? null : Number(artifact.strategy_template_version);
  const sourceTemplateName = artifact.strategy_template_name ? String(artifact.strategy_template_name) : null;
  const sourceTemplateTrigger = entry.trigger == null ? null : String(entry.trigger);
  const sourceTitle = String(artifact.title || '');

  try {
    const result = await sql`
      WITH new_strategy AS (
        INSERT INTO strategies (
          user_id, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger,
          source_backtest_artifact_id, source_backtest_artifact_title
        )
        VALUES (
          ${auth.user.userId}, ${name}, ${formula}, ${timeframe},
          ${sourceTemplateId}, ${sourceTemplateVersion},
          ${sourceTemplateName}, ${sourceTemplateTrigger},
          ${artifactId}, ${sourceTitle}
        )
        RETURNING id, name, formula, timeframe, created_at, updated_at
      ),
      new_version AS (
        INSERT INTO strategy_versions (
          strategy_id, version_no, name, formula, timeframe,
          source_backtest_strategy_id, source_backtest_strategy_version,
          source_backtest_strategy_name, source_backtest_strategy_trigger,
          source_backtest_artifact_id, source_backtest_artifact_title
        )
        SELECT id, 1, name, formula, timeframe,
          ${sourceTemplateId}, ${sourceTemplateVersion},
          ${sourceTemplateName}, ${sourceTemplateTrigger},
          ${artifactId}, ${sourceTitle}
        FROM new_strategy
        RETURNING strategy_id, version_no
      )
      SELECT s.id, s.name, s.formula, s.timeframe, s.created_at, s.updated_at, v.version_no
      FROM new_strategy s JOIN new_version v ON v.strategy_id = s.id
    `;
    return NextResponse.json({
      strategy: result.rows[0],
      provenance: {
        source_backtest_artifact_id: artifactId,
        source_backtest_artifact_title: sourceTitle,
        source_backtest_strategy_id: sourceTemplateId,
        source_backtest_strategy_version: sourceTemplateVersion,
      },
    }, { status: 201 });
  } catch (error: any) {
    if (error?.code === '23505') return NextResponse.json({ error: '已存在同名策略' }, { status: 409 });
    console.error('[artifact/extract-selection-strategy] error:', error);
    return NextResponse.json({ error: '提取选股策略失败' }, { status: 500 });
  }
}

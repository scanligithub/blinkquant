import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { sql } from '@/lib/db';
import { ensureSelectionStrategyVersions } from '@/lib/strategy-versions';

export const runtime = 'nodejs';

const BACKTEST_NODE = 'https://scanli-blinkquant-node1.hf.space';

async function auth(req: NextRequest) {
  return requireAuth(req);
}

async function forward(req: NextRequest, method: string) {
  const result = await auth(req);
  if (!result.user) return NextResponse.json({ error: 'Unauthorized' }, { status: result.status });

  let body: string | undefined;
  if (method !== 'GET' && method !== 'DELETE') {
    const rawBody = await req.text();
    try {
      const parsed = JSON.parse(rawBody);
      if (parsed?.config?.source_selection_strategy != null) {
        await ensureSelectionStrategyVersions();
        const sourceId = Number(parsed.config.source_selection_strategy.id);
        const sourceVersion = Number(parsed.config.source_selection_strategy.version_no);
        if (!Number.isInteger(sourceId) || sourceId <= 0 || !Number.isInteger(sourceVersion) || sourceVersion <= 0) {
          return NextResponse.json({ error: '来源选股策略引用无效' }, { status: 400 });
        }

        const sourceStrategy = await sql`
          SELECT id, name, formula, timeframe
          FROM strategies
          WHERE id = ${sourceId} AND user_id = ${result.user.userId}
          LIMIT 1
        `;
        if (!sourceStrategy.rows[0]) {
          return NextResponse.json({ error: '来源选股策略不存在或无权访问' }, { status: 400 });
        }

        const sourceVersionRow = await sql`
          SELECT version_no, name, formula, timeframe
          FROM strategy_versions
          WHERE strategy_id = ${sourceId} AND version_no = ${sourceVersion}
          LIMIT 1
        `;
        if (!sourceVersionRow.rows[0]) {
          return NextResponse.json({ error: '来源选股策略版本不存在或无权访问' }, { status: 400 });
        }

        const version = sourceVersionRow.rows[0];
        parsed.config.source_selection_strategy = {
          id: sourceId,
          version_no: Number(version.version_no),
          name: String(version.name),
          formula: String(version.formula),
          timeframe: String(version.timeframe),
        };
      }
      body = JSON.stringify(parsed);
    } catch {
      return NextResponse.json({ error: '请求格式无效' }, { status: 400 });
    }
  }
  const url = new URL('/api/v1/backtest-strategy-templates', BACKTEST_NODE);
  if (method === 'PUT' || method === 'DELETE') {
    const id = req.nextUrl.searchParams.get('id');
    if (id) url.searchParams.set('id', id);
  }

  // Node1 re-validates the browser session through Vercel /api/auth/session.
  // Do not pass a client-controlled user id; the authenticated identity must
  // come from the same signed session cookie that requireAuth() accepted here.
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  const cookie = req.headers.get('cookie');
  if (cookie) headers.Cookie = cookie;

  const response = await fetch(url, {
    method,
    headers,
    body,
    signal: AbortSignal.timeout(10000),
  });
  const text = await response.text();
  let payload: unknown;
  try { payload = JSON.parse(text); } catch { payload = { error: text.slice(0, 500) }; }
  return NextResponse.json(payload, { status: response.status });
}

export async function GET(req: NextRequest) { return forward(req, 'GET'); }
export async function POST(req: NextRequest) { return forward(req, 'POST'); }
export async function PUT(req: NextRequest) { return forward(req, 'PUT'); }
export async function DELETE(req: NextRequest) { return forward(req, 'DELETE'); }

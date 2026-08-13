import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildSystemPrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLText,
  validateFormula,
  type NLMeta,
} from '@/lib/selectNL';

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

const LLM_ENDPOINT = process.env.LLM_ENDPOINT;
const LLM_API_KEY = process.env.LLM_API_KEY;
const LLM_MODEL = process.env.LLM_MODEL;
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 15000);

const META_TTL_MS = 24 * 60 * 60 * 1000;
let metaCache: { at: number; data: NLMeta } | null = null;
const rateStore = new Map<string, { timestamps: number[] }>();

async function fetchNlMeta(): Promise<NLMeta> {
  if (metaCache && Date.now() - metaCache.at < META_TTL_MS) return metaCache.data;
  const result = await Promise.any(
    NODES.map(async (nodeUrl) => {
      const res = await fetch(`${nodeUrl}/api/v1/nl-meta`, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`Node responded with ${res.status}`);
      return res.json();
    })
  );
  metaCache = { at: Date.now(), data: result as NLMeta };
  return result as NLMeta;
}

export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  if (!LLM_ENDPOINT || !LLM_API_KEY || !LLM_MODEL) {
    return NextResponse.json({ error: 'AI 选股未配置', code: 'NOT_CONFIGURED' }, { status: 503 });
  }

  const key = `select-nl:${auth.user.userId}`;
  const now = Date.now();
  const limit = checkRateLimit(rateStore, key, now);
  if (!limit.allowed) {
    return NextResponse.json(
      { error: '调用过于频繁，请稍后再试', code: 'RATE_LIMITED', retryAfterMs: limit.retryAfterMs },
      { status: 429 }
    );
  }

  let body: { query?: string };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '请求体非法' }, { status: 400 });
  }
  const query = (body.query || '').trim();
  if (!query) {
    return NextResponse.json({ error: '请输入选股需求' }, { status: 400 });
  }

  try {
    const meta = await fetchNlMeta();
    const systemPrompt = buildSystemPrompt(meta);

    const llmRes = await fetch(LLM_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${LLM_API_KEY}` },
      body: JSON.stringify({
        model: LLM_MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: query },
        ],
        temperature: 0,
      }),
      signal: AbortSignal.timeout(LLM_TIMEOUT_MS),
    });
    if (!llmRes.ok) {
      throw new Error(`LLM HTTP ${llmRes.status}`);
    }
    const llmJson = await llmRes.json();
    const raw = llmJson?.choices?.[0]?.message?.content ?? '';

    const parsed = parseSelectNLText(raw);
    const validation = validateFormula(meta, parsed.formula);
    if (validation.ok === false) {
      return NextResponse.json(
        {
          error: `翻译结果不合法：${validation.reason}`,
          code: 'INVALID_FORMULA',
          formula: parsed.formula,
          explanation: parsed.explanation,
        },
        { status: 400 }
      );
    }
    if (!meta.timeframes.includes(parsed.timeframe)) {
      return NextResponse.json(
        { error: `翻译结果周期不合法：${parsed.timeframe}`, code: 'INVALID_FORMULA' },
        { status: 400 }
      );
    }

    recordRequest(rateStore, key, now);

    return NextResponse.json({
      success: true,
      data: { formula: parsed.formula, timeframe: parsed.timeframe, explanation: parsed.explanation },
    });
  } catch (err) {
    console.error('select-nl failed:', err);
    return NextResponse.json(
      { error: 'AI 选股服务暂不可用，请稍后再试或改用公式', code: 'LLM_UNAVAILABLE' },
      { status: 502 }
    );
  }
}

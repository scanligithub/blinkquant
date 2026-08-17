import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildSystemPrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLText,
  validateFormula,
  findMagnitudeMismatch,
  type AnalyzeResult,
  type SelectNLResult,
} from '@/lib/selectNL';
import { fetchNlMeta, callLlm, rateStore, LLM_ENDPOINT, LLM_API_KEY, LLM_MODEL, NL_TEST_MODE } from '@/lib/selectNLServer';

export const runtime = 'nodejs';
export const maxDuration = 60;

// 翻译端点（两步翻译的第二步）：输入已确认的语义分析（/api/select-nl/analyze 的产物），
// 将其机械映射为公式。弱模型不再需要从原始中文语义直接翻译。
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
  // 限流策略：checkRateLimit 检查、成功翻译后才 recordRequest。
  // 失败尝试（LLM 错误 / 公式非法）不扣配额，避免用户被误伤；
  // 代价是恶意用户可以无限次失败 LLM 调用烧 token——当前接受（配合 Vercel 每实例内存 Map 的近似性）。
  // 测试模式（NL_TEST_MODE=true）跳过限流，仅供自动化测试使用。
  const limit = NL_TEST_MODE ? null : checkRateLimit(rateStore, key, now);
  if (limit && !limit.allowed) {
    return NextResponse.json(
      { error: '调用过于频繁，请稍后再试', code: 'RATE_LIMITED', retryAfterMs: limit.retryAfterMs },
      { status: 429 }
    );
  }

  let body: { analysis?: AnalyzeResult };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: '请求体非法' }, { status: 400 });
  }
  const analysis = body.analysis;
  if (
    !analysis ||
    typeof analysis.restatement !== 'string' ||
    analysis.restatement.trim().length === 0 ||
    !Array.isArray(analysis.conditions) ||
    analysis.conditions.length === 0
  ) {
    return NextResponse.json({ error: '缺少已确认的语义分析' }, { status: 400 });
  }

  try {
    const meta = await fetchNlMeta();
    const systemPrompt = buildSystemPrompt(meta);
    const startedAt = Date.now();

    // 把「已确认的语义」注入 user 消息；弱模型按条件清单+逻辑翻译，无需自行理解中文歧义。
    const conditionLines = analysis.conditions
      .map((c, i) => `${i + 1}. ${c}`)
      .join('\n');
    const baseQuery = [
      '这是用户需求对应的已确认语义，请严格按它翻译成 BlinkQuant 公式：',
      `需求复述：${analysis.restatement}`,
      `条件清单：\n${conditionLines}`,
      `逻辑关系：${analysis.logic}`,
      `周期：${analysis.timeframe}`,
    ].join('\n');

    // 单次 LLM 调用（Hobby 60s 硬限下无重试预算）；量级兑底不符时允许追加提示重翻一次。
    const attempt = async (query: string) => {
      const raw = await callLlm(systemPrompt, query);
      let parsed: SelectNLResult;
      try {
        parsed = parseSelectNLText(raw);
      } catch {
        return { kind: 'invalid' as const, reason: 'AI 翻译输出格式异常，请返回确认语义或换种说法' };
      }
      const validation = validateFormula(meta, parsed.formula);
      if (validation.ok === false) {
        return {
          kind: 'invalid' as const,
          reason: `翻译结果不合法：${validation.reason}`,
          formula: parsed.formula,
          explanation: parsed.explanation,
        };
      }
      if (!meta.timeframes.includes(parsed.timeframe)) {
        return {
          kind: 'invalid' as const,
          reason: `翻译结果周期不合法：${parsed.timeframe}`,
          formula: parsed.formula,
          explanation: parsed.explanation,
        };
      }
      const mismatch = findMagnitudeMismatch(analysis, parsed.formula);
      if (mismatch) return { kind: 'mismatch' as const, reason: mismatch, parsed };
      return { kind: 'ok' as const, parsed };
    };

    let result = await attempt(baseQuery);
    if (result.kind === 'mismatch' && Date.now() - startedAt < 40000) {
      // 追加量级兑底提示重翻一次；正式计数前失败不触发限流。
      result = await attempt(`${baseQuery}\n注意：${result.reason}。请重新换算后输出完整 JSON。`);
    }
    if (result.kind !== 'ok') {
      const payload = result.kind === 'invalid'
        ? { error: result.reason, code: 'INVALID_FORMULA', formula: (result as any).formula, explanation: (result as any).explanation }
        : { error: `翻译结果不合法：${result.reason}`, code: 'INVALID_FORMULA' };
      return NextResponse.json(payload, { status: 400 });
    }

    // 只有成功翻译才计入限流配额（策略见上方注释）
    if (!NL_TEST_MODE) recordRequest(rateStore, key, now);

    return NextResponse.json({
      success: true,
      data: { formula: result.parsed.formula, timeframe: result.parsed.timeframe, explanation: result.parsed.explanation },
    });
  } catch (err) {
    console.error('select-nl failed:', err);
    return NextResponse.json(
      { error: 'AI 选股服务暂不可用，请稍后再试或改用公式', code: 'LLM_UNAVAILABLE' },
      { status: 502 }
    );
  }
}

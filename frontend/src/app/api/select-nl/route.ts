import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildSystemPrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLText,
  validateFormula,
  findMagnitudeMismatch,
  shouldRetryTranslation,
  buildTranslateUserMessage,
  buildRepairSystemSuffix,
  buildRepairUserMessage,
  trySafeBollRefRewrite,
  trySafeAbsAbsRewrite,
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

    // 首次翻译 user 消息（含备选值硬约束，追加自 analyze 已确认语义）
    const userMsg = buildTranslateUserMessage(analysis);

    // 单/双次 LLM 调用（Hobby 60s 硬限下最多一次 repair，且仅在时间预算内）；
    // 弱模型首次输出可能非法或量级漂移，用「非法公式原文 + reason + repair system」修正一次。
    // 返回 SelectNLResult 表示成功；否则返回非法明细。
    const translateOnce = async (
      sys: string,
      user: string
    ): Promise<SelectNLResult | { kind: 'invalid'; reason: string; formula?: string; explanation?: string }> => {
      const raw = await callLlm(sys, user);
      let parsed: SelectNLResult;
      try {
        parsed = parseSelectNLText(raw);
      } catch {
        return { kind: 'invalid', reason: 'AI 翻译输出格式异常，请返回确认语义或换种说法' };
      }
      // 安全 BOLL 改写 / ABS 双向展开还原（零 token），成功后直接过检则省一次 repair
      let rewritten = trySafeBollRefRewrite(parsed.formula);
      if (!rewritten) rewritten = trySafeAbsAbsRewrite(parsed.formula);
      if (rewritten) parsed = { ...parsed, formula: rewritten };
      const validation = validateFormula(meta, parsed.formula);
      if (validation.ok === false) {
        return {
          kind: 'invalid',
          reason: `翻译结果不合法：${validation.reason}`,
          formula: parsed.formula,
          explanation: parsed.explanation,
        };
      }
      if (!meta.timeframes.includes(parsed.timeframe)) {
        return {
          kind: 'invalid',
          reason: `翻译结果周期不合法：${parsed.timeframe}`,
          formula: parsed.formula,
          explanation: parsed.explanation,
        };
      }
      const mismatch = findMagnitudeMismatch(analysis, parsed.formula);
      if (mismatch) return { kind: 'invalid', reason: mismatch, formula: parsed.formula, explanation: parsed.explanation };
      return parsed;
    };

    let result = await translateOnce(systemPrompt, userMsg);
    if (result && (result as any).kind === 'invalid') {
      const denied = result as { kind: 'invalid'; reason: string; formula?: string; explanation?: string };
      const elapsedMs = Date.now() - startedAt;
      if (shouldRetryTranslation(denied, elapsedMs, 40000)) {
        // repair：专属 system + 带「非法公式原文 + reason」的 user 消息；正式计数前失败不触发限流。
        const repairSystem = systemPrompt + buildRepairSystemSuffix();
        const repairUser = buildRepairUserMessage(analysis, denied.formula ?? '', denied.reason);
        result = await translateOnce(repairSystem, repairUser);
      }
    }
    if (result && (result as any).kind === 'invalid') {
      const denied = result as { kind: 'invalid'; reason: string; formula?: string; explanation?: string };
      return NextResponse.json(
        {
          error: denied.reason.startsWith('翻译结果不合法') ? denied.reason : `翻译结果不合法：${denied.reason}`,
          code: 'INVALID_FORMULA',
          formula: denied.formula,
          explanation: denied.explanation,
        },
        { status: 400 }
      );
    }
    const parsed = result as SelectNLResult;

    // 只有成功翻译才计入限流配额（策略见上方注释）
    if (!NL_TEST_MODE) recordRequest(rateStore, key, now);

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

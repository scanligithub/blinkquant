import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildSystemPrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLText,
  validateFormula,
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

    // 把「已确认的语义」注入 user 消息；弱模型按条件清单+逻辑翻译，无需自行理解中文歧义。
    const conditionLines = analysis.conditions
      .map((c, i) => `${i + 1}. ${c}`)
      .join('\n');
    const query = [
      '这是用户需求对应的已确认语义，请严格按它翻译成 BlinkQuant 公式：',
      `需求复述：${analysis.restatement}`,
      `条件清单：\n${conditionLines}`,
      `逻辑关系：${analysis.logic}`,
      `周期：${analysis.timeframe}`,
    ].join('\n');

    // 单次 LLM 调用（Hobby 60s 硬限下无重试预算；非 JSON 输出直接报错，用户可返回重试）
    const raw = await callLlm(systemPrompt, query);
    let parsed: SelectNLResult;
    try {
      parsed = parseSelectNLText(raw);
    } catch {
      return NextResponse.json(
        { error: 'AI 翻译输出格式异常，请返回确认语义或换种说法', code: 'INVALID_LLM' },
        { status: 400 }
      );
    }

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

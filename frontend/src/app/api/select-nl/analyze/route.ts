import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import {
  buildAnalyzePrompt,
  checkRateLimit,
  recordRequest,
  parseSelectNLAnalysis,
  type AnalyzeResult,
} from '@/lib/selectNL';
import { fetchNlMeta, callLlm, rateStore, LLM_ENDPOINT, LLM_API_KEY, LLM_MODEL } from '@/lib/selectNLServer';

export const runtime = 'nodejs';
export const maxDuration = 60;

// 语义分析端点（两步翻译的第一步）：
// 只做语义拆解，不产公式。用户确认/纠正后由 /api/select-nl 完成翻译。
export async function POST(req: NextRequest) {
  const auth = await requireAuth(req);
  if (!auth.user) {
    return NextResponse.json({ error: '未登录' }, { status: auth.status });
  }

  if (!LLM_ENDPOINT || !LLM_API_KEY || !LLM_MODEL) {
    return NextResponse.json({ error: 'AI 选股未配置', code: 'NOT_CONFIGURED' }, { status: 503 });
  }

  const key = `select-nl-analyze:${auth.user.userId}`;
  const now = Date.now();
  // 分析端点独立限流（与翻译端点同 Map 不同 key），成功分析才计配额。
  const limit = checkRateLimit(rateStore, key, now);
  if (!limit.allowed) {
    return NextResponse.json(
      { error: '调用过于频繁，请稍后再试', code: 'RATE_LIMITED', retryAfterMs: limit.retryAfterMs },
      { status: 429 }
    );
  }

  let body: { query?: string; correction?: string; previous?: AnalyzeResult };
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
    const systemPrompt = buildAnalyzePrompt(meta);

    // 首轮：只有 query；纠正轮：query + correction + previous（原需求 + 用户纠正 + 上次分析）。
    // 弱模型结合「原需求+纠正+上次结果」做修订，比只给纠正语更稳定。
    const parts: string[] = [`原始需求：${query}`];
    const correction = (body.correction || '').trim();
    const previous = body.previous;
    if (correction && previous && Array.isArray(previous.conditions)) {
      parts.push(`用户纠正：${correction}`);
      parts.push(`之前的语义分析：${previous.restatement}`);
      parts.push(`之前的条件清单：`);
      previous.conditions.forEach((c, i) => parts.push(`${i + 1}. ${c}`));
      parts.push('请根据纠正意见修订你的语义分析，输出修订后的完整 JSON。');
    } else {
      parts.push('请输出你对这条需求的中文语义分析 JSON。');
    }

    // 单次 LLM 调用（Hobby 60s 硬限下无重试预算）
    const raw = await callLlm(systemPrompt, parts.join('\n'));
    let analyzed: AnalyzeResult;
    try {
      analyzed = parseSelectNLAnalysis(raw);
    } catch {
      return NextResponse.json(
        { error: 'AI 语义分析输出格式异常，请重新输入或换种说法', code: 'INVALID_LLM' },
        { status: 400 }
      );
    }

    if (!meta.timeframes.includes(analyzed.timeframe)) {
      return NextResponse.json(
        { error: `语义分析周期不合法：${analyzed.timeframe}`, code: 'INVALID_FORMULA' },
        { status: 400 }
      );
    }

    // 只有成功分析才计入限流配额
    recordRequest(rateStore, key, now);

    return NextResponse.json({ success: true, data: analyzed });
  } catch (err) {
    console.error('select-nl analyze failed:', err);
    return NextResponse.json(
      { error: 'AI 选股服务暂不可用，请稍后再试或改用公式', code: 'LLM_UNAVAILABLE' },
      { status: 502 }
    );
  }
}

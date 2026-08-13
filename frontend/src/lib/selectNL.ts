// src/lib/selectNL.ts
// 纯函数：自然语言选股 LLM 管道的提示词构建 / JSON 解析 / 公式强校验 / 限流

export interface NLMeta {
  fields: string[];
  indicators: string[];
  timeframes: string[];
  units: Record<string, string>;
  example_queries: string[];
}

export interface SelectNLResult {
  formula: string;
  timeframe: string;
  explanation: string;
}

export const MAX_FORMULA_LENGTH = 500;

const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

function escapeRe(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function stripCodeFence(raw: string): string {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

export function parseSelectNLText(raw: string): SelectNLResult {
  const cleaned = stripCodeFence(raw);
  let parsed: any;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    throw new Error('LLM 输出不是合法 JSON');
  }
  if (!parsed || typeof parsed !== 'object') throw new Error('LLM 输出不是合法 JSON');
  if (typeof parsed.formula !== 'string' || parsed.formula.trim().length === 0) {
    throw new Error('翻译结果缺少 formula');
  }
  const explanation = typeof parsed.explanation === 'string' ? parsed.explanation.trim() : '';
  const timeframe = typeof parsed.timeframe === 'string' ? parsed.timeframe.toUpperCase() : 'D';
  return { formula: parsed.formula.trim(), timeframe, explanation };
}

export function validateFormula(
  meta: NLMeta,
  formula: string
): { ok: true } | { ok: false; reason: string } {
  if (typeof formula !== 'string' || formula.trim().length === 0) {
    return { ok: false, reason: '公式为空' };
  }
  if (formula.length > MAX_FORMULA_LENGTH) {
    return { ok: false, reason: `公式过长（上限 ${MAX_FORMULA_LENGTH} 字符）` };
  }
  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);

  // 1. 函数调用形态校验：FUNC(FIELD, N)，参数必须恰好 (字段, 正整数)
  const callRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(([^()]*)\\)`, 'g');
  let m: RegExpExecArray | null;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    const args = m[2].split(',').map((s) => s.trim());
    if (args.length !== 2) return { ok: false, reason: `函数 ${func} 必须恰好 2 个参数` };
    if (!fields.has(args[0])) return { ok: false, reason: `函数 ${func} 第一参数 ${args[0]} 不在字段白名单` };
    if (!/^\d+$/.test(args[1]) || Number(args[1]) <= 0) {
      return { ok: false, reason: `函数 ${func} 第二参数必须是正整数` };
    }
  }

  // 2. 其余大写标识符必须 ∈ 白名单（NOT 已移除——后端不支持 ast.Not）
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t: RegExpExecArray | null;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }

  // 3. 非法字符（防注入到 AST 之外）
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };

  return { ok: true };
}

export function buildSystemPrompt(meta: NLMeta): string {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units)
    .map(([k, v]) => `${k}=${v}`)
    .join('，');
  return [
    '你是一名 A 股量化选股公式翻译助手。请把用户的中文选股需求翻译成 BlinkQuant 公式。',
    '字段白名单（只能使用这些，大小写必须一致）：',
    fieldsLine,
    '',
    '单位：',
    unitsLine,
    '',
    '单位换算规则（重要）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。例如"总市值大于100亿"应表达为 TOTAL_MV > 1e10。',
    '',
    `可选指标函数（只能使用，参数形态 FUNC(字段, 正整数窗口)：${meta.indicators.join('、')}。`,
    '',
    `周期 timeframe 只能是 ${meta.timeframes.join('/')}。`,
    '',
    '示例：',
    meta.example_queries.join('\n'),
    '',
    '输出必须是合法 JSON：{"formula":"...","timeframe":"D","explanation":"中文解释"}。',
    '只输出 JSON，不要输出其他文字。',
  ].join('\n');
}

export interface RateWindow {
  timestamps: number[];
}

export function checkRateLimit(
  store: Map<string, RateWindow>,
  key: string,
  now: number,
  limitPerMinute = 3,
  limitPerDay = 20
): { allowed: boolean; remaining: number; retryAfterMs: number } {
  const MINUTE = 60 * 1000;
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  const recent = win.timestamps.filter((ts) => now - ts < DAY);
  const minuteCount = recent.filter((ts) => now - ts < MINUTE).length;
  if (minuteCount >= limitPerMinute) {
    const last = recent[recent.length - 1];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, MINUTE - (now - last)) };
  }
  if (recent.length >= limitPerDay) {
    const first = recent[0];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, DAY - (now - first)) };
  }
  return { allowed: true, remaining: limitPerDay - recent.length, retryAfterMs: 0 };
}

export function recordRequest(store: Map<string, RateWindow>, key: string, now: number): void {
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  win.timestamps = [...win.timestamps.filter((ts) => now - ts < DAY), now];
  store.set(key, win);
}

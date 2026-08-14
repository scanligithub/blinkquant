// src/lib/selectNL.ts
// 纯函数：自然语言选股 LLM 管道的提示词构建 / JSON 解析 / 公式强校验 / 限流

export interface NLMeta {
  fields: string[];
  indicators: string[];
  timeframes: string[];
  units: Record<string, string>;
  example_queries: string[];
  signatures: Record<string, string[]>;
  descriptions: Record<string, string>;
}

export interface SelectNLResult {
  formula: string;
  timeframe: string;
  explanation: string;
}

export const MAX_FORMULA_LENGTH = 500;

const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

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

const COMPARE_STR = '>=|<=|>|<';
const POS_INT_MAX = 500;
const ARITH_MAX_OPS = 3;

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
  let depth = 0;
  for (const ch of formula) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return { ok: false, reason: '公式括号不配对' };
  }
  if (depth !== 0) return { ok: false, reason: '公式括号不配对' };
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };

  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);
  const sigs = meta.signatures ?? {};

  // 1. 校验所有函数调用签名（含嵌套，正则全局扫描）
  const callRegex = /([A-Z_][A-Z0-9_]*)\s*\(/g;
  let m: RegExpExecArray | null;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    // AND/OR 是布尔逻辑词，后跟 ( 是括号条件而非函数调用
    if (func === 'AND' || func === 'OR') continue;
    const sig = sigs[func];
    if (!sig) return { ok: false, reason: `函数 ${func} 未注册` };
    const openIdx = m.index + m[0].length - 1;
    const closeIdx = matchParen(formula, openIdx);
    if (closeIdx === -1) return { ok: false, reason: '公式括号不配对' };
    const argStr = formula.slice(openIdx + 1, closeIdx);
    const args = splitTopLevel(argStr, ',').map((s) => s.trim());
    const v = validateCallArgs(meta, sig, args, func);
    if (v.ok === false) return v;
    callRegex.lastIndex = closeIdx + 1;
  }

  // 2. 其余大写标识符必须 ∈ 白名单
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t: RegExpExecArray | null;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }
  return { ok: true };
}

function matchParen(s: string, openIdx: number): number {
  let d = 0;
  for (let i = openIdx; i < s.length; i++) {
    if (s[i] === '(') d++;
    else if (s[i] === ')') d--;
    if (d === 0) return i;
  }
  return -1;
}

function splitTopLevel(s: string, sep: string): string[] {
  const parts: string[] = [];
  let d = 0, cur = '';
  for (const ch of s) {
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (ch === sep && d === 0) { parts.push(cur); cur = ''; }
    else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur);
  return parts;
}

function splitBoolTopLevel(s: string): string[] {
  const out: string[] = [];
  let d2 = 0, seg = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d2++;
    else if (ch === ')') d2--;
    if (d2 === 0 && (s.slice(i, i + 3) === 'AND' || s.slice(i, i + 2) === 'OR')) {
      const word = s.slice(i, i + 3) === 'AND' ? 'AND' : 'OR';
      const beforeOk = i === 0 || !/[A-Z0-9_]/.test(s[i - 1]);
      const afterIdx = i + word.length;
      const afterOk = afterIdx >= s.length || !/[A-Z0-9_]/.test(s[afterIdx]);
      if (beforeOk && afterOk) {
        out.push(seg.trim());
        seg = '';
        i = afterIdx - 1;
        continue;
      }
    }
    seg += ch;
  }
  if (seg.trim() !== '') out.push(seg.trim());
  return out.filter((x) => x !== '');
}

function validateCallArgs(
  meta: NLMeta,
  sig: string[],
  args: string[],
  func: string
): { ok: true } | { ok: false; reason: string } {
  if (args.length !== sig.length) {
    return { ok: false, reason: `函数 ${func} 必须恰好 ${sig.length} 个参数` };
  }
  for (let i = 0; i < sig.length; i++) {
    const kind = sig[i];
    const arg = args[i];
    if (kind === 'field') {
      if (!meta.fields.includes(arg)) {
        return { ok: false, reason: `函数 ${func} 参数 ${arg} 不在字段白名单` };
      }
    } else if (kind === 'pos_int') {
      if (!/^\d+$/.test(arg) || Number(arg) < 1 || Number(arg) > POS_INT_MAX) {
        return { ok: false, reason: `函数 ${func} 窗口必须是 1-${POS_INT_MAX} 正整数` };
      }
    } else if (kind === 'series') {
      if (!isSeriesExpr(meta, arg)) {
        return { ok: false, reason: `函数 ${func} 参数 ${arg} 必须是字段或窗口指标调用` };
      }
    } else if (kind === 'cond') {
      if (!isCondExpr(meta, arg)) {
        return { ok: false, reason: `函数 ${func} 条件参数不合法` };
      }
    }
  }
  return { ok: true };
}

function stripOuterParens(tok: string): string {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s: string): string[] {
  // 按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符
  const parts: string[] = [];
  let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (d === 0 && '+-*/'.includes(ch)) {
      // 处理 1e-3：'-' 前是 e/E 时不作为操作符（不切分）
      if ((ch === '-' || ch === '+') && /[eE]/.test(s[i - 1] ?? '')) { cur += ch; continue; }
      parts.push(cur.trim());
      cur = '';
    } else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur.trim());
  return parts;
}

function countTopLevelOps(tok: string): number {
  return splitArithTopLevel(stripOuterParens(tok)).length - 1;
}

function isArithExpr(meta: NLMeta, tok: string): boolean {
  const parts = splitArithTopLevel(stripOuterParens(tok));
  if (parts.length < 2) return false;
  if (countTopLevelOps(tok) > ARITH_MAX_OPS) return false;
  return parts.every((p) => isSeriesExpr(meta, p) || isNumber(p) || isArithExpr(meta, p));
}

function isSeriesExpr(meta: NLMeta, tok: string): boolean {
  if (meta.fields.includes(tok)) return true;
  // 1. 平衡外括号剥离：'(CLOSE-OPEN)' → 'CLOSE-OPEN'（后端 ast 对括号透明，前端需显式剥）
  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 函数调用路径：仅当 call 的闭合括号恰在末尾（纯调用，尾部无残留）
  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(/.exec(tok);
  if (mm) {
    const sig = meta.signatures?.[mm[1]];
    const openIdx = mm.index + mm[0].length - 1;
    if (sig && !sig.includes('cond')) {
      const closeIdx = matchParen(tok, openIdx);
      if (closeIdx === tok.length - 1) {
        const argStr = tok.slice(openIdx + 1, closeIdx);
        const args = splitTopLevel(argStr, ',').map((s) => s.trim());
        if (validateCallArgs(meta, sig, args, mm[1]).ok === true) return true;
      }
    }
  }
  // 3. 算术表达式路径：函数调用路径不匹配（非纯调用/尾部有残留/非调用）时尝试
  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}

function isCondExpr(meta: NLMeta, tok: string): boolean {
  const parts = splitBoolTopLevel(tok);
  if (parts.length === 0) return false;
  return parts.every((p) => isCompareExpr(meta, p));
}

function isCompareExpr(meta: NLMeta, expr: string): boolean {
  const m = new RegExp(`^(.*?)\\s*(${COMPARE_STR})\\s*(.*)$`).exec(expr.trim());
  if (!m) return false;
  const left = m[1].trim();
  const right = m[3].trim();
  // 有意比后端更严格：后端允许常量在任一侧（仅拒绝双常量），此处要求左侧为 series，与 spec 一致
  if (!isSeriesExpr(meta, left)) return false;
  if (!isSeriesExpr(meta, right) && !isNumber(right)) return false;
  return true;
}

function isNumber(s: string): boolean {
  return /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(s);
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
    `可选算子（函数名(参数形态)：含义）：`,
    ...Object.entries(meta.descriptions ?? {})
      .map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
    '',
    'CROSS_UP/CROSS_DOWN/MAX/MIN 参数可嵌套指标调用（如 CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))），但不支持更深嵌套。',
    'COUNT/BARSLAST 的条件参数是比较表达式（> >= < <=），可用 AND/OR 组合。',
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
    const oldest = recent[recent.length - minuteCount];
    return { allowed: false, remaining: 0, retryAfterMs: Math.max(0, MINUTE - (now - oldest)) };
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

// LLM 调用超时：默认 50s，钳制上限 55s（Vercel Hobby 函数硬限 60s，必须留余量）
export const LLM_TIMEOUT_DEFAULT_MS = 50000;
export const LLM_TIMEOUT_CAP_MS = 55000;

export function resolveLlmTimeout(raw?: string | null): number {
  const v = raw === undefined || raw === null || raw === '' ? NaN : Number(raw);
  if (Number.isNaN(v) || v < 1000) return LLM_TIMEOUT_DEFAULT_MS;
  return Math.min(v, LLM_TIMEOUT_CAP_MS);
}

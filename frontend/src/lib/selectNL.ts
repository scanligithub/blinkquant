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

// 两步语义翻译（语义确认 → 公式翻译）的第一阶段输出
export interface AnalyzeResult {
  restatement: string;
  conditions: string[];
  logic: string;
  timeframe: string;
  date?: string;
}

export const MAX_FORMULA_LENGTH = 500;

const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

export function stripCodeFence(raw: string): string {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

// 兜底：LLM 偶尔在 JSON 前后夹带自然语言，提取首个平衡的大括号对象
function extractFirstJsonObject(raw: string): string | null {
  const start = raw.indexOf('{');
  if (start === -1) return null;
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < raw.length; i++) {
    const ch = raw[i];
    if (inStr) {
      if (esc) esc = false;
      else if (ch === '\\') esc = true;
      else if (ch === '"') inStr = false;
      continue;
    }
    if (ch === '"') { inStr = true; continue; }
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return raw.slice(start, i + 1);
    }
  }
  return null;
}

export function parseSelectNLText(raw: string): SelectNLResult {
  const cleaned = stripCodeFence(raw);
  let parsed: any;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    // 尝试提取首个对象（可能只是被自然语言包裹）
    const obj = extractFirstJsonObject(cleaned);
    if (obj) {
      try {
        parsed = JSON.parse(obj);
      } catch {
        throw new Error('LLM 输出不是合法 JSON');
      }
    } else {
      throw new Error('LLM 输出不是合法 JSON');
    }
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
    '单位换算规则（重要，数值必须精确换算）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。换算时把数字乘以对应倍数：',
    '1亿=1e8，100亿=1e10，200亿=2e10，5亿=5e8；1万=1e4，5000万=5e7，20万=2e5。',
    '注意：5000万=5e7（不是5e9），200亿=2e10（不是2e11）。',
    '',
    `可选算子（函数名(参数形态)：含义）：`,
    ...Object.entries(meta.descriptions ?? {})
      .map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
    '',
    'CROSS_UP/CROSS_DOWN/MAX/MIN 参数可嵌套一层指标调用（如 CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))），禁止更深嵌套。',
    'REF 的第一个参数必须是字段（如 REF(CLOSE,1)），禁止嵌套调用（REF(MA(...),1)、REF(HHV(...),1)、REF(BOLL_UPPER(...),1) 均非法）。',
    'COUNT(cond, N) 必须 2 个参数；BARSLAST(cond) 只能 1 个条件参数。条件为比较式（> >= < <=），可用 AND/OR。',
    '示例：连续3天放量 = COUNT(VOL > REF(VOL,1), 3) >= 3；距上次站上20日线 = BARSLAST(CLOSE > MA(CLOSE,20))。',
    '',
    '【易错模式 — 必须遵守】',
    '1) 创N日新高/新低：',
    '   正确: CLOSE >= HHV(CLOSE,N) ； CLOSE <= LLV(CLOSE,N)（优先 >= / <=，不要用 ==）',
    '   错误: BARSLAST(...)、REF(HHV(...),1)、REF(LLV(...),1)',
    '2) 近N日振幅 / 振幅大于x%：',
    '   正确: (HIGH-LOW)/CLOSE*100 > x  或  ATR(N) 相关比较',
    '   错误: BARSLAST(...)、把振幅写成「距上次」类条件',
    '3) 上破/下破布林轨：',
    '   正确: CLOSE > BOLL_UPPER(CLOSE,20,2) 或 CROSS_UP(CLOSE, BOLL_UPPER(CLOSE,20,2))',
    '         CLOSE < BOLL_LOWER(CLOSE,20,2) 或 CROSS_DOWN(CLOSE, BOLL_LOWER(CLOSE,20,2))',
    '   错误: REF(BOLL_UPPER(...),1)、REF(BOLL_LOWER(...),1)',
    '4) 不要用未在清单中的算子；不要为「昨天的均线/布林」去写 REF(指标调用,1)。',
    '5) 绝对偏差/偏离/乖离：用 ABS(序列) 比较（如 ABS(CLOSE - MA(CLOSE,20)) > 2），禁止展开成 OR 双向不等式。',
    '6) 较高者/较大值/较小值：用 MAX(A,B)/MIN(A,B) 直接比较（如 CROSS_UP(MAX(OPEN,CLOSE), MA(CLOSE,20))）；',
    '   禁止把 MAX/MIN 作为外层 MA/REF/HHV/LLV/SUM 等 window 函数的参数（MA(MAX(...),N)、REF(MAX(...),1) 均非法）。',
    '7) 距上次站上/跌破均线等「距上次」类条件：BARSLAST 的条件必须是简单比较式（如 BARSLAST(CLOSE > MA(CLOSE,20)) 站上 / BARSLAST(CLOSE < MA(CLOSE,20)) 跌破）；',
    '   禁止把 CROSS_UP/CROSS_DOWN 函数调用作为 BARSLAST 的条件（BARSLAST(CROSS_UP(...))、BARSLAST(CROSS_DOWN(...)) 均非法）。',
    '8) MACD 金叉/死叉：用 CROSS_UP(MACD_DIF(f,s), MACD_DEA(f,s,si)) / CROSS_DOWN(...)；MACD 柱>0 用 MACD_HIST(f,s,si) > 0。',
    '9) DMI/ADX 金叉：用 CROSS_UP(DMI_PDI(N), DMI_MDI(N))；ADX 强弱用 DMI_ADX(N) > 25 直接比较。',
    '10) 零参算子必须写括号：OBV()、BBI()、SAR()、UO()，禁止写裸名 OBV/BBI/SAR/UO。',
    '11) CCI 突破用 CCI(N) > 100；WR 超买 WR(N) > 80、超卖 WR(N) < 20；MFI 用 MFI(N) < 20。',
    '12) 涨停/跌停：必须用预计算标志字段（仅支持周期 D）：',
    '   收盘涨停（封板）= IS_LIMIT_UP == 1 ；盘中触及涨停（触板）= IS_TOUCH_LIMIT_UP == 1。',
    '   收盘跌停 = IS_LIMIT_DOWN == 1 ；盘中触及跌停 = IS_TOUCH_LIMIT_DOWN == 1。',
    '   禁止用 PCT_CHG >= LIMIT_UP_PCT 判断涨停（实际涨停价按 0.01 元修约，真实涨停的当日涨幅常低于限幅，如 9.96%，会漏选）。',
    '   LIMIT_UP_PCT 仅用于「涨幅达到板块限幅」这类字面需求；任何情况下禁止写死 10/20/30。',
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

export function parseSelectNLAnalysis(raw: string): AnalyzeResult {
  const cleaned = stripCodeFence(raw);
  let parsed: any;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    throw new Error('LLM 输出不是合法 JSON');
  }
  if (!parsed || typeof parsed !== 'object') throw new Error('LLM 输出不是合法 JSON');
  if (typeof parsed.restatement !== 'string' || parsed.restatement.trim().length === 0) {
    throw new Error('分析结果缺少 restatement');
  }
  if (!Array.isArray(parsed.conditions)) throw new Error('分析结果缺少 conditions');
  const conditions = parsed.conditions
    .filter((c) => typeof c === 'string' && c.trim().length > 0)
    .map((c) => c.trim());
  if (conditions.length === 0) throw new Error('分析结果缺少 conditions');
  const logic = typeof parsed.logic === 'string' && parsed.logic.trim() !== '' ? parsed.logic.trim() : conditions.map((_, i) => `${i + 1}`).join(' AND ');
  const timeframe = typeof parsed.timeframe === 'string' ? parsed.timeframe.toUpperCase() : 'D';
  // 可选日期：仅接受严格 ISO 格式，其余（相对表述/非法值）一律丢弃，走后端默认最新交易日
  const date =
    typeof parsed.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(parsed.date) ? parsed.date : undefined;
  return { restatement: parsed.restatement.trim(), conditions, logic, timeframe, ...(date ? { date } : {}) };
}

const UNIT_FACTORS: Record<string, number> = { 万亿: 1e12, 亿: 1e8, 万: 1e4 };
const UNIT_RE = /(\d+(?:\.\d+)?)\s*(万亿|亿|万)/g;
const NUM_LITERAL_RE = /\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;

// 翻译尝试结果（route 内 attempt 的统一形状）
export interface TranslateAttemptResult {
  kind: 'ok' | 'invalid' | 'mismatch';
  reason?: string;
  formula?: string;
  explanation?: string;
  parsed?: SelectNLResult;
}

// 重试决策：invalid/mismatch 在时间预算内可再翻一次；ok 直接通过。
// 时间预算用于守 Vercel Hobby 60s 硬限，避免重试把首调用慢的请求拖超时。
export function shouldRetryTranslation(result: TranslateAttemptResult, elapsedMs: number, budgetMs: number): boolean {
  if (result.kind === 'ok') return false;
  return elapsedMs < budgetMs;
}

// 量级兑底：从已确认语义中提取带「万亿/亿/万」的数值阈值，
// 校验公式里是否存在数值量级与之接近的常量。用于拦截弱模型把 200亿=2e11 这类换算漂移。
// stricter than the translation prompt; a mismatch here means we should retry the translation once.
export function findMagnitudeMismatch(analysis: AnalyzeResult, formula: string): string | null {
  const text = [analysis.restatement, ...analysis.conditions].filter(Boolean).join('\n');
  const expected: { raw: string; value: number }[] = [];
  let m: RegExpExecArray | null;
  UNIT_RE.lastIndex = 0;
  while ((m = UNIT_RE.exec(text)) !== null) {
    const n = Number(m[1]);
    const factor = UNIT_FACTORS[m[2]];
    const value = n * factor;
    if (!Number.isFinite(value) || value <= 0) continue;
    // 同一量级只保留一次，避免 restatement 与 conditions 重复
    if (!expected.some((e) => Math.abs(e.value - value) / value < 1e-9)) {
      expected.push({ raw: m[0], value });
    }
  }
  if (expected.length === 0) return null;

  const constants = (formula.match(NUM_LITERAL_RE) ?? []).map(Number).filter((v) => Number.isFinite(v) && v > 0);

  for (const e of expected) {
    const hit = constants.some((c) => c >= e.value * 0.5 && c <= e.value * 1.5);
    if (!hit) return `需求中「${e.raw}」应换算为 ${e.value}，但公式未出现该量级数值`;
  }
  return null;
}

// 根据已确认语义追加硬约束（补丁式，针对 e1/e4/i5 类）
export function buildHardConstraintSuffix(analysis: AnalyzeResult): string {
  const text = [analysis.restatement, ...(analysis.conditions || [])].join('\n');
  const lines: string[] = [];
  if (/新高|新低/.test(text)) {
    lines.push('硬约束：本需求含新高/新低，禁止使用 BARSLAST；请用 HHV/LLV 与 CLOSE 的 >= 或 <= 比较。');
  }
  if (/振幅/.test(text)) {
    lines.push('硬约束：本需求与振幅相关，禁止使用 BARSLAST；优先 (HIGH-LOW)/CLOSE*100 或 ATR。');
  }
  if (/布林|BOLL/i.test(text)) {
    lines.push('硬约束：布林相关禁止 REF(BOLL_UPPER/LOWER(...), n)；上破/下破用 CLOSE 与 BOLL_* 比较或 CROSS_UP/CROSS_DOWN(CLOSE, BOLL_*(...))。');
  }
  if (/绝对偏差|偏离|乖离/.test(text)) {
    lines.push('硬约束：本需求含绝对偏差/偏离，禁止展开成 OR 双向不等式；请用 ABS(值A - 值B) 或 ABS(值A - 值B) > N 比较。');
  }
  if (/较高者|较大值|取较大|较小值|较低者|取较小/.test(text)) {
    lines.push('硬约束：本需求含「较高者/较大值/较小值」，请用 MAX(参数A, 参数B) 或 MIN(参数A, 参数B)；禁止把 MAX/MIN 作为外层 MA/REF/HHV/LLV/SUM 等 window 函数的参数（如 MA(MAX(...),N)、REF(MAX(...),1) 非法）。');
  }
  if (/涨停|跌停|封板|一字板|触板|炸板/.test(text)) {
    lines.push('硬约束：本需求含涨停/跌停语义，必须使用预计算标志字段（周期 D）：收盘涨停 IS_LIMIT_UP == 1；盘中触及涨停 IS_TOUCH_LIMIT_UP == 1；收盘跌停 IS_LIMIT_DOWN == 1；盘中触及跌停 IS_TOUCH_LIMIT_DOWN == 1。禁止用 PCT_CHG 与 LIMIT_UP_PCT 比较，禁止写死 10/20/30 数值。');
  }
  return lines.length ? '\n' + lines.join('\n') : '';
}

// 首次翻译的 user 消息
export function buildTranslateUserMessage(analysis: AnalyzeResult): string {
  const conditionLines = analysis.conditions.map((c, i) => `${i + 1}. ${c}`).join('\n');
  return [
    '这是用户需求对应的已确认语义，请严格按它翻译成 BlinkQuant 公式：',
    `需求复述：${analysis.restatement}`,
    `条件清单：\n${conditionLines}`,
    `逻辑关系：${analysis.logic}`,
    `周期：${analysis.timeframe}`,
    ...(analysis.date ? [`查询交易日：${analysis.date}（date 由系统单独处理，公式中不要写任何日期条件）`] : []),
    buildHardConstraintSuffix(analysis),
  ].join('\n');
}

// repair 专用 system：在全量 buildSystemPrompt 之后追加，比整段重写更聚焦。
export function buildRepairSystemSuffix(): string {
  return [
    '',
    '【公式修复模式】',
    '上一次输出的公式未通过签名校验。请只输出修正后的合法 JSON（formula/timeframe/explanation）。',
    '规则：',
    '- 只修改公式使之满足函数签名与白名单，不要改变用户已确认的语义意图；',
    '- 禁止引入未列出的算子；禁止 REF/MA/HHV/LLV/SUM 等 window 函数的第一参写成函数调用；',
    '- BARSLAST 只能有 1 个 cond 参数；COUNT 必须是 COUNT(cond, N)；',
    '- 若上次误用 BARSLAST 表达新高/新低/振幅，改为 HHV/LLV 或 (HIGH-LOW)/CLOSE；',
    '- 若上次写成 REF(BOLL_*(...),1)，改为 CLOSE 与 BOLL_* 比较或 CROSS_UP/CROSS_DOWN(CLOSE, BOLL_*(...))；',
    '- 若上次把绝对偏差/偏离展开成 OR 双向不等式，改为 ABS(值A - 值B) > N 形式；',
    '- 若上次把 MAX/MIN 写进 MA/REF/HHV/LLV/SUM 等 window 函数参数（如 MA(MAX(...),N)、REF(MAX(...),1)），改为用 MAX/MIN 直接比较（如 CROSS_UP(MAX(OPEN,CLOSE), MA(CLOSE,20))）；',
    '- 若上次把 CROSS_UP/CROSS_DOWN 函数调用作为 BARSLAST/COUNT 的条件参数，改为简单比较式（如 BARSLAST(CLOSE < MA(CLOSE,20)) > 5）。',
    '- 若上次写了裸名 OBV/BBI/SAR/UO 或漏了括号，改为带括号调用（OBV()/BBI()/SAR()/UO()）；',
    '- 若上次把 CROSS_UP/CROSS_DOWN 用于 DMI，参数应为 DMI_PDI(N)/DMI_MDI(N)；',
  ].join('\n');
}

// repair 的 user 消息：必须带上被拒公式原文 + reason
export function buildRepairUserMessage(
  analysis: AnalyzeResult,
  rejectedFormula: string,
  reason: string
): string {
  return [
    buildTranslateUserMessage(analysis),
    '',
    '上次非法公式：',
    rejectedFormula,
    `校验失败原因：${reason}`,
    '请输出修正后的完整 JSON。',
  ].join('\n');
}

// 唯一安全的确定性改写：REF(BOLL_UPPER|BOLL_LOWER(...), 1) 与 CLOSE 比较/意图为突破时，
// 改为 CROSS_UP/CROSS_DOWN(CLOSE, BOLL_*(...))。不改写 REF(MA(...),1)（昨日均线是合法语义，只是不支持）。
export function trySafeBollRefRewrite(formula: string): string | null {
  const up = formula.match(
    /CLOSE\s*>\s*REF\s*\(\s*(BOLL_UPPER\s*\([^)]*\))\s*,\s*1\s*\)|REF\s*\(\s*(BOLL_UPPER\s*\([^)]*\))\s*,\s*1\s*\)\s*<\s*CLOSE/i
  );
  if (up) {
    const inner = up[1] || up[2];
    return formula.replace(up[0], `CROSS_UP(CLOSE, ${inner})`);
  }
  const down = formula.match(
    /CLOSE\s*<\s*REF\s*\(\s*(BOLL_LOWER\s*\([^)]*\))\s*,\s*1\s*\)|REF\s*\(\s*(BOLL_LOWER\s*\([^)]*\))\s*,\s*1\s*\)\s*>\s*CLOSE/i
  );
  if (down) {
    const inner = down[1] || down[2];
    return formula.replace(down[0], `CROSS_DOWN(CLOSE, ${inner})`);
  }
  return null;
}

// 确定性还原：弱模型把「绝对偏差」误展开成双向不等式（如 CLOSE >= MA+2 OR CLOSE <= MA-2），
// 收敛回 ABS(差值) 形式。
// 仅同一左值 X、同一基准 B、同一阈值 N，且 OR 两侧 = X>=(B+N) 与 X<=(B-N) 精确匹配时改写；
// B 为字段或单层函数调用（含嵌套括号）。任一不符即返回 null，不误改其他 OR 组合。
export function trySafeAbsAbsRewrite(formula: string): string | null {
  const parts = splitBoolTopLevel(formula);
  if (parts.length !== 2) return null;
  const leftM =
    /^([A-Z_][A-Z0-9_]*)\s*>=\s*((?:[A-Z_][A-Z0-9_]*\s*\([^)]*\))|[A-Z_][A-Z0-9_]*)\s*\+\s*(\d+(?:\.\d+)?)$/.exec(
      parts[0]
    );
  if (!leftM) return null;
  const [, x, base, n] = leftM;
  const rightM = new RegExp(
    `^${escapeRegExp(x)}\\s*<=\\s*${escapeRegExp(base)}\\s*-\\s*${escapeRegExp(n)}$`
  ).exec(parts[1]);
  if (!rightM) return null;
  const rewritten = `ABS(${x} - ${base}) > ${n}`;
  let depth = 0;
  for (const ch of rewritten) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return null;
  }
  if (depth !== 0) return null;
  return rewritten;
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// 确定性还原：弱模型把「CCI 上穿/突破 N」误写成 CROSS_UP(X, MA(N,1))、CROSS_UP(X, MA(X,1)) 或
// X>N AND REF(X,1)<=N，收敛回直接比较 X > N / X < N（提示词已指明 CCI 突破用 CCI(N) > 100）。
// 仅 X 为单值字段/算子调用时改写；N 来自公式字面量，或退化为 MA(X,1) 时从已确认分析恢复阈值；
// 形态与阈值任一不满足即返回 null，不误改其他公式。
export function trySafeNumericCrossRewrite(formula: string, analysis?: AnalyzeResult): string | null {
  const x = '([A-Z_][A-Z0-9_]*\\s*\\([^)]*\\)|[A-Z_][A-Z0-9_]*)';
  const n = '([0-9]+(?:\\.[0-9]+)?)';
  const m1 = formula.match(new RegExp(`^CROSS_UP\\s*\\(\\s*${x}\\s*,\\s*MA\\s*\\(\\s*${n}\\s*,\\s*1\\s*\\)\\s*\\)$`, 'i'));
  if (m1) return `${m1[1]} > ${m1[2]}`;
  const m2 = formula.match(new RegExp(`^CROSS_DOWN\\s*\\(\\s*${x}\\s*,\\s*MA\\s*\\(\\s*${n}\\s*,\\s*1\\s*\\)\\s*\\)$`, 'i'));
  if (m2) return `${m2[1]} < ${m2[2]}`;
  // 弱模型把数值线误写成同序列：CROSS_UP(CCI(14), MA(CCI(14), 1))，从已确认分析恢复阈值
  const xcall = '([A-Z_][A-Z0-9_]*\\s*\\([^)]*\\))';
  const mUp = formula.match(new RegExp(`^CROSS_UP\\s*\\(\\s*${xcall}\\s*,\\s*MA\\s*\\(\\s*\\1\\s*,\\s*1\\s*\\)\\s*\\)$`, 'i'));
  if (mUp) {
    const lvl = numericLevelFromAnalysis(analysis);
    if (lvl !== null) return `${mUp[1]} > ${lvl}`;
  }
  const mDn = formula.match(new RegExp(`^CROSS_DOWN\\s*\\(\\s*${xcall}\\s*,\\s*MA\\s*\\(\\s*\\1\\s*,\\s*1\\s*\\)\\s*\\)$`, 'i'));
  if (mDn) {
    const lvl = numericLevelFromAnalysis(analysis);
    if (lvl !== null) return `${mDn[1]} < ${lvl}`;
  }
  const m3 = formula.match(new RegExp(`^${x}\\s*>\\s*${n}\\s+AND\\s+REF\\s*\\(\\s*\\1\\s*,\\s*1\\s*\\)\\s*<=\\s*\\2$`, 'i'));
  if (m3) return `${m3[1]} > ${m3[2]}`;
  const m4 = formula.match(new RegExp(`^${x}\\s*<\\s*${n}\\s+AND\\s+REF\\s*\\(\\s*\\1\\s*,\\s*1\\s*\\)\\s*>=\\s*\\2$`, 'i'));
  if (m4) return `${m4[1]} < ${m4[2]}`;
  return null;
}

// 确定性还原：弱模型把「涨停/跌停」写成 PCT_CHG 与写死数值或 LIMIT_UP_PCT 的比较（旧口径会漏选
// 实际涨幅 9.96%~9.99% 的真实涨停），收敛回预计算标志字段。
// 收盘封板 → IS_LIMIT_UP == 1；需求明确「触及/触板/盘中」→ IS_TOUCH_LIMIT_UP == 1；跌停同理。
// 仅当 analysis 文本含涨停/跌停关键词 且 公式整体精确匹配旧形式时才改写，否则返回 null（不误改「涨幅大于5%」）。
export function trySafeLimitUpDownRewrite(formula: string, analysis?: AnalyzeResult): string | null {
  if (!analysis) return null;
  const text = [...(analysis.conditions || []), analysis.restatement || ''].join(' ');
  if (!/涨停|跌停|封板|一字板|触板|炸板/.test(text)) return null;
  const wantTouch = /触及|触板|盘中|炸板/.test(text);
  const upTarget = wantTouch ? 'IS_TOUCH_LIMIT_UP == 1' : 'IS_LIMIT_UP == 1';
  const downTarget = wantTouch ? 'IS_TOUCH_LIMIT_DOWN == 1' : 'IS_LIMIT_DOWN == 1';
  const up = /^PCT_CHG\s*(>=|>)\s*(?:(?:10|20|30)(?:\.0+)?|LIMIT_UP_PCT)$/.exec(formula.trim());
  if (up) return upTarget;
  const down = /^PCT_CHG\s*(<=|<)\s*(?:-\s*|0\s*-\s*)?(?:(?:10|20|30)(?:\.0+)?|LIMIT_UP_PCT)$/.exec(formula.trim());
  if (down) return downTarget;
  const downInvert = /^0\s*-\s*PCT_CHG\s*(>=|>)\s*(?:(?:10|20|30)(?:\.0+)?|LIMIT_UP_PCT)$/.exec(formula.trim());
  if (downInvert) return downTarget;
  // 已是旧推荐形式 PCT_CHG <= 0 - LIMIT_UP_PCT 的变体已覆盖；其余不动
  return null;
}

// 从已确认分析（conditions+restatement）提取突破方向后的数值阈值；无则返回 null。
function numericLevelFromAnalysis(analysis?: AnalyzeResult): string | null {
  if (!analysis) return null;
  const text = [...(analysis.conditions || []), analysis.restatement || ''].join(' ');
  const m = text.match(/(?:突破|上穿|上破|下穿|下破|大于|小于|高于|低于|>|<)\s*(\d+(?:\.\d+)?)/);
  return m ? m[1] : null;
}

export function buildAnalyzePrompt(meta: NLMeta): string {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units)
    .map(([k, v]) => `${k}=${v}`)
    .join('，');
  const indicatorsLine = Object.entries(meta.descriptions ?? {})
    .map(([k, d]) => `${k}: ${d}`)
    .join('\n');
  return [
    '你是一名 A 股量化选股需求理解助手。请把用户的中文选股需求拆解成清晰的语义，供用户确认。',
    '不要直接输出公式，不要做任何公式翻译。',
    '字段白名单（理解需求时可能用到的数据维度，大小写必须一致）：',
    fieldsLine,
    '',
    '单位：',
    unitsLine,
    '',
    '单位换算规则（重要，数值必须精确换算）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。例如"总市值大于100亿"的阈值是 1e10；"5000万"=5e7；"200亿"=2e10；"5亿"=5e8。',
    '注意：5000万=5e7（不是5e9），200亿=2e10（不是2e11）。',
    '',
    '可选指标（函数名(参数形态)：含义），理解需求时明确指标语义，尤其是歧义术语（如"振幅""新高""乖离"）：',
    ...Object.entries(meta.descriptions ?? {}).map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
    '',
    '分析要求：',
    '1. 用中文复述你理解的需求（restatement），明确标出任何有歧义或需要用户确认的术语。',
    '2. 把需求拆成若干条独立的条件（conditions），每条是中文自然语言描述，并尽量指出对应字段/指标。',
    '3. 给出条件之间的逻辑关系（logic），用条件序号（1、2、3…）+ AND/OR，括号可省略。',
    '4. 给出周期 timeframe，只能是 ' + meta.timeframes.join('/') + '。',
    '5. 歧义术语在 conditions 中必须写成可比较的中文语义（仍不要输出公式）：',
    '   - 「创N日新高」→「收盘价大于等于近N日最高价」',
    '   - 「创N日新低」→「收盘价小于等于近N日最低价」',
    '   - 「近N日振幅」「振幅大于x%」→「（最高价-最低价）/收盘价 的百分比」或明确阈值；不要写成「距上次某条件成立」',
    '   - 「上破/下破布林上轨/下轨」→「收盘价大于/小于布林上轨/下轨」或「收盘价上穿/下穿布林轨」',
    '   - 「距上次…不超过N日」才对应 BARSLAST 类语义；「新高/新低/振幅」不要用 BARSLAST 语义描述',
    '   - 「涨停」（默认收盘封板）→「当日收盘价封在涨停价」；「盘中触及涨停/触板」→「当日最高价达到过涨停价」；「跌停」「触及跌停」同理。此类需求周期应选 D',
    '6. 若需求明确提到某个具体交易日（如「2026年8月20日」「2026-08-20」），输出 ISO 格式的 date 字段（YYYY-MM-DD）；',
    '   「今天/最新/当前」等相对表述不要输出 date（系统默认查询最新交易日）。',
    '',
    '输出必须是合法 JSON：{"restatement":"...","conditions":["...","..."],"logic":"1 AND 2","timeframe":"D"}。',
    '若用户提到具体交易日，额外输出 "date":"YYYY-MM-DD" 字段；否则不要输出 date。',
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

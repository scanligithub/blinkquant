// frontend/tests/select-nl.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// ---- 复制自 src/lib/selectNL.ts（保持与实现一致）----
const META = {
  fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN'],
  indicators: ['EMA', 'MA', 'REF', 'ROC', 'STD'],
  timeframes: ['D', 'W', 'M'],
  units: { TOTAL_MV: '元', FLOAT_MV: '元', TOTAL_SHARES: '股', FLOAT_SHARES: '股', AMOUNT: '元', VOL: '股', PE_TTM: '无量纲(倍)', PB_MRQ: '无量纲(倍)', TURN: '百分比(%)', FORECAST_YOY: '百分比(%)', PCT_CHG: '百分比(%)', S_CLOSE: '指数点位' },
  example_queries: ['CLOSE > MA(CLOSE, 20)', 'PE_TTM < 20 AND TOTAL_MV > 1e10'],
};
const MAX_FORMULA_LENGTH = 500;
const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

function stripCodeFence(raw) {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

function parseSelectNLText(raw) {
  const cleaned = stripCodeFence(raw);
  let parsed;
  try { parsed = JSON.parse(cleaned); }
  catch { throw new Error('LLM 输出不是合法 JSON'); }
  if (!parsed || typeof parsed !== 'object') throw new Error('LLM 输出不是合法 JSON');
  if (typeof parsed.formula !== 'string' || parsed.formula.trim().length === 0) throw new Error('翻译结果缺少 formula');
  const explanation = typeof parsed.explanation === 'string' ? parsed.explanation.trim() : '';
  const timeframe = typeof parsed.timeframe === 'string' ? parsed.timeframe.toUpperCase() : 'D';
  return { formula: parsed.formula.trim(), timeframe, explanation };
}

function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

function validateFormula(meta, formula) {
  if (typeof formula !== 'string' || formula.trim().length === 0) return { ok: false, reason: '公式为空' };
  if (formula.length > MAX_FORMULA_LENGTH) return { ok: false, reason: `公式过长（上限 ${MAX_FORMULA_LENGTH} 字符）` };
  // 括号配对检查（防嵌套调用/未闭合括号漏到后端 AST）
  let depth = 0;
  for (const ch of formula) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (depth < 0) return { ok: false, reason: '公式括号不配对' };
  }
  if (depth !== 0) return { ok: false, reason: '公式括号不配对' };

  // 嵌套调用检查：FUNC( 的参数内不允许再出现括号（防嵌套调用漏到后端 AST）
  const callStartRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(`, 'g');
  let cs;
  while ((cs = callStartRegex.exec(formula)) !== null) {
    const rest = formula.slice(callStartRegex.lastIndex);
    const openIdx = rest.indexOf('(');
    const closeIdx = rest.indexOf(')');
    if (openIdx !== -1 && openIdx < closeIdx) {
      return { ok: false, reason: '公式包含嵌套括号（不支持函数嵌套调用）' };
    }
  }
  const fields = new Set(meta.fields);
  const indicators = new Set(meta.indicators);
  const callRegex = new RegExp(`\\b(${meta.indicators.map(escapeRe).join('|')})\\s*\\(([^()]*)\\)`, 'g');
  let m;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
    const args = m[2].split(',').map((s) => s.trim());
    if (args.length !== 2) return { ok: false, reason: `函数 ${func} 必须恰好 2 个参数` };
    if (!fields.has(args[0])) return { ok: false, reason: `函数 ${func} 第一参数 ${args[0]} 不在字段白名单` };
    if (!/^\d+$/.test(args[1]) || Number(args[1]) <= 0) return { ok: false, reason: `函数 ${func} 第二参数必须是正整数` };
  }
  const tokenRegex = /[A-Z_][A-Z0-9_]*/g;
  let t;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }
  if (/[;'"]/.test(formula)) return { ok: false, reason: '公式包含非法字符' };
  return { ok: true };
}

function buildSystemPrompt(meta) {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units).map(([k, v]) => `${k}=${v}`).join('，');
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
    '可选指标函数（只能使用，参数形态 FUNC(字段, 正整数窗口)：' + meta.indicators.join('、') + '。',
    '',
    '周期 timeframe 只能是 ' + meta.timeframes.join('/') + '。',
    '',
    '示例：',
    meta.example_queries.join('\n'),
    '',
    '输出必须是合法 JSON：{"formula":"...","timeframe":"D","explanation":"中文解释"}。',
    '只输出 JSON，不要输出其他文字。',
  ].join('\n');
}

function checkRateLimit(store, key, now, limitPerMinute = 3, limitPerDay = 20) {
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

function recordRequest(store, key, now) {
  const DAY = 24 * 60 * 60 * 1000;
  const win = store.get(key) || { timestamps: [] };
  win.timestamps = [...win.timestamps.filter((ts) => now - ts < DAY), now];
  store.set(key, win);
}
// ---- 复制结束 ----

test('parseSelectNLText: 纯 JSON 正常解析', () => {
  const r = parseSelectNLText('{"formula":"CLOSE > MA(CLOSE, 20)","timeframe":"D","explanation":"突破20日均线"}');
  assert.deepEqual(r, { formula: 'CLOSE > MA(CLOSE, 20)', timeframe: 'D', explanation: '突破20日均线' });
});

test('parseSelectNLText: 代码围栏剥离', () => {
  const r = parseSelectNLText('```json\n{"formula":"PE_TTM < 20","timeframe":"d","explanation":"低估值"}\n```');
  assert.equal(r.formula, 'PE_TTM < 20');
  assert.equal(r.timeframe, 'D');
});

test('parseSelectNLText: 缺少 formula 抛错', () => {
  assert.throws(() => parseSelectNLText('{"explanation":"x"}'), /formula/);
});

test('parseSelectNLText: 非法 JSON 抛错', () => {
  assert.throws(() => parseSelectNLText('not json'), /JSON/);
});

test('validateFormula: 合法公式通过', () => {
  const r = validateFormula(META, 'CLOSE > MA(CLOSE, 20) AND PE_TTM < 30');
  assert.equal(r.ok, true);
});

test('validateFormula: 未知函数拒绝', () => {
  const r = validateFormula(META, 'KDJ(CLOSE, 9) > 50');
  assert.equal(r.ok, false);
  assert.match(r.reason, /KDJ/);
});

test('validateFormula: 未知字段拒绝', () => {
  const r = validateFormula(META, 'NOPE > 5');
  assert.equal(r.ok, false);
  assert.match(r.reason, /NOPE/);
});

test('validateFormula: 负窗口拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, -5) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: 非整数窗口拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, 2.5) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: 非法字符拒绝', () => {
  const r = validateFormula(META, 'CLOSE > 5; DROP');
  assert.equal(r.ok, false);
});

test('validateFormula: 超长公式拒绝', () => {
  const long = 'CLOSE > 1' + ' AND CLOSE > 1'.repeat(200);
  const r = validateFormula(META, long);
  assert.equal(r.ok, false);
});

test('validateFormula: 空公式拒绝', () => {
  const r = validateFormula(META, '   ');
  assert.equal(r.ok, false);
});

test('validateFormula: 括号不配对拒绝', () => {
  const r = validateFormula(META, 'CLOSE > (5');
  assert.equal(r.ok, false);
  assert.match(r.reason, /括号/);
});

test('validateFormula: 嵌套函数调用拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, MA(CLOSE, 20)) > 10');
  assert.equal(r.ok, false);
  assert.match(r.reason, /括号/);
});

test('validateFormula: NOT 拒绝（后端不支持 ast.Not）', () => {
  const r = validateFormula(META, 'NOT (CLOSE > 11)');
  assert.equal(r.ok, false);
  assert.match(r.reason, /NOT/);
});

test('buildSystemPrompt: 包含字段与单位与示例', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /PE_TTM/);
  assert.match(p, /TOTAL_MV=元/);
  assert.match(p, /CLOSE > MA\(CLOSE, 20\)/);
  assert.match(p, /timeframe/);
});

test('buildSystemPrompt: 包含单位换算规则(亿→1e8)', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /亿/);
  assert.match(p, /1e8/);
  assert.match(p, /1e10/);
});

test('checkRateLimit: 允许窗口内请求', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  const r = checkRateLimit(store, 'k', 1000);
  assert.equal(r.allowed, true);
});

test('checkRateLimit: 每分钟超过阈值拒绝', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  recordRequest(store, 'k', 1000);
  recordRequest(store, 'k', 2000);
  const r = checkRateLimit(store, 'k', 3000);
  assert.equal(r.allowed, false);
});

test('checkRateLimit: 每分钟阈值边界（3次/分内允许）', () => {
  const store = new Map();
  recordRequest(store, 'k', 0);
  recordRequest(store, 'k', 1000);
  const r = checkRateLimit(store, 'k', 2000); // 第 3 次请求仍允许
  assert.equal(r.allowed, true);
});

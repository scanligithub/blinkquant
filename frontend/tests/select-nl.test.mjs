// frontend/tests/select-nl.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// ---- 复制自 src/lib/selectNL.ts（保持与实现一致）----
const META = {
  fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN'],
  indicators: ['ABS', 'ATR', 'BARSLAST', 'BOLL_LOWER', 'BOLL_UPPER', 'COUNT', 'CROSS_DOWN', 'CROSS_UP', 'EMA', 'HHV', 'KDJ_D', 'KDJ_K', 'LLV', 'MA', 'MAX', 'MIN', 'REF', 'ROC', 'RSI', 'STD', 'SUM'],
  timeframes: ['D', 'W', 'M'],
  units: { TOTAL_MV: '元', FLOAT_MV: '元', TOTAL_SHARES: '股', FLOAT_SHARES: '股', AMOUNT: '元', VOL: '股', PE_TTM: '无量纲(倍)', PB_MRQ: '无量纲(倍)', TURN: '百分比(%)', FORECAST_YOY: '百分比(%)', PCT_CHG: '百分比(%)', S_CLOSE: '指数点位' },
  example_queries: ['CLOSE > MA(CLOSE, 20)', 'PE_TTM < 20 AND TOTAL_MV > 1e10', 'CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))', 'SUM(AMOUNT, 5) > 5e9', 'CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))', 'CLOSE > BOLL_UPPER(CLOSE, 20, 2)'],
  signatures: { MA: ['field', 'pos_int'], EMA: ['field', 'pos_int'], STD: ['field', 'pos_int'], ROC: ['field', 'pos_int'], REF: ['field', 'pos_int'], HHV: ['field', 'pos_int'], LLV: ['field', 'pos_int'], SUM: ['field', 'pos_int'], CROSS_UP: ['series', 'series'], CROSS_DOWN: ['series', 'series'], MAX: ['series', 'series'], MIN: ['series', 'series'], ABS: ['series'], COUNT: ['cond', 'pos_int'], BARSLAST: ['cond'], ATR: ['pos_int'], RSI: ['series', 'pos_int'], BOLL_UPPER: ['series', 'pos_int', 'pos_int'], BOLL_LOWER: ['series', 'pos_int', 'pos_int'], KDJ_K: ['pos_int', 'pos_int'], KDJ_D: ['pos_int', 'pos_int'] },
  descriptions: { MA: 'N日简单移动平均', EMA: 'N日指数移动平均', STD: 'N日标准差', ROC: 'N日变动率(%)', REF: 'N日前值', HHV: 'N周期内最高值', LLV: 'N周期内最低值', SUM: 'N周期内求和', CROSS_UP: '上穿（今日A>B且昨日A<=B）', CROSS_DOWN: '下穿（今日A<B且昨日A>=B）', MAX: '取两序列较大值', MIN: '取两序列较小值', ABS: '绝对值', COUNT: 'N周期内条件成立次数', BARSLAST: '距上次条件成立周期数', ATR: 'N日真实波幅均值（最高最低与昨收的最大差距，简化版）', RSI: 'N日相对强弱（涨跌幅均值比，简化版）', BOLL_UPPER: '布林上轨（N日均价 + K倍N日标准差）', BOLL_LOWER: '布林下轨（N日均价 - K倍N日标准差）', KDJ_K: 'KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）', KDJ_D: 'KDJ随机指标D值（固定用HIGH/LOW/CLOSE，简化版）' },
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

const COMPARE_STR = '>=|<=|>|<';
const POS_INT_MAX = 500;
const ARITH_MAX_OPS = 3;

function validateFormula(meta, formula) {
  if (typeof formula !== 'string' || formula.trim().length === 0) return { ok: false, reason: '公式为空' };
  if (formula.length > MAX_FORMULA_LENGTH) return { ok: false, reason: `公式过长（上限 ${MAX_FORMULA_LENGTH} 字符）` };
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
  let m;
  while ((m = callRegex.exec(formula)) !== null) {
    const func = m[1];
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
  let t;
  while ((t = tokenRegex.exec(formula)) !== null) {
    const token = t[0];
    if (['AND', 'OR'].includes(token)) continue;
    if (indicators.has(token) || fields.has(token)) continue;
    return { ok: false, reason: `未识别标识符 ${token}` };
  }
  return { ok: true };
}

function matchParen(s, openIdx) {
  let d = 0;
  for (let i = openIdx; i < s.length; i++) {
    if (s[i] === '(') d++;
    else if (s[i] === ')') d--;
    if (d === 0) return i;
  }
  return -1;
}

function splitTopLevel(s, sep) {
  const parts = [];
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

function splitBoolTopLevel(s) {
  const out = [];
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

function validateCallArgs(meta, sig, args, func) {
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

function isSeriesExpr(meta, tok) {
  if (meta.fields.includes(tok)) return true;
  // 1. 平衡外括号剥离
  if (tok.trim().startsWith('(') && matchParen(tok.trim(), 0) === tok.trim().length - 1) {
    return isSeriesExpr(meta, tok.trim().slice(1, -1));
  }
  // 2. 函数调用路径：仅当 call 的闭合括号恰在末尾
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
  // 3. 算术表达式路径
  const aparts = splitArithTopLevel(tok);
  if (aparts.length > 1) return isArithExpr(meta, tok);
  return false;
}

function stripOuterParens(tok) {
  let t = tok.trim();
  while (t.startsWith('(') && matchParen(t, 0) === t.length - 1) t = t.slice(1, -1).trim();
  return t;
}

function splitArithTopLevel(s) {
  // 按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符
  const parts = [];
  let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') d++;
    else if (ch === ')') d--;
    if (d === 0 && '+-*/'.includes(ch)) {
      if ((ch === '-' || ch === '+') && /[eE]/.test(s[i - 1] ?? '')) { cur += ch; continue; }
      parts.push(cur.trim());
      cur = '';
    } else cur += ch;
  }
  if (cur.trim() !== '') parts.push(cur.trim());
  return parts;
}

function countTopLevelOps(tok) {
  return splitArithTopLevel(stripOuterParens(tok)).length - 1;
}

function isArithExpr(meta, tok) {
  const parts = splitArithTopLevel(stripOuterParens(tok));
  if (parts.length < 2) return false;
  if (countTopLevelOps(tok) > ARITH_MAX_OPS) return false;
  return parts.every((p) => isSeriesExpr(meta, p) || isNumber(p) || isArithExpr(meta, p));
}

function isCondExpr(meta, tok) {
  const parts = splitBoolTopLevel(tok);
  if (parts.length === 0) return false;
  return parts.every((p) => isCompareExpr(meta, p));
}

function isCompareExpr(meta, expr) {
  const m = new RegExp(`^(.*?)\\s*(${COMPARE_STR})\\s*(.*)$`).exec(expr.trim());
  if (!m) return false;
  const left = m[1].trim();
  const right = m[3].trim();
  // 有意比后端更严格：后端允许常量在任一侧（仅拒绝双常量），此处要求左侧为 series，与 spec 一致
  if (!isSeriesExpr(meta, left)) return false;
  if (!isSeriesExpr(meta, right) && !isNumber(right)) return false;
  return true;
}

function isNumber(s) {
  return /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(s);
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
    '可选算子（函数名(参数形态)：含义）：',
    ...Object.entries(meta.descriptions ?? {}).map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
    '',
    'CROSS_UP/CROSS_DOWN/MAX/MIN 参数可嵌套指标调用（如 CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))），但不支持更深嵌套。',
    'COUNT/BARSLAST 的条件参数是比较表达式（> >= < <=），可用 AND/OR 组合。',
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

// LLM 调用超时：默认 50s，钳制上限 55s（Vercel Hobby 函数硬限 60s，必须留余量）
const LLM_TIMEOUT_DEFAULT_MS = 50000;
const LLM_TIMEOUT_CAP_MS = 55000;

function resolveLlmTimeout(raw) {
  const v = raw === undefined || raw === null || raw === '' ? NaN : Number(raw);
  if (Number.isNaN(v) || v < 1000) return LLM_TIMEOUT_DEFAULT_MS;
  return Math.min(v, LLM_TIMEOUT_CAP_MS);
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

test('validateFormula: NOT 拒绝（后端不支持 ast.Not）', () => {
  const r = validateFormula(META, 'NOT (CLOSE > 11)');
  assert.equal(r.ok, false);
  assert.match(r.reason, /NOT/);
});

test('validateFormula: CROSS_UP 嵌套指标通过', () => {
  const r = validateFormula(META, 'CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))');
  assert.equal(r.ok, true);
});

test('validateFormula: CROSS_UP 字段参数通过', () => {
  const r = validateFormula(META, 'CROSS_UP(CLOSE, OPEN)');
  assert.equal(r.ok, true);
});

test('validateFormula: COUNT 条件通过', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > MA(CLOSE, 20), 10) >= 7');
  assert.equal(r.ok, true);
});

test('validateFormula: COUNT AND 条件通过', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > MA(CLOSE, 20) AND VOL > 1e7, 10) > 3');
  assert.equal(r.ok, true);
});

test('validateFormula: BARSLAST 通过', () => {
  const r = validateFormula(META, 'BARSLAST(CLOSE > MA(CLOSE, 20)) <= 5');
  assert.equal(r.ok, true);
});

test('validateFormula: HHV/LLV/SUM 通过', () => {
  assert.equal(validateFormula(META, 'CLOSE > HHV(CLOSE, 20)').ok, true);
  assert.equal(validateFormula(META, 'CLOSE < LLV(CLOSE, 20)').ok, true);
  assert.equal(validateFormula(META, 'SUM(AMOUNT, 5) > 5e9').ok, true);
});

test('validateFormula: 二层嵌套拒绝', () => {
  const r = validateFormula(META, 'CROSS_UP(MA(MA(CLOSE, 20), 20), MA(CLOSE, 60))');
  assert.equal(r.ok, false);
});

test('validateFormula: COUNT 条件嵌套 COUNT 拒绝', () => {
  const r = validateFormula(META, 'COUNT(COUNT(CLOSE > 10, 2) > 1, 3)');
  assert.equal(r.ok, false);
});

test('validateFormula: 窗口超上限拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE, 501) > 0');
  assert.equal(r.ok, false);
});

test('validateFormula: KDJ 金叉通过', () => {
  const r = validateFormula(META, 'CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))');
  assert.equal(r.ok, true);
});

test('validateFormula: BOLL 突破通过', () => {
  assert.equal(validateFormula(META, 'CLOSE > BOLL_UPPER(CLOSE, 20, 2)').ok, true);
});

test('validateFormula: ATR 顶层通过', () => {
  assert.equal(validateFormula(META, 'ATR(14) < 0.8').ok, true);
});

test('validateFormula: RSI 交叉通过', () => {
  assert.equal(validateFormula(META, 'CROSS_UP(RSI(CLOSE, 6), RSI(CLOSE, 24))').ok, true);
});

test('validateFormula: BOLL 窗口超上限拒绝', () => {
  const r = validateFormula(META, 'BOLL_UPPER(CLOSE, 20, 501)');
  assert.equal(r.ok, false);
});

test('validateFormula: 两层嵌套一致放行', () => {
  const r = validateFormula(META, 'CROSS_UP(MAX(MAX(CLOSE, OPEN), OPEN), MAX(CLOSE, OPEN))');
  assert.equal(r.ok, true);
});

test('validateFormula: 未知算子拒绝', () => {
  const r = validateFormula(META, 'KDJ(CLOSE, 9) > 50');
  assert.equal(r.ok, false);
});

test('validateFormula: cond 支持数值操作数', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > 11, 3)');
  assert.equal(r.ok, true);
});

test('validateFormula: cond == 拒绝', () => {
  const r = validateFormula(META, 'COUNT(CLOSE == 10, 3)');
  assert.equal(r.ok, false);
});

test('validateFormula: AND/OR 子串字段不被误分词', () => {
  const r1 = validateFormula(META, 'COUNT(IS_FORECAST_GOOD > 0.5, 5)');
  assert.equal(r1.ok, true);
  const r2 = validateFormula(META, 'COUNT(FORECAST_YOY > 10 AND CLOSE > 5, 3)');
  assert.equal(r2.ok, true);
});

test('buildSystemPrompt: 包含新算子说明', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /CROSS_UP/);
  assert.match(p, /上穿/);
  assert.match(p, /COUNT/);
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

test('resolveLlmTimeout: 未配置用默认 50s', () => {
  assert.equal(resolveLlmTimeout(undefined), 50000);
  assert.equal(resolveLlmTimeout(''), 50000);
  assert.equal(resolveLlmTimeout('abc'), 50000);
});

test('resolveLlmTimeout: 配置超上限被钳制到 55s', () => {
  assert.equal(resolveLlmTimeout('90000'), 55000);
  assert.equal(resolveLlmTimeout('60000'), 55000);
});

test('resolveLlmTimeout: 合法值原样返回', () => {
  assert.equal(resolveLlmTimeout('45000'), 45000);
});

test('validateFormula: ABS 算术参数通过', () => {
  assert.equal(validateFormula(META, 'ABS(REF(CLOSE, 1) - REF(CLOSE, 2))').ok, true);
});

test('validateFormula: cond 括号算术比较通过', () => {
  assert.equal(validateFormula(META, 'COUNT((CLOSE - OPEN) / CLOSE > 0.05, 5)').ok, true);
});

test('validateFormula: cond 常量乘法通过', () => {
  assert.equal(validateFormula(META, 'COUNT(CLOSE * 1.1 > REF(CLOSE, 1), 5)').ok, true);
});

test('validateFormula: 顶层运算符超上限拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE / CLOSE / CLOSE / CLOSE / CLOSE)');
  assert.equal(r.ok, false);
});

test('validateFormula: 括号内运算符不计入父级顶层', () => {
  assert.equal(validateFormula(META, 'ABS(((CLOSE - OPEN) / (CLOSE / CLOSE)) * 2)').ok, true);
});

test('validateFormula: 窗口 field 参数算术拒绝', () => {
  const r = validateFormula(META, 'MA(CLOSE - OPEN, 20)');
  assert.equal(r.ok, false);
});

test('validateFormula: 幂运算符拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE ** 2)');
  assert.equal(r.ok, false);
});

test('validateFormula: 布尔操作数拒绝', () => {
  const r = validateFormula(META, 'ABS(CLOSE - True)');
  assert.equal(r.ok, false);
});

test('validateFormula: 嵌套括号算术通过', () => {
  assert.equal(
    validateFormula(META, 'ABS((REF(CLOSE, 1) - REF(CLOSE, 2)) / REF(CLOSE, 2))').ok,
    true
  );
});

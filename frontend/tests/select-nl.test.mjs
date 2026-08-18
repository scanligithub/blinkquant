// frontend/tests/select-nl.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
  buildCoverageCases,
  computeCoverageMatrix,
  formatCoverageMatrix,
} from '../scripts/nl-coverage.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---- 复制自 src/lib/selectNL.ts（保持与实现一致）----
const META = {
  fields: ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOL', 'AMOUNT', 'PCT_CHG', 'S_CLOSE', 'PE_TTM', 'PB_MRQ', 'FORECAST_YOY', 'IS_FORECAST_GOOD', 'IS_FORECAST_BAD', 'TOTAL_SHARES', 'FLOAT_SHARES', 'TOTAL_MV', 'FLOAT_MV', 'TURN'],
  indicators: ['ABS', 'ATR', 'BARSLAST', 'BOLL_LOWER', 'BOLL_UPPER', 'COUNT', 'CROSS_DOWN', 'CROSS_UP', 'EMA', 'HHV', 'KDJ_D', 'KDJ_K', 'LLV', 'MA', 'MACD_DEA', 'MACD_DIF', 'MACD_HIST', 'MAX', 'MIN', 'REF', 'ROC', 'RSI', 'STD', 'SUM'],
  timeframes: ['D', 'W', 'M'],
  units: { TOTAL_MV: '元', FLOAT_MV: '元', TOTAL_SHARES: '股', FLOAT_SHARES: '股', AMOUNT: '元', VOL: '股', PE_TTM: '无量纲(倍)', PB_MRQ: '无量纲(倍)', TURN: '百分比(%)', FORECAST_YOY: '百分比(%)', PCT_CHG: '百分比(%)', S_CLOSE: '指数点位' },
  example_queries: ['CLOSE > MA(CLOSE, 20)', 'PE_TTM < 20 AND TOTAL_MV > 1e10', 'CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))', 'SUM(AMOUNT, 5) > 5e9', 'CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))', 'CLOSE > BOLL_UPPER(CLOSE, 20, 2)', 'CROSS_UP(MACD_DIF(12, 26), MACD_DEA(12, 26, 9))'],
  signatures: { MA: ['field', 'pos_int'], EMA: ['field', 'pos_int'], STD: ['field', 'pos_int'], ROC: ['field', 'pos_int'], REF: ['field', 'pos_int'], HHV: ['field', 'pos_int'], LLV: ['field', 'pos_int'], SUM: ['field', 'pos_int'], CROSS_UP: ['series', 'series'], CROSS_DOWN: ['series', 'series'], MAX: ['series', 'series'], MIN: ['series', 'series'], ABS: ['series'], COUNT: ['cond', 'pos_int'], BARSLAST: ['cond'], ATR: ['pos_int'], RSI: ['series', 'pos_int'], BOLL_UPPER: ['series', 'pos_int', 'pos_int'], BOLL_LOWER: ['series', 'pos_int', 'pos_int'], KDJ_K: ['pos_int', 'pos_int'], KDJ_D: ['pos_int', 'pos_int'], MACD_DIF: ['pos_int', 'pos_int'], MACD_DEA: ['pos_int', 'pos_int', 'pos_int'], MACD_HIST: ['pos_int', 'pos_int', 'pos_int'] },
  descriptions: { MA: 'N日简单移动平均', EMA: 'N日指数移动平均', STD: 'N日标准差', ROC: 'N日变动率(%)', REF: 'N日前值', HHV: 'N周期内最高值', LLV: 'N周期内最低值', SUM: 'N周期内求和', CROSS_UP: '上穿（今日A>B且昨日A<=B）', CROSS_DOWN: '下穿（今日A<B且昨日A>=B）', MAX: '取两序列较大值', MIN: '取两序列较小值', ABS: '绝对值', COUNT: 'N周期内条件成立次数', BARSLAST: '距上次条件成立周期数', ATR: 'N日真实波幅均值（最高最低与昨收的最大差距，简化版）', RSI: 'N日相对强弱（涨跌幅均值比，简化版）', BOLL_UPPER: '布林上轨（N日均价 + K倍N日标准差）', BOLL_LOWER: '布林下轨（N日均价 - K倍N日标准差）', KDJ_K: 'KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）', KDJ_D: 'KDJ随机指标D值（固定用HIGH/LOW/CLOSE，简化版）', MACD_DIF: 'MACD快慢线差（EMA(CLOSE,fast) - EMA(CLOSE,slow)，固定用CLOSE）', MACD_DEA: 'MACD信号线（DIF的signal期EMA，固定用CLOSE）', MACD_HIST: 'MACD柱（2 × (DIF - DEA)，固定用CLOSE）' },
};
const MAX_FORMULA_LENGTH = 500;
const CODE_FENCE = /```(?:json)?\s*([\s\S]*?)```/;

function stripCodeFence(raw) {
  const m = CODE_FENCE.exec(raw);
  return m ? m[1].trim() : raw.trim();
}

// 兜底：LLM 偶尔在 JSON 前后夹带自然语言，提取首个平衡的大括号对象
function extractFirstJsonObject(raw) {
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

function parseSelectNLText(raw) {
  const cleaned = stripCodeFence(raw);
  let text = cleaned;
  let parsed;
  try { parsed = JSON.parse(text); }
  catch {
    // 尝试提取首个对象（可能只是被自然语言包裹）
    const obj = extractFirstJsonObject(text);
    if (obj) {
      try { parsed = JSON.parse(obj); }
      catch { throw new Error('LLM 输出不是合法 JSON'); }
    } else {
      throw new Error('LLM 输出不是合法 JSON');
    }
  }
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

const UNIT_FACTORS = { 万亿: 1e12, 亿: 1e8, 万: 1e4 };
const UNIT_RE = /(\d+(?:\.\d+)?)\s*(万亿|亿|万)/g;
const NUM_LITERAL_RE = /\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;

// 重试决策：invalid/mismatch 在时间预算内可再翻一次；ok 直接通过
function shouldRetryTranslation(result, elapsedMs, budgetMs) {
  if (result.kind === 'ok') return false;
  return elapsedMs < budgetMs;
}

function findMagnitudeMismatch(analysis, formula) {
  const text = [analysis.restatement, ...analysis.conditions].filter(Boolean).join('\n');
  const expected = [];
  let m;
  UNIT_RE.lastIndex = 0;
  while ((m = UNIT_RE.exec(text)) !== null) {
    const n = Number(m[1]);
    const factor = UNIT_FACTORS[m[2]];
    const value = n * factor;
    if (!Number.isFinite(value) || value <= 0) continue;
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
function buildHardConstraintSuffix(analysis) {
  const text = [analysis.restatement, ...(analysis.conditions || [])].join('\n');
  const lines = [];
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
  return lines.length ? '\n' + lines.join('\n') : '';
}

// 首次翻译的 user 消息
function buildTranslateUserMessage(analysis) {
  const conditionLines = analysis.conditions.map((c, i) => `${i + 1}. ${c}`).join('\n');
  return [
    '这是用户需求对应的已确认语义，请严格按它翻译成 BlinkQuant 公式：',
    `需求复述：${analysis.restatement}`,
    `条件清单：\n${conditionLines}`,
    `逻辑关系：${analysis.logic}`,
    `周期：${analysis.timeframe}`,
    buildHardConstraintSuffix(analysis),
  ].join('\n');
}

// repair 专用 system：在全量 buildSystemPrompt 之后追加，比整段重写更聚焦。
function buildRepairSystemSuffix() {
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
  ].join('\n');
}

// repair 的 user 消息：必须带上被拒公式原文 + reason
function buildRepairUserMessage(analysis, rejectedFormula, reason) {
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
// 改为 CROSS_UP/CROSS_DOWN(CLOSE, BOLL_*(...))。不改 REF(MA(...),1)（昨日均线是合法语义）。
function trySafeBollRefRewrite(formula) {
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

function trySafeAbsAbsRewrite(formula) {
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

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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
    '单位换算规则（重要，数值必须精确换算）：',
    '用户说"亿"=1e8、"万"=1e4、"万亿"=1e12。换算时把数字乘以对应倍数：',
    '1亿=1e8，100亿=1e10，200亿=2e10，5亿=5e8；1万=1e4，5000万=5e7，20万=2e5。',
    '注意：5000万=5e7（不是5e9），200亿=2e10（不是2e11）。',
    '',
    '可选算子（函数名(参数形态)：含义）：',
    ...Object.entries(meta.descriptions ?? {}).map(([k, d]) => `${k}(${(meta.signatures?.[k] ?? []).join(', ')}): ${d}`),
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

test('parseSelectNLText: 前后夹杂文字时提取首个 JSON 对象', () => {
  const r = parseSelectNLText('好的，翻译如下：\n{"formula":"PE_TTM < 20","timeframe":"d","explanation":"低估值"}\n请确认。');
  assert.deepEqual(r, { formula: 'PE_TTM < 20', timeframe: 'D', explanation: '低估值' });
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

test('validateFormula: MACD 金叉通过', () => {
  const r = validateFormula(META, 'CROSS_UP(MACD_DIF(12, 26), MACD_DEA(12, 26, 9))');
  assert.equal(r.ok, true);
});

test('validateFormula: MACD DIF/DEA/柱 单值比较通过', () => {
  assert.equal(validateFormula(META, 'MACD_DIF(12, 26) > 0').ok, true);
  assert.equal(validateFormula(META, 'MACD_DEA(12, 26, 9) < 0').ok, true);
  assert.equal(validateFormula(META, 'MACD_HIST(12, 26, 9) > 0').ok, true);
});

test('validateFormula: MACD 参数个数错误拒绝', () => {
  const r = validateFormula(META, 'MACD_DIF(12) > 0');
  assert.equal(r.ok, false);
  assert.match(r.reason, /MACD_DIF/);
  const r2 = validateFormula(META, 'MACD_DEA(12, 26) > 0');
  assert.equal(r2.ok, false);
  assert.match(r2.reason, /MACD_DEA/);
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
  assert.match(p, /5000万=5e7/);
  assert.match(p, /200亿=2e10/);
});

test('buildSystemPrompt: 含 MACD 算子说明与金叉示例', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /MACD_DIF/);
  assert.match(p, /MACD_DEA/);
  assert.match(p, /MACD_HIST/);
  assert.match(p, /CROSS_UP\(MACD_DIF/);
});

test('buildSystemPrompt: 禁止 REF 嵌套调用 + COUNT 条件正例', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /REF 的第一个参数必须是字段/);
  assert.match(p, /禁止嵌套调用/);
  assert.match(p, /REF\(CLOSE,\s*1\)/);
  assert.match(p, /COUNT\(VOL > REF\(VOL,\s*1\),\s*3\) >= 3/);
});

test('buildSystemPrompt: 易错模式含新低/振幅/布林/BARSLAST 签名', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /创N日新高|新低/);
  assert.match(p, /振幅/);
  assert.match(p, /BOLL_UPPER/);
  assert.match(p, /BARSLAST\(cond\)/);
  assert.match(p, /禁止嵌套调用|REF\(BOLL/);
  assert.match(p, /CLOSE >= HHV\(CLOSE,N\)/);
  assert.match(p, /CLOSE <= LLV\(CLOSE,N\)/);
});

test('buildAnalyzePrompt: 新高新低振幅布林消歧措辞', () => {
  const p = buildAnalyzePrompt(META);
  assert.match(p, /近N日最高价|最高价/);
  assert.match(p, /振幅/);
  assert.match(p, /布林/);
  assert.match(p, /BARSLAST/);
});

test('buildHardConstraintSuffix: 新高/新低/振幅/布林关键词触发', () => {
  const a = { restatement: '创5日新低', conditions: ['收盘价创5日新低'], logic: '1', timeframe: 'D' };
  assert.match(buildHardConstraintSuffix(a), /BARSLAST/);
  assert.match(buildHardConstraintSuffix(a), /HHV\/LLV/);
  const b = { restatement: '近期振幅较大', conditions: ['近5日振幅大于10%'], logic: '1', timeframe: 'D' };
  assert.match(buildHardConstraintSuffix(b), /振幅/);
  assert.match(buildHardConstraintSuffix(b), /\(HIGH-LOW\)/);
  const c = { restatement: '上破布林上轨', conditions: ['收盘价上破布林上轨'], logic: '1', timeframe: 'D' };
  assert.match(buildHardConstraintSuffix(c), /REF\(BOLL/);
});

test('buildHardConstraintSuffix: 无关键词返回空串', () => {
  const a = { restatement: '市盈率低于20', conditions: ['市盈率低于20'], logic: '1', timeframe: 'D' };
  assert.equal(buildHardConstraintSuffix(a), '');
});

test('buildHardConstraintSuffix: 绝对偏差触发 ABS 约束', () => {
  const a = { restatement: '收盘价距20日均线绝对偏差大于2元', conditions: ['收盘价距20日均线绝对偏差大于2元'], logic: '1', timeframe: 'D' };
  const s = buildHardConstraintSuffix(a);
  assert.match(s, /绝对偏差|偏离/);
  assert.match(s, /ABS/);
  // 含禁止 OR 展开的引导
  assert.match(s, /OR|展开/);
});

test('buildHardConstraintSuffix: 较高者/较小值触发 MAX/MIN 约束', () => {
  const a = { restatement: '今日开盘价与收盘价中的较高者上穿20日均线', conditions: ['今日开盘价与收盘价中的较高者上穿20日均线'], logic: '1', timeframe: 'D' };
  const s = buildHardConstraintSuffix(a);
  assert.match(s, /较高者/);
  assert.match(s, /MAX/);
  const b = { restatement: '开盘价与收盘价取较小值后小于昨日最低价', conditions: ['开盘价与收盘价取较小值后小于昨日最低价'], logic: '1', timeframe: 'D' };
  assert.match(buildHardConstraintSuffix(b), /MIN/);
});

test('buildHardConstraintSuffix: 普通对比约束不误伤无关键词排序', () => {
  const a = { restatement: '连续5天上涨', conditions: ['连续5天上涨'], logic: '1', timeframe: 'D' };
  assert.doesNotMatch(buildHardConstraintSuffix(a), /比较高|取较小值|较小者|较高者/);
});

test('buildTranslateUserMessage: 含复述/条件/逻辑/周期/硬约束', () => {
  const a = { restatement: '创5日新高', conditions: ['收盘价创5日新高'], logic: '1', timeframe: 'D' };
  const m = buildTranslateUserMessage(a);
  assert.match(m, /创5日新高/);
  assert.match(m, /条件清单/);
  assert.match(m, /硬约束：本需求含新高/);
});

test('buildRepairSystemSuffix: 修复模式规则与签名约束', () => {
  const s = buildRepairSystemSuffix();
  assert.match(s, /公式修复模式/);
  assert.match(s, /BARSLAST 只能有 1 个/);
  assert.match(s, /REF\(BOLL/);
  assert.match(s, /HHV\/LLV/);
});

test('buildRepairSystemSuffix: 含 ABS/MAX 嵌套与展开修复规则', () => {
  const s = buildRepairSystemSuffix();
  // ABS：禁止 OR 双向展开，鼓励 ABS(序列) 形式
  assert.match(s, /ABS/);
  assert.match(s, /OR|展开|双向/);
  // MAX：禁止把 MAX/MIN 作为外层 window 函数参数（如 MA(MAX(...),N)、REF(MAX(...),1)）
  assert.match(s, /绝对值|绝对偏差|偏离/);
  assert.match(s, /MAX|MIN/);
});

test('buildRepairSystemSuffix: BARSLAST 条件必须为简单比较式', () => {
  const s = buildRepairSystemSuffix();
  // Nemotron 曾把 CROSS_UP/CROSS_DOWN 嵌套进 BARSLAST/COUNT 条件
  assert.match(s, /BARSLAST/);
  assert.match(s, /CROSS_UP|CROSS_DOWN/);
  assert.match(s, /简单比较式|比较式/);
});

test('buildSystemPrompt: 易错模式含 BARSLAST 条件约束', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /BARSLAST/);
  assert.match(p, /CROSS_UP|CROSS_DOWN/);
  // 示范正确形态：距上次跌破均线 = BARSLAST(CLOSE < MA(CLOSE,20)) > N
  assert.match(p, /BARSLAST\s*\(\s*CLOSE\s*<\s*MA/);
});

test('buildRepairUserMessage: 含非法公式与原因', () => {
  const a = { restatement: 'x', conditions: ['y'], logic: '1', timeframe: 'D' };
  const m = buildRepairUserMessage(a, 'BARSLAST(CLOSE, 5)', '函数 BARSLAST 必须恰好 1 个参数');
  assert.match(m, /BARSLAST\(CLOSE, 5\)/);
  assert.match(m, /必须恰好 1 个参数/);
});

test('trySafeBollRefRewrite: 仅改 BOLL 突破形态不改 REF(MA)', () => {
  const f = 'CLOSE > REF(BOLL_UPPER(CLOSE, 20, 2), 1)';
  const r = trySafeBollRefRewrite(f);
  assert.ok(r && r.includes('CROSS_UP(CLOSE, BOLL_UPPER(CLOSE, 20, 2))'));
  assert.equal(trySafeBollRefRewrite('CLOSE > REF(MA(CLOSE, 20), 1)'), null);
});

test('trySafeAbsAbsRewrite: OR 双向展开转 ABS(差值)', () => {
  // 弱模型把 ABS 展开成双向不等式时的确定性还原
  const f = 'CLOSE >= MA(CLOSE, 20) + 2 OR CLOSE <= MA(CLOSE, 20) - 2';
  const r = trySafeAbsAbsRewrite(f);
  assert.ok(r, '应收敛为 ABS 形式');
  assert.match(r, /ABS/);
  assert.doesNotMatch(r, /OR/);
  // 验证收敛结果可通过公式校验（序列形式）
  const v = validateFormula(META, r);
  assert.equal(v.ok, true, `收敛公式校验失败: ${v.reason}`);
});

test('trySafeAbsAbsRewrite: 非双向展开不误改', () => {
  assert.equal(trySafeAbsAbsRewrite('CLOSE > MA(CLOSE, 20)'), null);
  assert.equal(trySafeAbsAbsRewrite('CLOSE >= MA(CLOSE, 20) + 2 OR PE_TTM < 20'), null);
  assert.equal(trySafeAbsAbsRewrite('OPEN > CLOSE AND CLOSE >= LOW'), null);
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

test('validateFormula: 顶层 AND 后跟括号条件不误判函数', () => {
  const r = validateFormula(META, 'CLOSE > MA(CLOSE, 20) AND (CLOSE - OPEN) / (HIGH - LOW) > 0.4');
  assert.equal(r.ok, true);
});

test('validateFormula: 顶层 OR 后跟括号条件不误判函数', () => {
  const r = validateFormula(META, 'CLOSE > MA(CLOSE, 20) OR (CLOSE - OPEN) / (HIGH - LOW) > 0.4');
  assert.equal(r.ok, true);
});

test('validateFormula: COUNT 内 AND 后跟括号条件通过', () => {
  const r = validateFormula(META, 'COUNT(CLOSE > OPEN AND (CLOSE - OPEN) / (HIGH - LOW) > 0.4, 3) > 0');
  assert.equal(r.ok, true);
});

// ---- 两步语义翻译：parseSelectNLAnalysis / buildAnalyzePrompt ----

function parseSelectNLAnalysis(raw) {
  // 见 src/lib/selectNL.ts
  const cleaned = stripCodeFence(raw);
  let parsed;
  try { parsed = JSON.parse(cleaned); }
  catch { throw new Error('LLM 输出不是合法 JSON'); }
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
  return { restatement: parsed.restatement.trim(), conditions, logic, timeframe };
}

function buildAnalyzePrompt(meta) {
  const fieldsLine = meta.fields.join('、');
  const unitsLine = Object.entries(meta.units).map(([k, v]) => `${k}=${v}`).join('，');
  const indicatorsLine = Object.entries(meta.descriptions ?? {}).map(([k, d]) => `${k}: ${d}`).join('\n');
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
    '',
    '输出必须是合法 JSON：{"restatement":"...","conditions":["...","..."],"logic":"1 AND 2","timeframe":"D"}。',
    '只输出 JSON，不要输出其他文字。',
  ].join('\n');
}

test('parseSelectNLAnalysis: 正常解析', () => {
  const r = parseSelectNLAnalysis('{"restatement":"找5日振幅大于3%的股票","conditions":["近5日振幅 > 3%","总市值 > 100亿"],"logic":"1 AND 2","timeframe":"D"}');
  assert.deepEqual(r, {
    restatement: '找5日振幅大于3%的股票',
    conditions: ['近5日振幅 > 3%', '总市值 > 100亿'],
    logic: '1 AND 2',
    timeframe: 'D',
  });
});

test('parseSelectNLAnalysis: 代码围栏剥离', () => {
  const r = parseSelectNLAnalysis('```json\n{"restatement":"x","conditions":["a"],"logic":"1","timeframe":"w"}\n```');
  assert.equal(r.restatement, 'x');
  assert.equal(r.timeframe, 'W');
});

test('parseSelectNLAnalysis: 缺少 restatement 抛错', () => {
  assert.throws(() => parseSelectNLAnalysis('{"conditions":["a"],"logic":"1"}'), /restatement/);
});

test('parseSelectNLAnalysis: conditions 缺失或空抛错', () => {
  assert.throws(() => parseSelectNLAnalysis('{"restatement":"x"}'), /conditions/);
  assert.throws(() => parseSelectNLAnalysis('{"restatement":"x","conditions":[]}'), /conditions/);
});

test('parseSelectNLAnalysis: 非法 JSON 抛错', () => {
  assert.throws(() => parseSelectNLAnalysis('not json'), /JSON/);
});

test('parseSelectNLAnalysis: 缺少 logic 时默认顺序 AND', () => {
  const r = parseSelectNLAnalysis('{"restatement":"x","conditions":["a","b","c"]}');
  assert.equal(r.logic, '1 AND 2 AND 3');
});

test('parseSelectNLAnalysis: 缺少 timeframe 默认 D', () => {
  const r = parseSelectNLAnalysis('{"restatement":"x","conditions":["a"]}');
  assert.equal(r.timeframe, 'D');
});

test('findMagnitudeMismatch: 200亿 公式 2e11 报错', () => {
  const a = { restatement: '流通市值大于200亿元', conditions: ['流通市值大于200亿元'], logic: '1', timeframe: 'D' };
  const r = findMagnitudeMismatch(a, 'FLOAT_MV > 2e11');
  assert.match(r, /200亿/);
  assert.match(r, /20000000000/);
});

test('findMagnitudeMismatch: 200亿 公式 2e10 通过', () => {
  const a = { restatement: '流通市值大于200亿元', conditions: ['流通市值大于200亿元'], logic: '1', timeframe: 'D' };
  assert.equal(findMagnitudeMismatch(a, 'FLOAT_MV > 2e10'), null);
});

test('findMagnitudeMismatch: 5000万 公式 5e9 报错 / 5e7 通过', () => {
  const a = { restatement: '总市值大于5000万', conditions: ['总市值大于5000万'], logic: '1', timeframe: 'D' };
  assert.match(findMagnitudeMismatch(a, 'TOTAL_MV > 5e9'), /5000万/);
  assert.equal(findMagnitudeMismatch(a, 'TOTAL_MV > 5e7'), null);
});

test('findMagnitudeMismatch: 无单位短语返回 null', () => {
  const a = { restatement: '市盈率低于20', conditions: ['市盈率低于20'], logic: '1', timeframe: 'D' };
  assert.equal(findMagnitudeMismatch(a, 'PE_TTM < 20'), null);
});

test('findMagnitudeMismatch: 长整型写法 20000000000 也命中', () => {
  const a = { restatement: '总市值大于100亿', conditions: ['总市值大于100亿'], logic: '1', timeframe: 'D' };
  assert.equal(findMagnitudeMismatch(a, 'TOTAL_MV > 10000000000'), null);
});

test('shouldRetryTranslation: invalid/mismatch 可重试，ok 不重试', () => {
  assert.equal(shouldRetryTranslation({ kind: 'invalid', reason: 'x' }, 1000, 40000), true);
  assert.equal(shouldRetryTranslation({ kind: 'mismatch', reason: 'x' }, 1000, 40000), true);
  assert.equal(shouldRetryTranslation({ kind: 'ok', formula: 'X', timeframe: 'D', explanation: '' }, 1000, 40000), false);
});

test('shouldRetryTranslation: 超过时间预算不重试', () => {
  assert.equal(shouldRetryTranslation({ kind: 'invalid', reason: 'x' }, 45000, 40000), false);
});

test('buildAnalyzePrompt: 强调不翻译公式且输出 JSON 契约', () => {
  const p = buildAnalyzePrompt(META);
  assert.match(p, /不要直接输出公式/);
  assert.match(p, /restatement/);
  assert.match(p, /conditions/);
  assert.match(p, /"logic"/);
});

test('buildAnalyzePrompt: 包含字段/单位/指标与周期', () => {
  const p = buildAnalyzePrompt(META);
  assert.match(p, /PE_TTM/);
  assert.match(p, /TOTAL_MV=元/);
  assert.match(p, /CROSS_UP/);
  assert.match(p, /timeframe/);
  assert.match(p, /D\/W\/M/);
});

// 守卫测试：测试内嵌实现副本与 src/lib/selectNL.ts 保持一致（防漂移）
test('guard: 测试副本与 selectNL.ts 新增函数一致', () => {
  const src = readFileSync(join(__dirname, '..', 'src', 'lib', 'selectNL.ts'), 'utf8');
  assert.match(src, /export function parseSelectNLAnalysis/);
  assert.match(src, /export function buildAnalyzePrompt/);
  assert.match(src, /export function findMagnitudeMismatch/);
  assert.match(src, /export function buildHardConstraintSuffix/);
  assert.match(src, /export function buildTranslateUserMessage/);
  assert.match(src, /export function buildRepairSystemSuffix/);
  assert.match(src, /export function buildRepairUserMessage/);
  assert.match(src, /export function trySafeBollRefRewrite/);
});

// ---- 注册表全覆盖：生成器与覆盖矩阵（import scripts/nl-coverage.mjs，纯 .mjs 无需复制）----

test('buildCoverageCases: 只生成未被现有用例断言的字段/算子缺口', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN', 'PCT_CHG'],
    indicators: ['MA', 'ABS', 'CROSS_UP'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const existing = [
    { cid: 'x1', q: '收盘价站上20日均线的股票', sub: ['CLOSE', 'MA('], tf: 'D' },
    { cid: 'x2', q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP'], tf: 'D' },
  ];
  const out = buildCoverageCases(meta, existing);
  const cids = out.cases.map((c) => c.cid).sort();
  assert.deepEqual(cids, ['gF_OPEN', 'gF_PCT_CHG', 'gI_ABS']);
  assert.deepEqual(out.uncoveredFields, []);
  assert.deepEqual(out.uncoveredInds, []);
  const openCase = out.cases.find((c) => c.cid === 'gF_OPEN');
  assert.equal(typeof openCase.q, 'string');
  assert.ok(openCase.q.length > 0);
  assert.deepEqual(openCase.sub, ['OPEN']);
});

test('buildCoverageCases: 注册表新增无生成器的项计入未覆盖', () => {
  const meta = {
    fields: ['NOVEL_FIELD'],
    indicators: ['NOVEL_IND'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const out = buildCoverageCases(meta, []);
  assert.deepEqual(out.cases, []);
  assert.deepEqual(out.uncoveredFields, ['NOVEL_FIELD']);
  assert.deepEqual(out.uncoveredInds, ['NOVEL_IND']);
});

test('buildCoverageCases: sub_any 不参与覆盖判定（多解算子仍生成专属用例）', () => {
  const meta = {
    fields: ['CLOSE'],
    indicators: ['COUNT', 'REF'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const existing = [
    { cid: 'x', q: '收盘价大于10元的股票', sub: ['CLOSE'], tf: 'D' },
    { cid: 'y', q: '连续5天收盘价上涨的股票', sub_any: ['COUNT(', 'REF(CLOSE'], tf: 'D' },
  ];
  const out = buildCoverageCases(meta, existing);
  const cids = out.cases.map((c) => c.cid).sort();
  assert.deepEqual(cids, ['gI_COUNT', 'gI_REF']);
});

test('computeCoverageMatrix: 从公式反推字段/算子覆盖与缺失', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN', 'PCT_CHG'],
    indicators: ['MA', 'ABS', 'CROSS_UP'],
    timeframes: ['D', 'W', 'M'],
    units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const results = [
    { ok: true, formula: 'CLOSE > MA(CLOSE, 20)' },
    { ok: true, formula: 'ABS(CLOSE - OPEN) > 2' },
  ];
  const m = computeCoverageMatrix(meta, results);
  assert.equal(m.fields.total, 3);
  assert.equal(m.fields.covered, 2);
  assert.deepEqual(m.fields.missing, ['PCT_CHG']);
  assert.equal(m.indicators.total, 3);
  assert.equal(m.indicators.covered, 2);
  assert.deepEqual(m.indicators.missing, ['CROSS_UP']);
});

test('computeCoverageMatrix: 大小写不敏感的公式 token 提取', () => {
  const meta = {
    fields: ['CLOSE'], indicators: ['MA'],
    timeframes: ['D'], units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const m = computeCoverageMatrix(meta, [{ ok: true, formula: 'close > ma(CLOSE,20)' }]);
  assert.equal(m.fields.covered, 1);
  assert.equal(m.indicators.covered, 1);
  assert.deepEqual(m.fields.missing, []);
});

test('formatCoverageMatrix: 输出含总数与缺失项', () => {
  const meta = {
    fields: ['CLOSE', 'OPEN'], indicators: ['MA'],
    timeframes: ['D'], units: {}, example_queries: [], signatures: {}, descriptions: {},
  };
  const m = computeCoverageMatrix(meta, [{ ok: true, formula: 'CLOSE > MA(CLOSE,20)' }]);
  const s = formatCoverageMatrix(m);
  assert.match(s, /字段: 1\/2/);
  assert.match(s, /缺: OPEN/);
  assert.match(s, /算子: 1\/1/);
});

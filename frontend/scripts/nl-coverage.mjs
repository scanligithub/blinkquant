// frontend/scripts/nl-coverage.mjs
// 注册表全覆盖：按 nl-meta 的字段/算子全集，补齐测试缺口用例并计算覆盖矩阵。
// 供 nl-test.mjs 集成脚本使用，也可被 node --test 直接 import 做单测（纯 .mjs）。

// 字段生成器：为每个字段写最佳口语查询（描述驱动；语义口径由 q 表达，不由描述机械拼接）。
// note: 口语罕见、易混淆的字段标注说明。
export const FIELD_GEN = {
  CLOSE: { q: '收盘价大于10元的股票', sub: ['CLOSE'] },
  OPEN: { q: '开盘价大于昨日收盘价的股票', sub: ['OPEN'] },
  HIGH: { q: '当日最高价大于20日均线的股票', sub: ['HIGH'] },
  LOW: { q: '当日最低价小于20日均线的股票', sub: ['LOW'] },
  VOL: { q: '成交量大于100万手的股票', sub: ['VOL'] },
  AMOUNT: { q: '成交额大于5亿的股票', sub: ['AMOUNT'] },
  PCT_CHG: { q: '当日涨幅大于3%的股票', sub: ['PCT_CHG'] },
  S_CLOSE: { q: '指数收盘点位高于3000的股票', sub: ['S_CLOSE'], note: '指数点位字段' },
  PE_TTM: { q: '市盈率低于15倍的股票', sub: ['PE_TTM'] },
  PB_MRQ: { q: '市净率低于1.5倍的股票', sub: ['PB_MRQ'] },
  FORECAST_YOY: { q: '预测净利润同比增长大于20%的股票', sub: ['FORECAST_YOY'], note: '预测数据' },
  IS_FORECAST_GOOD: { q: '业绩预增的股票', sub: ['IS_FORECAST_GOOD'], note: '预测利好' },
  IS_FORECAST_BAD: { q: '业绩预亏的股票', sub: ['IS_FORECAST_BAD'], note: '预测利空' },
  TOTAL_SHARES: { q: '总股本大于10亿股的股票', sub: ['TOTAL_SHARES'] },
  FLOAT_SHARES: { q: '流通股本大于5亿股的股票', sub: ['FLOAT_SHARES'] },
  TOTAL_MV: { q: '总市值大于100亿的股票', sub: ['TOTAL_MV'] },
  FLOAT_MV: { q: '流通市值大于200亿的股票', sub: ['FLOAT_MV'] },
  TURN: { q: '换手率大于5%的股票', sub: ['TURN'] },
};

// 算子生成器：为每个算子写最佳口语查询。
export const IND_GEN = {
  ABS: { q: '收盘价距20日均线绝对偏差大于2元的股票', sub: ['ABS'] },
  ATR: { q: '14日真实波幅均值大于3的股票', sub: ['ATR'] },
  BARSLAST: { q: '距上次突破20日均线不超过3天的股票', sub: ['BARSLAST'] },
  BOLL_LOWER: { q: '收盘价跌破布林下轨的股票', sub: ['BOLL_LOWER'] },
  BOLL_UPPER: { q: '收盘价突破布林上轨的股票', sub: ['BOLL_UPPER'] },
  COUNT: { q: '近5日收盘价站上20日均线的天数不少于3天的股票', sub: ['COUNT'] },
  CROSS_DOWN: { q: '5日均线下穿30日均线的股票', sub: ['CROSS_DOWN'] },
  CROSS_UP: { q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP'] },
  EMA: { q: '收盘价站上20日指数均线的股票', sub: ['EMA'] },
  HHV: { q: '创20日新高的股票', sub: ['HHV'] },
  KDJ_D: { q: 'KDJ的D值大于80的股票', sub: ['KDJ_D'] },
  KDJ_K: { q: 'KDJ的K值大于80的股票', sub: ['KDJ_K'] },
  LLV: { q: '创20日新低的股票', sub: ['LLV'] },
  MA: { q: '收盘价站上20日均线的股票', sub: ['MA'] },
  MAX: { q: '开盘价与收盘价取较大值后大于昨日最高价的股票', sub: ['MAX'] },
  MIN: { q: '开盘价与收盘价取较小值后小于昨日最低价的股票', sub: ['MIN'] },
  REF: { q: '今日收盘价高于昨日收盘价的股票', sub: ['REF'] },
  ROC: { q: '5日变动率大于5%的股票', sub: ['ROC'] },
  RSI: { q: '14日RSI大于70的股票', sub: ['RSI'] },
  STD: { q: '20日收盘价标准差大于2的股票', sub: ['STD'] },
  SUM: { q: '近5日成交额之和大于100亿的股票', sub: ['SUM'] },
};

// 现有用例断言 token 前缀匹配任意注册表字段/算子，判定该字段/算子已被手工用例覆盖。
function coveredTokens(meta, existingCases) {
  const coveredFields = new Set();
  const coveredInds = new Set();
  const allTokens = [...meta.fields, ...meta.indicators];
  for (const c of existingCases) {
    for (const s of [...(c.sub || []), ...(c.sub_any || [])]) {
      const up = String(s).toUpperCase();
      for (const tok of allTokens) {
        if (up.startsWith(tok)) {
          if (meta.fields.includes(tok)) coveredFields.add(tok);
          else coveredInds.add(tok);
        }
      }
    }
  }
  return { coveredFields, coveredInds };
}

// 生成缺口用例：未被现有用例断言的字段/算子，有生成器则生成，无生成器计入未覆盖。
export function buildCoverageCases(meta, existingCases) {
  const { coveredFields, coveredInds } = coveredTokens(meta, existingCases);
  const cases = [];
  const uncoveredFields = [];
  const uncoveredInds = [];
  for (const f of meta.fields || []) {
    if (coveredFields.has(f)) continue;
    const g = FIELD_GEN[f];
    if (!g) { uncoveredFields.push(f); continue; }
    cases.push({ cid: `gF_${f}`, cat: '覆盖-字段', q: g.q, sub: g.sub, tf: 'D' });
  }
  for (const ind of meta.indicators || []) {
    if (coveredInds.has(ind)) continue;
    const g = IND_GEN[ind];
    if (!g) { uncoveredInds.push(ind); continue; }
    cases.push({ cid: `gI_${ind}`, cat: '覆盖-算子', q: g.q, sub: g.sub, tf: 'D' });
  }
  return { cases, uncoveredFields, uncoveredInds };
}

const TOKEN_RE = /[A-Z][A-Z0-9_]*/g;

// 从执行结果反推覆盖矩阵：公式大写后提取大写标识符，与注册表全集比对。
export function computeCoverageMatrix(meta, results) {
  const hitFields = new Set();
  const hitInds = new Set();
  for (const r of results) {
    const tokens = new Set((String(r.formula || '').toUpperCase().match(TOKEN_RE)) || []);
    for (const f of meta.fields || []) if (tokens.has(f)) hitFields.add(f);
    for (const i of meta.indicators || []) if (tokens.has(i)) hitInds.add(i);
  }
  return {
    fields: {
      total: (meta.fields || []).length,
      covered: hitFields.size,
      missing: (meta.fields || []).filter((f) => !hitFields.has(f)),
    },
    indicators: {
      total: (meta.indicators || []).length,
      covered: hitInds.size,
      missing: (meta.indicators || []).filter((i) => !hitInds.has(i)),
    },
  };
}

export function formatCoverageMatrix(m) {
  const lines = [];
  lines.push('--- 覆盖矩阵 ---');
  lines.push(
    `字段: ${m.fields.covered}/${m.fields.total}` +
    (m.fields.missing.length ? ` (缺: ${m.fields.missing.join(', ')})` : '')
  );
  lines.push(
    `算子: ${m.indicators.covered}/${m.indicators.total}` +
    (m.indicators.missing.length ? ` (缺: ${m.indicators.missing.join(', ')})` : '')
  );
  if (m.uncoveredFields && m.uncoveredFields.length) {
    lines.push(`无生成器的字段: ${m.uncoveredFields.join(', ')}`);
  }
  if (m.uncoveredInds && m.uncoveredInds.length) {
    lines.push(`无生成器的算子: ${m.uncoveredInds.join(', ')}`);
  }
  return lines.join('\n');
}

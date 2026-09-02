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
  VOL: { q: '成交量大于100万股的股票', sub: ['VOL'] },
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
  LIMIT_UP_PCT: { q: '涨停的股票', sub: ['LIMIT_UP_PCT'] },
};

// 算子生成器：为每个算子写最佳口语查询。
export const IND_GEN = {
  ABS: { q: '收盘价距20日均线绝对偏差大于2元的股票', sub: ['ABS'] },
  ATR: { q: '14日真实波幅均值大于3的股票', sub: ['ATR'] },
  BARSLAST: { q: '上一次收盘价跌破20日均线距离现在已超过5天的股票', sub: ['BARSLAST'] },
  BOLL_LOWER: { q: '收盘价跌破布林下轨的股票', sub: ['BOLL_LOWER'] },
  BOLL_UPPER: { q: '收盘价突破布林上轨的股票', sub: ['BOLL_UPPER'] },
  COUNT: { q: '近5日收盘价站上20日均线的天数不少于3天的股票', sub: ['COUNT'] },
  CROSS_DOWN: { q: '5日均线下穿30日均线的股票', sub: ['CROSS_DOWN'] },
  CROSS_UP: { q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP'] },
  AROON_DOWN: { q: '阿隆下降大于50的股票', sub: ['AROON_DOWN'] },
  AROON_UP: { q: '阿隆上升大于80的股票', sub: ['AROON_UP'] },
  BBI: { q: '收盘价站上多空指标的股票', sub: ['BBI'] },
  BIAS: { q: '20日乖离率大于5%的股票', sub: ['BIAS'] },
  BOLL_MID: { q: '收盘价高于布林中轨的股票', sub: ['BOLL_MID'] },
  CCI: { q: '14日CCI突破100的股票', sub: ['CCI'] },
  CR: { q: '20日能量指标大于100的股票', sub: ['CR'] },
  DEMA: { q: '收盘价站上20日双重指数均线的股票', sub: ['DEMA'] },
  DMI_ADX: { q: '14日ADX大于25的股票', sub: ['DMI_ADX'] },
  DMI_MDI: { q: '14日-DI小于20的股票', sub: ['DMI_MDI'] },
  DMI_PDI: { q: '14日+DI大于-DI的股票', sub: ['DMI_PDI'] },
  KDJ_J: { q: 'KDJ的J值大于100的股票', sub: ['KDJ_J'] },
  MFI: { q: '14日资金流量指数小于20的股票', sub: ['MFI'] },
  OBV: { q: '能量潮OBV大于0的股票', sub: ['OBV'] },
  PPO: { q: '价格振荡PPO大于0的股票', sub: ['PPO'] },
  PSY: { q: '12日心理线大于60的股票', sub: ['PSY'] },
  SAR: { q: '收盘价站上抛物线SAR的股票', sub: ['SAR'] },
  TEMA: { q: '收盘价站上20日三重指数均线的股票', sub: ['TEMA'] },
  TRIX: { q: '12日TRIX大于0的股票', sub: ['TRIX'] },
  UO: { q: '终极摆动指标大于50的股票', sub: ['UO'] },
  VR: { q: '14日量比大于150的股票', sub: ['VR'] },
  VWAP: { q: '收盘价低于20日量价均价的股票', sub: ['VWAP'] },
  WR: { q: '14日威廉指标大于80的股票', sub: ['WR'] },
  EMA: { q: '收盘价站上20日指数均线的股票', sub: ['EMA'] },
  HHV: { q: '创20日新高的股票', sub: ['HHV'] },
  KDJ_D: { q: 'KDJ的D值大于80的股票', sub: ['KDJ_D'] },
  KDJ_K: { q: 'KDJ的K值大于80的股票', sub: ['KDJ_K'] },
  LLV: { q: '创20日新低的股票', sub: ['LLV'] },
  MA: { q: '收盘价站上20日均线的股票', sub: ['MA'] },
  MACD_DIF: { q: 'MACD的DIF值大于0的股票', sub: ['MACD_DIF'] },
  MACD_DEA: { q: 'MACD的DEA值大于0的股票', sub: ['MACD_DEA'] },
  MACD_HIST: { q: 'MACD柱大于0的股票', sub: ['MACD_HIST'] },
  MAX: { q: '今日开盘价与收盘价中的较高者上穿20日均线的股票', sub: ['MAX'] },
  MIN: { q: '开盘价与收盘价取较小值后小于昨日最低价的股票', sub: ['MIN'] },
  REF: { q: '今日收盘价高于昨日收盘价的股票', sub: ['REF'] },
  ROC: { q: '5日变动率大于5%的股票', sub: ['ROC'] },
  RSI: { q: '14日RSI大于70的股票', sub: ['RSI'] },
  STD: { q: '20日收盘价标准差大于2的股票', sub: ['STD'] },
  SUM: { q: '近5日成交额之和大于100亿的股票', sub: ['SUM'] },
};

// 现有用例断言 token 前缀匹配任意注册表字段/算子，判定该字段/算子已被手工用例覆盖。
// 注意：sub_any（多解，如 COUNT/REF 任选其一）不参与覆盖判定——否则模型只落一个解时
// 另一算子在若干轮随机中可能一直无专属用例，导致矩阵缺漏。故 sub_any 仅在执行时校验。
function coveredTokens(meta, existingCases) {
  const coveredFields = new Set();
  const coveredInds = new Set();
  const allTokens = [...meta.fields, ...meta.indicators];
  for (const c of existingCases) {
    for (const s of c.sub || []) {
      const up = String(s).toUpperCase();
      for (const tok of allTokens) {
        // 按令牌边界匹配：'MA(' 命中 MA，但 'CROSS_UP' 不命中 CR/CROSS
        if (up === tok || up.startsWith(tok) && !/[A-Z0-9_]/.test(up[tok.length])) {
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

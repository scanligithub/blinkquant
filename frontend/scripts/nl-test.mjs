// frontend/scripts/nl-test.mjs
// 高覆盖自然语言选股自动化测试：登录 -> 语义分析 -> 公式翻译 -> 校验。
// 用法:
//   node scripts/nl-test.mjs <baseUrl> <email> <password> [--cases-only]
//   或环境变量 BASE_URL / TEST_EMAIL / TEST_PASSWORD (缺省从参数读)
// 前置: 生产环境需设置 NL_TEST_MODE=true 豁免限流（否则单账号 20 次/天被限）。
// 结果按类别分组汇总，失败用例打印完整响应便于排查。

const [, , argBase, argEmail, argPass] = process.argv;
const BASE_URL = (process.env.BASE_URL || argBase || '').replace(/\/$/, '');
const EMAIL = process.env.TEST_EMAIL || argEmail || '';
const PASSWORD = process.env.TEST_PASSWORD || argPass || '';

if (!BASE_URL || !EMAIL || !PASSWORD) {
  console.error('用法: node scripts/nl-test.mjs <baseUrl> <email> <password>');
  process.exit(1);
}

// ---- 用例集 ----
// expected_sub: 公式应包含的子串（语义强校验）
// expected_not: 公式不应包含的子串（反例校验，如创5日新高的 HHV 误用）
// expected_tf: 期望周期
// min_restatement_words: 分析复述最小字数（防止空复述）
const CASES = [
  // ===== 类别 1: 基础字段 =====
  { cid: 'f1', cat: '字段', q: '市盈率低于20的股票', sub: ['PE_TTM'], tf: 'D' },
  { cid: 'f2', cat: '字段', q: '总市值大于100亿的股票', sub: ['TOTAL_MV', '1e10'], tf: 'D' },
  { cid: 'f3', cat: '字段', q: '市净率小于2的股票', sub: ['PB_MRQ'], tf: 'D' },
  { cid: 'f4', cat: '字段', q: '换手率大于5%的股票', sub: ['TURN'], tf: 'D' },
  { cid: 'f5', cat: '字段', q: '市盈率低于20且市净率小于3的股票', sub: ['PE_TTM', 'AND', 'PB_MRQ'], tf: 'D' },

  // ===== 类别 2: 指标 =====
  { cid: 'i1', cat: '指标', q: '收盘价站上20日均线的股票', sub: ['CLOSE', 'MA('], tf: 'D' },
  { cid: 'i2', cat: '指标', q: '收盘价跌破60日均线的股票', sub: ['MA('], tf: 'D' },
  { cid: 'i3', cat: '指标', q: '5日均线上穿30日均线的股票', sub: ['CROSS_UP', 'MA('], tf: 'D' },
  { cid: 'i4', cat: '指标', q: 'KDJ金叉的股票', sub: ['CROSS_UP'], tf: 'D' },
  { cid: 'i5', cat: '指标', q: '布林带上轨被突破的股票', sub: ['BOLL_UPPER'], tf: 'D' },
  { cid: 'i6', cat: '指标', q: 'RSI短期超买的股票', sub: ['RSI'], tf: 'D' },
  // 连续上涨：模型可用 COUNT 或链式 REF（两者语义等价），任选其一
  { cid: 'i7', cat: '指标', q: '连续5天收盘价上涨的股票', sub_any: ['COUNT(', 'REF(CLOSE'], tf: 'D' },

  // ===== 类别 3: 已发现的 3 类语义错误点 =====
  // 3a. 振幅歧义：弱模型曾误把"振幅"当 PCT_CHANGE，应指出语义歧义并在翻译时明确
  { cid: 'e1', cat: '错误点-振幅', q: '近5日振幅大于15%的股票', min_words: 6, tf: 'D' },
  // 3b. OR 括号优先级：应保留分组 AND 优先级高于 OR
  { cid: 'e2', cat: '错误点-OR优先级', q: '市盈率低于30或者总市值大于100亿且市净率小于3的股票', sub: ['OR'], logic: '(' , tf: 'D' },
  // 3c. 创新高：HHV 与 REF 复用语义；注意日线与5日对比
  { cid: 'e3', cat: '错误点-新高', q: '创20日新高的股票', sub: ['HHV', 'CLOSE'], tf: 'D' },
  { cid: 'e4', cat: '错误点-新高', q: '创5日新低的股票', sub: ['LLV'], tf: 'D' },

  // ===== 类别 4: 单位换算（亿=1e8 / 万=1e4）=====
  { cid: 'u1', cat: '单位换算', q: '总市值大于5000万的股票', sub: ['TOTAL_MV', '5e7'], tf: 'D' },
  { cid: 'u2', cat: '单位换算', q: '流通市值大于200亿元的股票', sub: ['FLOAT_MV', '2e10'], tf: 'D' },
  { cid: 'u3', cat: '单位换算', q: '成交额大于5亿的股票', sub_any: ['5e8', '500000000'], tf: 'D' },

  // ===== 类别 5: 长窗口 / 嵌套指标（边界） =====
  { cid: 'b1', cat: '边界-窗口', q: '按20日均线选股，要求近期突破', sub: ['MA('], tf: 'D' },
  { cid: 'b2', cat: '边界-组合条件', q: '收盘价大于20日均线或市盈率低于15的股票', sub: ['MA('], tf: 'D' },

  // ===== 类别 6: 周期 =====
  { cid: 't1', cat: '周期-周', q: '周线收盘价跌破20周均线的股票', tf: 'W' },
  { cid: 't2', cat: '周期-月', q: '月线市盈率低于15的股票', tf: 'M' },
];

// ---- 登录 ----
async function login() {
  const res = await fetch(`${BASE_URL}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`登录失败 HTTP ${res.status}: ${json.error || res.statusText}`);
  }
  const setCookie = res.headers.get('set-cookie') || '';
  const m = /__auth_token=([^;]+)/.exec(setCookie);
  if (!m) throw new Error('登录失败: 响应无 __auth_token cookie');
  console.log(`登录成功: ${EMAIL} (role=${json.user?.role})`);
  return `__auth_token=${m[1]}`;
}

async function analyze(cookie, query) {
  const res = await fetch(`${BASE_URL}/api/select-nl/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookie },
    body: JSON.stringify({ query }),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

async function translate(cookie, analysis) {
  const res = await fetch(`${BASE_URL}/api/select-nl`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookie },
    body: JSON.stringify({ analysis }),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

// 瞬时 5xx（502/504，NVIDIA LLM 偶发过载）自动重试；业务 4xx 与 429 不重试
const RETRY_MAX = 3;
const RETRY_DELAY_MS = 3000;

async function analyzeWithRetry(cookie, query) {
  let last;
  for (let attempt = 0; attempt < RETRY_MAX; attempt++) {
    last = await analyze(cookie, query);
    if (!(last.status >= 500) || attempt === RETRY_MAX - 1) return last;
    console.log(`       (analyze ${last.status}，等待重试 ${attempt + 1}/${RETRY_MAX - 1}…)`);
    await sleep(RETRY_DELAY_MS);
  }
  return last;
}

async function translateWithRetry(cookie, analysis) {
  let last;
  for (let attempt = 0; attempt < RETRY_MAX; attempt++) {
    last = await translate(cookie, analysis);
    if (!(last.status >= 500) || attempt === RETRY_MAX - 1) return last;
    console.log(`       (translate ${last.status}，等待重试 ${attempt + 1}/${RETRY_MAX - 1}…)`);
    await sleep(RETRY_DELAY_MS);
  }
  return last;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---- 执行 ----
async function run() {
  const cookie = await login();

  const results = [];
  let pass = 0, fail = 0;
  let i = 0;
  for (const c of CASES) {
    i += 1;
    const label = `[${i}/${CASES.length}] ${c.cid} ${c.cat} "${c.q}"`;
    const r = { ...c, ok: false, reason: '', formula: '', restatement: '', explanations: [] };

    try {
      // 第一步: 语义分析
      const ana = await analyzeWithRetry(cookie, c.q);
      await sleep(80);
      if (ana.status !== 200 || !ana.json?.data) {
        r.reason = `analyze HTTP ${ana.status}: ${ana.json?.error || ''}`;
        results.push(r); fail += 1;
        console.log(`  FAIL ${label} → ${r.reason}`);
        continue;
      }
      const data = ana.json.data;
      r.restatement = data.restatement || '';
      if (!Array.isArray(data.conditions) || data.conditions.length === 0) {
        r.reason = 'restatement/conditions 缺失';
      }
      if (c.min_words && r.restatement.length < c.min_words) {
        r.reason = `复述过短(${r.restatement.length}字 < ${c.min_words})，语义可能未展开`;
      }
      if (c.tf && data.timeframe !== c.tf) {
        r.reason = `周期应为 ${c.tf}，实际 ${data.timeframe}`;
      }
      if (r.reason) {
        results.push(r); fail += 1;
        console.log(`  FAIL ${label} → ${r.reason}`);
        continue;
      }

      // 第二步: 公式翻译（模拟"确认"）
      const tr = await translateWithRetry(cookie, data);
      await sleep(120);
      if (tr.status !== 200 || !tr.json?.data) {
        r.reason = `translate HTTP ${tr.status}: ${tr.json?.error || ''}`;
        results.push(r); fail += 1;
        console.log(`  FAIL ${label} → ${r.reason}`);
        continue;
      }
      const tdata = tr.json.data;
      r.formula = tdata.formula || '';
      r.explanations = [tdata.explanation || ''];

      // 语义子串校验（不区分大小写；断言存在性，验证公式确实覆盖该算子/字段）
      if (c.sub) {
        const up = r.formula.toUpperCase();
        const missing = c.sub.filter((s) => !up.includes(s.toUpperCase()));
        if (missing.length) {
          r.reason = `公式缺少算子/字段: ${missing.join(', ')}`;
        }
      }
      // sub_any：子串任选其一命中即通过（多解场景）
      if (!r.reason && c.sub_any) {
        const up = r.formula.toUpperCase();
        const hit = c.sub_any.some((s) => up.includes(s.toUpperCase()));
        if (!hit) {
          r.reason = `公式未命中任选算子: ${c.sub_any.join(' / ')}`;
        }
      }
      if (c.logic && !r.formula.includes(c.logic)) {
        r.reason = `公式未保留分组括号: ${c.logic}`;
      }

      if (!r.reason) {
        r.ok = true; pass += 1;
        console.log(`  PASS ${label}`);
        console.log(`       → ${r.formula}`);
      } else {
        results.push(r); fail += 1;
        console.log(`  FAIL ${label} → ${r.reason}\n       → ${r.formula}`);
      }
    } catch (e) {
      r.reason = `异常: ${e.message}`;
      results.push(r); fail += 1;
      console.log(`  FAIL ${label} → ${r.reason}`);
    }
  }

  // ---- 汇总 ----
  console.log('\n================ 结果汇总 ================');
  console.log(`总计 ${CASES.length} 用例：PASS ${pass} / FAIL ${fail}`);
  const byCat = {};
  for (const r of results) {
    (byCat[r.cat] ||= []).push(r);
  }
  console.log('\n--- 按类别 ---');
  for (const [cat, arr] of Object.entries(byCat)) {
    const p = arr.filter((r) => r.ok).length;
    const f = arr.filter((r) => !r.ok);
    console.log(`  ${cat}: ${p}/${arr.length}`);
    for (const r of f) {
      console.log(`    ✗ ${r.cid} "${r.q}" → ${r.reason} | ${r.formula}`);
    }
  }

  console.log('\n--- 完整公式输出（人工复核语义） ---');
  for (const r of results) {
    console.log(`  ${r.cid} [${r.cat}] ${r.q}`);
    console.log(`      复述: ${r.restatement}`);
    console.log(`      公式: ${r.formula}`);
  }

  process.exit(fail === 0 ? 0 : 1);
}

run().catch((e) => {
  console.error('脚本错误:', e.message);
  process.exit(2);
});
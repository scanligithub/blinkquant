// frontend/tests/apply-adjust.test.mjs
// Copy implementation to avoid TS import (same as src/utils/applyAdjust.ts)
import { test } from 'node:test';
import assert from 'node:assert/strict';

function applyAdjust(bars, mode) {
  if (!Array.isArray(bars) || bars.length === 0) return [];
  const factors = [];
  let last = 1.0;
  for (const bar of bars) {
    const raw = bar.adjustFactor;
    if (raw == null) {
      factors.push(last);
    } else {
      last = Number(raw);
      factors.push(last);
    }
  }
  const first = factors[0] ?? 1.0;
  const latest = factors[factors.length - 1] ?? 1.0;
  const safeLatest = latest > 0 ? latest : 1.0;
  const safeFirst = safeLatest > 0 ? first : 1.0;
  return bars.map((bar, idx) => {
    const f = factors[idx];
    let priceMul = 1;
    let volMul = 1;
    if (mode === 'none') {
      priceMul = safeLatest / f;
      volMul = f / safeLatest;
    } else if (mode === 'hfq') {
      priceMul = safeLatest / safeFirst;
      volMul = safeFirst / safeLatest;
    }
    return {
      time: bar.time,
      open: bar.open * priceMul,
      high: bar.high * priceMul,
      low: bar.low * priceMul,
      close: bar.close * priceMul,
      volume: bar.volume * volMul,
      main_net: bar.main_net,
      adjustFactor: bar.adjustFactor,
    };
  });
}

function roundBar(b) {
  const r = n => Number(n.toFixed(6));
  return { ...b, open: r(b.open), high: r(b.high), low: r(b.low), close: r(b.close), volume: r(b.volume) };
}

test('前复权不变', () => {
  const bars = [
    { time: 1, open: 10, high: 12, low: 9, close: 11, volume: 1000, main_net: 0, adjustFactor: 2 },
    { time: 2, open: 11, high: 13, low: 10, close: 12, volume: 1100, main_net: 0, adjustFactor: 2 },
  ];
  const res = applyAdjust(bars, 'qfq').map(roundBar);
  const exp = bars.map(roundBar);
  assert.deepEqual(res, exp);
});

test('无复权还原', () => {
  const bars = [
    { time: 1, open: 5, high: 6, low: 4, close: 5.5, volume: 2000, main_net: 0, adjustFactor: 0.5 },
    { time: 2, open: 10, high: 12, low: 9, close: 11, volume: 4000, main_net: 0, adjustFactor: 0.5 },
  ];
  const res = applyAdjust(bars, 'none').map(roundBar);
  const exp = bars.map(roundBar);
  assert.deepEqual(res, exp);
});

test('后复权常数倍率', () => {
  const bars = [
    { time: 1, open: 5, high: 6, low: 4, close: 5.5, volume: 2000, main_net: 0, adjustFactor: 0.5 },
    { time: 2, open: 10, high: 12, low: 9, close: 11, volume: 4000, main_net: 0, adjustFactor: 0.5 },
  ];
  const res = applyAdjust(bars, 'hfq').map(roundBar);
  const exp = bars.map(roundBar);
  assert.deepEqual(res, exp);
});

test('null 因子前向填充', () => {
  const bars = [
    { time: 1, open: 5, high: 6, low: 4, close: 5.5, volume: 2000, main_net: 0, adjustFactor: null },
    { time: 2, open: 10, high: 12, low: 9, close: 11, volume: 4000, main_net: 0, adjustFactor: 2 },
    { time: 3, open: 20, high: 22, low: 19, close: 21, volume: 8000, main_net: 0, adjustFactor: null },
  ];
  const res = applyAdjust(bars, 'none').map(roundBar);
const expected = [
      { time:1, open:10, high:12, low:8, close:11, volume:1000, main_net:0, adjustFactor: null },
      { time:2, open:10, high:12, low:9, close:11, volume:4000, main_net:0, adjustFactor: 2 },
      { time:3, open:20, high:22, low:19, close:21, volume:8000, main_net:0, adjustFactor: null },
    ].map(roundBar);
  assert.deepEqual(res, expected);
});

test('全 null 因子等价', () => {
  const bars = [
    { time:1, open:5, high:6, low:4, close:5.5, volume:2000, main_net:0, adjustFactor: null },
    { time:2, open:10, high:12, low:9, close:11, volume:4000, main_net:0, adjustFactor: null },
  ];
  const resNone = applyAdjust(bars, 'none').map(roundBar);
  const resQfq = applyAdjust(bars, 'qfq').map(roundBar);
  const resHfq = applyAdjust(bars, 'hfq').map(roundBar);
  const exp = bars.map(roundBar);
  assert.deepEqual(resNone, exp);
  assert.deepEqual(resQfq, exp);
  assert.deepEqual(resHfq, exp);
});

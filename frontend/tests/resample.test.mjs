// frontend/tests/resample.test.mjs
// Copy implementation to avoid TS import (same as src/app/page.tsx resampleData)
import { test } from 'node:test';
import assert from 'node:assert/strict';

function resampleData(dailyData, targetTimeframe) {
  if (targetTimeframe === 'D') return dailyData;
  const grouped = new Map();
  dailyData.forEach(item => {
    const date = new Date(item.time * 1000);
    let key;
    if (targetTimeframe === 'W') {
      const dayOfWeek = date.getDay();
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - dayOfWeek);
      key = weekStart.toISOString().split('T')[0];
    } else {
      key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
    }
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  });
  const resampled = [];
  grouped.forEach(items => {
    const sortedItems = items.sort((a, b) => a.time - b.time);
    const first = sortedItems[0];
    const last = sortedItems[sortedItems.length - 1];
resampled.push({
  time: first.time,
  open: first.open,
  high: Math.max(...sortedItems.map(i => i.high)),
  low: Math.min(...sortedItems.map(i => i.low)),
  close: last.close,
  volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
  amount: sortedItems.reduce((sum, i) => sum + (i.amount || 0), 0),
  turn: last.turn,
  peTTM: last.peTTM,
  total_mv: last.total_mv,
  float_mv: last.float_mv,
  main_net: sortedItems.reduce((sum, i) => sum + (i.main_net || 0), 0), // 聚合资金流数据
});
  });
  return resampled.sort((a, b) => a.time - b.time);
}

function deepClone(arr) {
  return JSON.parse(JSON.stringify(arr));
}

test('daily pass-through returns same array', () => {
  const data = [
    { time: 0, open: 10, high: 12, low: 9, close: 11, volume: 100, main_net: 5 },
    { time: 86400, open: 11, high: 13, low: 10, close: 12, volume: 150, main_net: 6 },
  ];
  const res = resampleData(deepClone(data), 'D');
  assert.deepEqual(res, data);
});

test('weekly aggregation includes financial fields', () => {
  const data = [
    { time: 0, open: 10, high: 12, low: 9, close: 11, volume: 100, amount: 200, turn: 0.5, peTTM: 10, total_mv: 1000, float_mv: 500, main_net: 5 },
    { time: 86400, open: 11, high: 13, low: 10, close: 12, volume: 150, amount: 300, turn: 0.6, peTTM: 11, total_mv: 1100, float_mv: 550, main_net: 6 },
  ];
  const res = resampleData(data, 'W');
  const expected = [{
    time: 0,
    open: 10,
    high: 13,
    low: 9,
    close: 12,
    volume: 250,
    amount: 500,
    turn: 0.6,
    peTTM: 11,
    total_mv: 1100,
    float_mv: 550,
    main_net: 11,
  }];
  assert.deepEqual(res, expected);
});

test('missing financial fields handled gracefully', () => {
  const data = [
    { time: 0, open: 10, high: 12, low: 9, close: 11, volume: 100, main_net: 5 },
    { time: 86400, open: 11, high: 13, low: 10, close: 12, volume: 150, main_net: 6 },
  ];
  const res = resampleData(data, 'W');
  const expected = [{
    time: 0,
    open: 10,
    high: 13,
    low: 9,
    close: 12,
    volume: 250,
    amount: 0,
    turn: undefined,
    peTTM: undefined,
    total_mv: undefined,
    float_mv: undefined,
    main_net: 11,
  }];
  assert.deepEqual(res, expected);
});

import { test } from 'node:test';
import assert from 'node:assert/strict';

// 复制实现以规避 TS import（沿用 apply-adjust.test.mjs 模式）
function parseParquetRecords(records) {
  return records.map((record) => {
    let timeValue;
    if (record.date instanceof Date) timeValue = Math.floor(record.date.getTime() / 1000);
    else throw new Error('Invalid date');
    return {
      time: timeValue,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      volume: record.volume,
      amount: record.amount,
      turn: record.turn,
      peTTM: record.peTTM,
      total_mv: record.total_mv,
      float_mv: record.float_mv,
      main_net: record.main_net || 0,
      adjustFactor: record.adjustFactor,
    };
  });
}

test('parseParquetRecords 转换 Date 为 epoch 秒', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].time, Math.floor(date.getTime() / 1000));
  assert.equal(result[0].close, 1.5);
});

test('parseParquetRecords 兜底 main_net 为 0', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100, main_net: 88 }]);
  assert.equal(result[0].main_net, 88);
  const noNet = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(noNet[0].main_net, 0);
});

test('parseParquetRecords 保留 adjustFactor 缺失为 undefined', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].adjustFactor, undefined);
  const withFactor = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100, adjustFactor: 1.2 }]);
  assert.equal(withFactor[0].adjustFactor, 1.2);
});

test('parseParquetRecords 非法日期抛错', () => {
  assert.throws(() => parseParquetRecords([{ date: '2024-01-02', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]), /Invalid date/);
});

test('parseParquetRecords 透传财务字段', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{
    date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100,
    amount: 123456, turn: 3.5, peTTM: 20.1, total_mv: 1e10, float_mv: 5e9,
  }]);
  assert.equal(result[0].amount, 123456);
  assert.equal(result[0].turn, 3.5);
  assert.equal(result[0].peTTM, 20.1);
  assert.equal(result[0].total_mv, 1e10);
  assert.equal(result[0].float_mv, 5e9);
});

test('parseParquetRecords 财务字段缺失时为 undefined', () => {
  const date = new Date('2024-01-02T00:00:00Z');
  const result = parseParquetRecords([{ date, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }]);
  assert.equal(result[0].amount, undefined);
  assert.equal(result[0].turn, undefined);
  assert.equal(result[0].peTTM, undefined);
  assert.equal(result[0].total_mv, undefined);
  assert.equal(result[0].float_mv, undefined);
});

import { LineData } from 'lightweight-charts';

/**
 * 计算布林带 (BOLL)
 * @param data K 线数据数组
 * @param period 周期，默认 20
 * @param multiplier 标准差倍数，默认 2
 * @returns 包含上轨、中轨、下轨的对象
 */
export function calculateBoll(data: any[], period: number = 20, multiplier: number = 2) {
  const closes = data.map(item => item.close);
  const upper: LineData[] = [];
  const middle: LineData[] = [];
  const lower: LineData[] = [];

  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      upper.push({ time: data[i].time, value: 0 });
      middle.push({ time: data[i].time, value: 0 });
      lower.push({ time: data[i].time, value: 0 });
      continue;
    }

    const slice = closes.slice(i - period + 1, i + 1);
    const ma = slice.reduce((a, b) => a + b, 0) / period;
    const std = Math.sqrt(slice.reduce((a, b) => a + Math.pow(b - ma, 2), 0) / period);

    middle.push({ time: data[i].time, value: ma });
    upper.push({ time: data[i].time, value: ma + multiplier * std });
    lower.push({ time: data[i].time, value: ma - multiplier * std });
  }

  return { upper, middle, lower };
}

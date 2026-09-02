import { LineData } from 'lightweight-charts';

/**
 * 计算 WR (威廉指标)
 * @param data K 线数据数组
 * @param period 周期，默认 14
 * @returns WR 数据数组
 */
export function calculateWR(data: any[], period: number = 14): LineData[] {
  const result: LineData[] = [];

  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push({ time: data[i].time, value: -100 });
      continue;
    }

    let highest = -Infinity, lowest = Infinity;
    for (let j = 0; j < period; j++) {
      if (data[i - j].high > highest) highest = data[i - j].high;
      if (data[i - j].low < lowest) lowest = data[i - j].low;
    }

    const wr = (highest - data[i].close) / (highest - lowest) * -100;
    result.push({ time: data[i].time, value: wr });
  }

  return result;
}

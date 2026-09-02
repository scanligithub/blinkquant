import { LineData } from 'lightweight-charts';

/**
 * 计算加权移动平均线 (WMA)
 * @param data K 线数据数组
 * @param period 周期
 * @returns 加权移动平均线数据数组
 */
export function calculateWMA(data: any[], period: number): LineData[] {
  const closes = data.map(item => item.close);
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push({ time: data[i].time, value: 0 });
      continue;
    }
    let sum = 0;
    let weightSum = 0;
    for (let j = 0; j < period; j++) {
      const weight = j + 1;
      sum += closes[i - j] * weight;
      weightSum += weight;
    }
    result.push({ time: data[i].time, value: sum / weightSum });
  }
  return result;
}

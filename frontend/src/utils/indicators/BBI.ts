import { LineData } from 'lightweight-charts';

/**
 * 计算 BBI (多空分界指标)
 * @param data K 线数据数组
 * @returns BBI 数据数组
 */
export function calculateBBI(data: any[]): LineData[] {
  const result: LineData[] = [];
  const periods = [3, 6, 12, 24];

  for (let i = 0; i < data.length; i++) {
    if (i < 23) {
      result.push({ time: data[i].time, value: 0 });
      continue;
    }

    let sum = 0;
    for (const p of periods) {
      let maSum = 0;
      for (let j = 0; j < p; j++) {
        maSum += data[i - j].close;
      }
      sum += maSum / p;
    }
    result.push({ time: data[i].time, value: sum / periods.length });
  }

  return result;
}

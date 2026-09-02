import { LineData } from 'lightweight-charts';

/**
 * 计算平滑移动平均线 (SMMA)
 * @param data K 线数据数组
 * @param period 周期
 * @returns 平滑移动平均线数据数组
 */
export function calculateSMMA(data: any[], period: number): LineData[] {
  const closes = data.map(item => item.close);
  const result: LineData[] = [];
  let smma = 0;
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push({ time: data[i].time, value: 0 });
      continue;
    }
    if (i === period - 1) {
      smma = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
    } else {
      smma = (smma * (period - 1) + closes[i]) / period;
    }
    result.push({ time: data[i].time, value: smma });
  }
  return result;
}

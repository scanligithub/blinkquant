import { LineData } from 'lightweight-charts';

/**
 * 计算指数移动平均线 (EMA)
 * @param data K 线数据数组，每个元素包含 time 和 close 字段
 * @param period 周期
 * @returns 指数移动平均线数据数组
 */
export function calculateEMAForData(data: any[], period: number): LineData[] {
  const closes = data.map(item => item.close);
  const result: number[] = [];
  const multiplier = 2 / (period + 1);
  for (let i = 0; i < period - 1 && i < closes.length; i++) result.push(0);
  if (closes.length < period) return data.map(d => ({ time: d.time, value: 0 }));

  let sum = 0;
  for (let i = 0; i < period; i++) sum += closes[i];
  result.push(sum / period);

  for (let i = period; i < closes.length; i++) {
    const ema = (closes[i] - result[i - 1]) * multiplier + result[i - 1];
    result.push(ema);
  }

  return data.map((d, i) => ({ time: d.time, value: result[i] || 0 }));
}

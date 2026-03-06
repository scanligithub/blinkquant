import { LineData } from 'lightweight-charts';

/**
 * 计算简单移动平均线 (MA)
 * @param data K 线数据数组，每个元素包含 time 和 close 字段
 * @param period 周期
 * @returns 移动平均线数据数组
 */
export function calculateMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) continue;
    let sum = 0;
    for (let j = 0; j < period; j++) sum += data[i - j].close;
    result.push({ time: data[i].time, value: sum / period });
  }
  return result;
}

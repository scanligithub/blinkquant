import { LineData } from 'lightweight-charts';

/**
 * 计算成交量移动平均 (VMA)
 * @param data 成交量数据数组，每个元素包含 time 和 value 字段
 * @param period 周期
 * @returns VMA 数据数组
 */
export function calculateVolumeMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) continue;
    let sum = 0;
    for (let j = 0; j < period; j++) sum += data[i - j].value;
    result.push({ time: data[i].time, value: sum / period });
  }
  return result;
}

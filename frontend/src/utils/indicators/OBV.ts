import { LineData } from 'lightweight-charts';

/**
 * 计算 OBV (累积量)
 * @param data K 线数据数组，包含 close 和 volume 字段
 * @returns OBV 数据数组
 */
export function calculateOBV(data: any[]): LineData[] {
  const result: LineData[] = [];
  let obv = 0;

  for (let i = 0; i < data.length; i++) {
    if (i === 0) {
      obv = 0;
    } else if (data[i].close > data[i - 1].close) {
      obv += data[i].volume || 0;
    } else if (data[i].close < data[i - 1].close) {
      obv -= data[i].volume || 0;
    }
    result.push({ time: data[i].time, value: obv });
  }

  return result;
}

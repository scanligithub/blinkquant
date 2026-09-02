import { LineData } from 'lightweight-charts';

/**
 * 计算成交量加权平均价 (VWAP)
 * @param data K 线数据数组，包含 high, low, close, volume 字段
 * @returns VWAP 数据数组
 */
export function calculateVWAP(data: any[]): LineData[] {
  const result: LineData[] = [];
  let cumulativeTP = 0;
  let cumulativeVol = 0;
  for (let i = 0; i < data.length; i++) {
    const tp = (data[i].high + data[i].low + data[i].close) / 3;
    const vol = data[i].volume || 0;
    cumulativeTP += tp * vol;
    cumulativeVol += vol;
    result.push({ time: data[i].time, value: cumulativeVol > 0 ? cumulativeTP / cumulativeVol : 0 });
  }
  return result;
}

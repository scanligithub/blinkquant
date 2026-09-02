import { LineData } from 'lightweight-charts';

/**
 * 计算 CCI (顺势指标)
 * @param data K 线数据数组
 * @param period 周期，默认 20
 * @returns CCI 数据数组
 */
export function calculateCCI(data: any[], period: number = 20): LineData[] {
  const result: LineData[] = [];

  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      result.push({ time: data[i].time, value: 0 });
      continue;
    }

    let tpSum = 0;
    for (let j = 0; j < period; j++) {
      tpSum += (data[i - j].high + data[i - j].low + data[i - j].close) / 3;
    }
    const tpAvg = tpSum / period;

    let md = 0;
    for (let j = 0; j < period; j++) {
      const tp = (data[i - j].high + data[i - j].low + data[i - j].close) / 3;
      md += Math.abs(tp - tpAvg);
    }
    md = md / period;

    const cci = md === 0 ? 0 : ((tpAvg - (data[i].high + data[i].low + data[i].close) / 3) / (0.015 * md));
    result.push({ time: data[i].time, value: cci });
  }

  return result;
}

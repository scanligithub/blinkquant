import { LineData } from 'lightweight-charts';
import { calculateEMAForData } from './EMA';

/**
 * 计算 MACD 指标
 * @param data K 线数据数组
 * @param fastPeriod 快线周期，默认 12
 * @param slowPeriod 慢线周期，默认 26
 * @param signalPeriod 信号线周期，默认 9
 * @returns 包含 DIF、DEA、MACD 柱状图的对象
 */
export function calculateMACD(data: any[], fastPeriod: number = 12, slowPeriod: number = 26, signalPeriod: number = 9) {
  const closes = data.map(item => item.close);
  const fastEMA = calculateEMAForData(data, fastPeriod).map(d => d.value);
  const slowEMA = calculateEMAForData(data, slowPeriod).map(d => d.value);

  const macdLine: LineData[] = [];
  for (let i = slowPeriod - 1; i < data.length; i++) {
    const dif = fastEMA[i] - slowEMA[i];
    if (!isNaN(dif) && isFinite(dif)) macdLine.push({ time: data[i].time, value: dif });
  }

  const macdValues = macdLine.map(item => item.value);
  const signalEMA = calculateEMAForData(macdLine.map((d, i) => ({ time: d.time, close: d.value })), signalPeriod).map(d => d.value);
  const signalLine: LineData[] = [];
  for (let i = signalPeriod - 1; i < macdLine.length; i++) {
    const dea = signalEMA[i];
    if (!isNaN(dea) && isFinite(dea)) signalLine.push({ time: macdLine[i].time, value: dea });
  }

  const histogram: any[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    const macdValue = macdLine[i + signalPeriod - 1]?.value || 0;
    const signalValue = signalLine[i]?.value || 0;
    const diff = macdValue - signalValue;
    histogram.push({
      time: signalLine[i].time,
      value: diff * 2,
      color: diff >= 0 ? '#ef4444' : '#22c55e',
    });
  }
  return { macdLine, signalLine, histogram };
}

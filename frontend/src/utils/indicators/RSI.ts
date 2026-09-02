import { LineData } from 'lightweight-charts';

/**
 * 计算 RSI (相对强弱指数)
 * @param data K 线数据数组
 * @param period 周期，默认 14
 * @returns RSI 数据数组
 */
export function calculateRSI(data: any[], period: number = 14): LineData[] {
  const closes = data.map(item => item.close);
  const result: LineData[] = [];
  let avgGain = 0, avgLoss = 0;

  for (let i = 0; i < data.length; i++) {
    if (i === 0) {
      result.push({ time: data[i].time, value: 50 });
      continue;
    }

    const change = closes[i] - closes[i - 1];
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? -change : 0;

    if (i < period) {
      avgGain += gain;
      avgLoss += loss;
      result.push({ time: data[i].time, value: 50 });
    } else if (i === period) {
      avgGain = avgGain / period;
      avgLoss = avgLoss / period;
      const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
      result.push({ time: data[i].time, value: 100 - (100 / (1 + rs)) });
    } else {
      avgGain = (avgGain * (period - 1) + gain) / period;
      avgLoss = (avgLoss * (period - 1) + loss) / period;
      const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
      result.push({ time: data[i].time, value: 100 - (100 / (1 + rs)) });
    }
  }
  return result;
}

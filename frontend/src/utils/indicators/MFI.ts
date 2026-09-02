import { LineData } from 'lightweight-charts';

/**
 * 计算 MFI (资金流量指数)
 * @param data K 线数据数组，包含 high, low, close, volume 字段
 * @param period 周期，默认 14
 * @returns MFI 数据数组
 */
export function calculateMFI(data: any[], period: number = 14): LineData[] {
  const result: LineData[] = [];

  for (let i = 0; i < data.length; i++) {
    const tp = (data[i].high + data[i].low + data[i].close) / 3;

    if (i === 0) {
      result.push({ time: data[i].time, value: 50 });
      continue;
    }

    if (i < period) {
      result.push({ time: data[i].time, value: 50 });
    } else {
      let posSum = 0, negSum = 0;
      for (let j = 0; j < period; j++) {
        const idx = i - period + j + 1;
        const prevTp = (data[idx - 1].high + data[idx - 1].low + data[idx - 1].close) / 3;
        const currTp = (data[idx].high + data[idx].low + data[idx].close) / 3;
        const mf = currTp * (data[idx].volume || 0);
        if (currTp > prevTp) posSum += mf;
        else negSum += mf;
      }

      const mr = negSum === 0 ? 100 : posSum / negSum;
      const mfi = 100 - (100 / (1 + mr));
      result.push({ time: data[i].time, value: mfi });
    }
  }

  return result;
}

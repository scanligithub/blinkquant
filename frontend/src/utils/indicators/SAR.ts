import { LineData } from 'lightweight-charts';

/**
 * 计算抛物线转向 (SAR)
 * @param data K 线数据数组
 * @param afStep 加速因子步长，默认 0.02
 * @param afMax 加速因子最大值，默认 0.2
 * @returns SAR 数据数组
 */
export function calculateSAR(data: any[], afStep: number = 0.02, afMax: number = 0.2): LineData[] {
  const result: LineData[] = [];
  let sar = 0;
  let ep = data[0]?.high || 0;
  let af = afStep;
  let isUp = true;

  for (let i = 0; i < data.length; i++) {
    const high = data[i].high;
    const low = data[i].low;
    if (i < 2) {
      result.push({ time: data[i].time, value: low });
      if (i === 1) {
        sar = data[0].low;
        ep = data[0].high;
        isUp = data[1].close > data[1].open;
        if (!isUp) {
          sar = data[0].high;
          ep = data[0].low;
        }
      }
      continue;
    }

    const prevSar = result[i - 1]?.value || sar;
    let newSar = prevSar + af * (ep - prevSar);

    if (isUp) {
      if (low < newSar) newSar = ep;
      if (high > ep) {
        ep = high;
        af = Math.min(af + afStep, afMax);
      }
      if (low < result[i - 2]?.value || 0) {
        isUp = false;
        ep = low;
        af = afStep;
        newSar = result[i - 1]?.value || high;
      }
    } else {
      if (high > newSar) newSar = ep;
      if (low < ep) {
        ep = low;
        af = Math.min(af + afStep, afMax);
      }
      if (high > result[i - 2]?.value || 0) {
        isUp = true;
        ep = high;
        af = afStep;
        newSar = result[i - 1]?.value || low;
      }
    }

    result.push({ time: data[i].time, value: newSar });
  }
  return result;
}

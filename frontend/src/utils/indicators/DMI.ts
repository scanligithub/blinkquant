import { LineData } from 'lightweight-charts';

/**
 * 计算 DMI/ADX (趋向指标)
 * @param data K 线数据数组
 * @param period 周期，默认 14
 * @returns 包含 +DI、-DI、ADX 的对象
 */
export function calculateDMI(data: any[], period: number = 14): { pdi: LineData[], mdi: LineData[], adx: LineData[] } {
  const result = {
    pdi: [] as LineData[],
    mdi: [] as LineData[],
    adx: [] as LineData[],
  };

  let plusDM = 0, minusDM = 0, trSum = 0;

  for (let i = 0; i < data.length; i++) {
    if (i === 0) {
      result.pdi.push({ time: data[i].time, value: 0 });
      result.mdi.push({ time: data[i].time, value: 0 });
      result.adx.push({ time: data[i].time, value: 0 });
      continue;
    }

    const high = data[i].high;
    const low = data[i].low;
    const prevHigh = data[i - 1].high;
    const prevLow = data[i - 1].low;

    const plusDMVal = high - prevHigh;
    const minusDMVal = prevLow - low;

    let tr = Math.max(high - low, Math.abs(high - data[i - 1].close), Math.abs(low - data[i - 1].close));

    if (i < period) {
      plusDM += plusDMVal > 0 && plusDMVal > minusDMVal ? plusDMVal : 0;
      minusDM += minusDMVal > 0 && minusDMVal > plusDMVal ? minusDMVal : 0;
      trSum += tr;
      result.pdi.push({ time: data[i].time, value: 0 });
      result.mdi.push({ time: data[i].time, value: 0 });
      result.adx.push({ time: data[i].time, value: 0 });
    } else if (i === period) {
      plusDM = plusDM / period;
      minusDM = minusDM / period;
      trSum = trSum / period;

      const plusDI = trSum === 0 ? 0 : (plusDM / trSum) * 100;
      const minusDI = trSum === 0 ? 0 : (minusDM / trSum) * 100;
      const dx = (Math.abs(plusDI - minusDI) / (plusDI + minusDI)) * 100;

      result.pdi.push({ time: data[i].time, value: plusDI });
      result.mdi.push({ time: data[i].time, value: minusDI });
      result.adx.push({ time: data[i].time, value: dx });
    } else {
      plusDM = (plusDM * (period - 1) + (plusDMVal > 0 && plusDMVal > minusDMVal ? plusDMVal : 0)) / period;
      minusDM = (minusDM * (period - 1) + (minusDMVal > 0 && minusDMVal > plusDMVal ? minusDMVal : 0)) / period;
      trSum = (trSum * (period - 1) + tr) / period;

      const plusDI = trSum === 0 ? 0 : (plusDM / trSum) * 100;
      const minusDI = trSum === 0 ? 0 : (minusDM / trSum) * 100;
      const dx = (Math.abs(plusDI - minusDI) / (plusDI + minusDI)) * 100;
      const adx = (result.adx[i - 1]?.value || 0 * (period - 1) + dx) / period;

      result.pdi.push({ time: data[i].time, value: plusDI });
      result.mdi.push({ time: data[i].time, value: minusDI });
      result.adx.push({ time: data[i].time, value: adx });
    }
  }

  return result;
}

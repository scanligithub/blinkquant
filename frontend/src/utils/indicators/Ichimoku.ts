import { LineData } from 'lightweight-charts';

/**
 * 计算 Ichimoku 云图指标
 * @param data K 线数据数组
 * @param tenkanPeriod 转换线周期，默认 9
 * @param kijunPeriod 基准线周期，默认 26
 * @param senkouBPeriod 先行线 B 周期，默认 52
 * @returns 包含转换线、基准线、先行线 A/B 的对象
 */
export function calculateIchimoku(data: any[], tenkanPeriod: number = 9, kijunPeriod: number = 26, senkouBPeriod: number = 52) {
  const result = {
    tenkan: [] as LineData[],
    kijun: [] as LineData[],
    spanA: [] as LineData[],
    spanB: [] as LineData[],
  };

  const getMidPoint = (start: number, end: number) => {
    let max = -Infinity, min = Infinity;
    for (let i = start; i <= end; i++) {
      if (data[i].high > max) max = data[i].high;
      if (data[i].low < min) min = data[i].low;
    }
    return (max + min) / 2;
  };

  for (let i = 0; i < data.length; i++) {
    // 转换线 (Tenkan-sen)
    if (i >= tenkanPeriod - 1) {
      result.tenkan.push({ time: data[i].time, value: getMidPoint(i - tenkanPeriod + 1, i) });
    } else {
      result.tenkan.push({ time: data[i].time, value: 0 });
    }

    // 基准线 (Kijun-sen)
    if (i >= kijunPeriod - 1) {
      const kijunVal = getMidPoint(i - kijunPeriod + 1, i);
      result.kijun.push({ time: data[i].time, value: kijunVal });
    } else {
      result.kijun.push({ time: data[i].time, value: 0 });
    }

    // 先行线 A (Span A)
    if (i >= kijunPeriod - 1) {
      const spanAVal = (result.tenkan[i - tenkanPeriod + 1]?.value || 0 + result.kijun[i - kijunPeriod + 1]?.value || 0) / 2;
      result.spanA.push({ time: data[i].time, value: spanAVal });
    } else {
      result.spanA.push({ time: data[i].time, value: 0 });
    }

    // 先行线 B (Span B)
    if (i >= senkouBPeriod - 1) {
      const spanBVal = getMidPoint(i - senkouBPeriod + 1, i);
      result.spanB.push({ time: data[i].time, value: spanBVal });
    } else {
      result.spanB.push({ time: data[i].time, value: 0 });
    }
  }

  return result;
}

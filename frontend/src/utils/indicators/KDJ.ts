import { LineData } from 'lightweight-charts';

/**
 * 计算 KDJ 随机指标
 * @param data K 线数据数组
 * @param n 周期，默认 9
 * @param m1 K 值平滑因子，默认 3
 * @param m2 D 值平滑因子，默认 3
 * @returns 包含 K、D、J 三条线的对象
 */
export function calculateKDJ(data: any[], n: number = 9, m1: number = 3, m2: number = 3) {
  const result = {
    k: [] as LineData[],
    d: [] as LineData[],
    j: [] as LineData[],
  };

  let kVal = 50, dVal = 50;

  for (let i = 0; i < data.length; i++) {
    if (i < n - 1) {
      result.k.push({ time: data[i].time, value: 50 });
      result.d.push({ time: data[i].time, value: 50 });
      result.j.push({ time: data[i].time, value: 50 });
      continue;
    }

    let highest = -Infinity, lowest = Infinity;
    for (let j = 0; j < n; j++) {
      if (data[i - j].high > highest) highest = data[i - j].high;
      if (data[i - j].low < lowest) lowest = data[i - j].low;
    }

    const rsv = (highest - lowest) === 0 ? 50 : ((data[i].close - lowest) / (highest - lowest)) * 100;
    kVal = (m1 * rsv + (n - m1) * kVal) / n;
    dVal = (m2 * kVal + (n - m2) * dVal) / n;
    const jVal = 3 * kVal - 2 * dVal;

    result.k.push({ time: data[i].time, value: kVal });
    result.d.push({ time: data[i].time, value: dVal });
    result.j.push({ time: data[i].time, value: jVal });
  }

  return result;
}

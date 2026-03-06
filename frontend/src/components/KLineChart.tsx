'use client';
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts';
import { useEffect, useRef, useState, useCallback } from 'react';

// 内联 Settings 图标组件
const SettingsIcon = ({ className }: { className?: string }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="24"
    height="24"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
  >
    <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);

function formatVolume(volume: number): string {
  if (volume >= 100000000) {
    return (volume / 100000000).toFixed(2) + '亿';
  } else if (volume >= 10000) {
    return (volume / 10000).toFixed(2) + '万';
  }
  return volume.toString();
}

// 【新增】：智能资金单位格式化器
function formatMoney(value: number): string {
  if (!value || isNaN(value)) return '0.00';
  const absVal = Math.abs(value);

  // 假设原始数据单位是 "元"
  if (absVal >= 100000000) {
    return (value / 100000000).toFixed(2) + '亿';
  } else if (absVal >= 10000) {
    return (value / 10000).toFixed(2) + '万';
  }
  return value.toFixed(2);
}

function calculateMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) continue;
    let sum = 0;
    for (let j = 0; j < period; j++) sum += data[i - j].close;
    result.push({ time: data[i].time, value: sum / period });
  }
  return result;
}

function calculateEMAForData(data: any[], period: number): LineData[] {
  const closes = data.map(item => item.close);
  const result: number[] = [];
  const multiplier = 2 / (period + 1);
  for (let i = 0; i < period - 1 && i < closes.length; i++) result.push(0);
  if (closes.length < period) return data.map(d => ({ time: d.time, value: 0 }));

  let sum = 0;
  for (let i = 0; i < period; i++) sum += closes[i];
  result.push(sum / period);

  for (let i = period; i < closes.length; i++) {
    const ema = (closes[i] - result[i - 1]) * multiplier + result[i - 1];
    result.push(ema);
  }

  return data.map((d, i) => ({ time: d.time, value: result[i] || 0 }));
}

// 计算布林带
function calculateBoll(data: any[], period: number = 20, multiplier: number = 2) {
  const closes = data.map(item => item.close);
  const upper: LineData[] = [];
  const middle: LineData[] = [];
  const lower: LineData[] = [];

  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      upper.push({ time: data[i].time, value: 0 });
      middle.push({ time: data[i].time, value: 0 });
      lower.push({ time: data[i].time, value: 0 });
      continue;
    }

    const slice = closes.slice(i - period + 1, i + 1);
    const ma = slice.reduce((a, b) => a + b, 0) / period;
    const std = Math.sqrt(slice.reduce((a, b) => a + Math.pow(b - ma, 2), 0) / period);

    middle.push({ time: data[i].time, value: ma });
    upper.push({ time: data[i].time, value: ma + multiplier * std });
    lower.push({ time: data[i].time, value: ma - multiplier * std });
  }

  return { upper, middle, lower };
  }
  
  // 计算加权移动平均线 (WMA)
  function calculateWMA(data: any[], period: number): LineData[] {
    const closes = data.map(item => item.close);
    const result: LineData[] = [];
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push({ time: data[i].time, value: 0 });
        continue;
      }
      let sum = 0;
      let weightSum = 0;
      for (let j = 0; j < period; j++) {
        const weight = j + 1;
        sum += closes[i - j] * weight;
        weightSum += weight;
      }
      result.push({ time: data[i].time, value: sum / weightSum });
    }
    return result;
  }
  
  // 计算平滑移动平均线 (SMMA)
  function calculateSMMA(data: any[], period: number): LineData[] {
    const closes = data.map(item => item.close);
    const result: LineData[] = [];
    let smma = 0;
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push({ time: data[i].time, value: 0 });
        continue;
      }
      if (i === period - 1) {
        smma = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
      } else {
        smma = (smma * (period - 1) + closes[i]) / period;
      }
      result.push({ time: data[i].time, value: smma });
    }
    return result;
  }
  
  // 计算成交量加权平均价 (VWAP)
  function calculateVWAP(data: any[]): LineData[] {
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
  
  // 计算抛物线转向 (SAR)
  function calculateSAR(data: any[], afStep: number = 0.02, afMax: number = 0.2): LineData[] {
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
  
  // 计算 Ichimoku 云图
  function calculateIchimoku(data: any[], tenkanPeriod: number = 9, kijunPeriod: number = 26, senkouBPeriod: number = 52) {
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
  
      // 先行线 A (Span A) - 向前平移 26 周期
      if (i >= kijunPeriod - 1) {
        const spanAVal = (result.tenkan[i - tenkanPeriod + 1]?.value || 0 + result.kijun[i - kijunPeriod + 1]?.value || 0) / 2;
        result.spanA.push({ time: data[i].time, value: spanAVal });
      } else {
        result.spanA.push({ time: data[i].time, value: 0 });
      }
  
      // 先行线 B (Span B) - 向前平移 26 周期
      if (i >= senkouBPeriod - 1) {
        const spanBVal = getMidPoint(i - senkouBPeriod + 1, i);
        result.spanB.push({ time: data[i].time, value: spanBVal });
      } else {
        result.spanB.push({ time: data[i].time, value: 0 });
      }
    }
  
    return result;
  }
  
  // 计算相对强弱指数 (RSI)
  function calculateRSI(data: any[], period: number = 14): LineData[] {
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
  
  // 计算随机指标 (KDJ)
  function calculateKDJ(data: any[], n: number = 9, m1: number = 3, m2: number = 3) {
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
  
  // 计算威廉指标 (WR)
  function calculateWR(data: any[], period: number = 14): LineData[] {
    const result: LineData[] = [];
  
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push({ time: data[i].time, value: -100 });
        continue;
      }
  
      let highest = -Infinity, lowest = Infinity;
      for (let j = 0; j < period; j++) {
        if (data[i - j].high > highest) highest = data[i - j].high;
        if (data[i - j].low < lowest) lowest = data[i - j].low;
      }
  
      const wr = (highest - data[i].close) / (highest - lowest) * -100;
      result.push({ time: data[i].time, value: wr });
    }
  
    return result;
  }
  
  // 计算累积量 (OBV)
  function calculateOBV(data: any[]): LineData[] {
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
  
  // 计算 CCI 指标
  function calculateCCI(data: any[], period: number = 20): LineData[] {
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
  
  // 计算 BBI 指标
  function calculateBBI(data: any[]): LineData[] {
    const result: LineData[] = [];
    const periods = [3, 6, 12, 24];
  
    for (let i = 0; i < data.length; i++) {
      if (i < 23) {
        result.push({ time: data[i].time, value: 0 });
        continue;
      }
  
      let sum = 0;
      for (const p of periods) {
        let maSum = 0;
        for (let j = 0; j < p; j++) {
          maSum += data[i - j].close;
        }
        sum += maSum / p;
      }
      result.push({ time: data[i].time, value: sum / periods.length });
    }
  
    return result;
  }
  
  // 计算 DMI/ADX 指标
  function calculateDMI(data: any[], period: number = 14): { pdi: LineData[], mdi: LineData[], adx: LineData[] } {
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
  
  // 计算 MFI 指标 (资金流量指数)
  function calculateMFI(data: any[], period: number = 14): LineData[] {
    const result: LineData[] = [];
    const positiveFlow: number[] = [];
    const negativeFlow: number[] = [];
  
    for (let i = 0; i < data.length; i++) {
      const tp = (data[i].high + data[i].low + data[i].close) / 3;
  
      if (i === 0) {
        result.push({ time: data[i].time, value: 50 });
        continue;
      }
  
      const rawMoneyFlow = tp * (data[i].volume || 0);
      const direction = tp - (data[i - 1].high + data[i - 1].low + data[i - 1].close) / 3;
  
      if (i < period) {
        if (direction > 0) positiveFlow.push(rawMoneyFlow);
        else negativeFlow.push(rawMoneyFlow);
        result.push({ time: data[i].time, value: 50 });
      } else {
        let posSum = 0, negSum = 0;
        for (let j = 0; j < period; j++) {
          const idx = i - period + j + 1;
          const d = tp - (data[idx - 1].high + data[idx - 1].low + data[idx - 1].close) / 3;
          const mf = (data[idx].high + data[idx].low + data[idx].close) / 3 * (data[idx].volume || 0);
          if (d > 0) posSum += mf;
          else negSum += mf;
        }
  
        const mr = negSum === 0 ? 100 : posSum / negSum;
        const mfi = 100 - (100 / (1 + mr));
        result.push({ time: data[i].time, value: mfi });
      }
    }
  
    return result;
  }
  
  function calculateVolumeMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) continue;
    let sum = 0;
    for (let j = 0; j < period; j++) sum += data[i - j].value;
    result.push({ time: data[i].time, value: sum / period });
  }
  return result;
}

function calculateMACD(data: any[], fastPeriod: number = 12, slowPeriod: number = 26, signalPeriod: number = 9) {
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

// 【新增】：计算 N 日累计资金净流向
function calculateRollingSum(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) continue;
    let sum = 0;
    for (let j = 0; j < period; j++) sum += data[i - j].value;
    result.push({ time: data[i].time, value: sum });
  }
  return result;
}

// 主图指标配置
const MAIN_INDICATORS = {
  MA: {
    label: 'MA (简单移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateMA,
  },
  EMA: {
    label: 'EMA (指数移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateEMAForData,
  },
  WMA: {
    label: 'WMA (加权移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateWMA,
  },
  SMMA: {
    label: 'SMMA (平滑移动平均)',
    periods: [5, 10, 20, 30, 60, 120],
    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'],
    calculate: calculateSMMA,
  },
  BOLL: {
    label: 'BOLL (布林带)',
    periods: [20],
    colors: { upper: '#FF6B6B', middle: '#45B7D1', lower: '#96CEB4' },
    calculate: calculateBoll,
  },
  VWAP: {
    label: 'VWAP (成交量加权平均价)',
    periods: [],
    colors: ['#45B7D1'],
    calculate: calculateVWAP,
  },
  SAR: {
    label: 'SAR (抛物线转向)',
    periods: [],
    colors: ['#FF6B6B'],
    calculate: calculateSAR,
  },
  ICHIMOKU: {
    label: 'Ichimoku (云图)',
    periods: [],
    colors: { tenkan: '#FF6B6B', kijun: '#45B7D1', spanA: '#4ECDC4', spanB: '#96CEB4' },
    calculate: calculateIchimoku,
  },
  BBI: {
    label: 'BBI (多空分界)',
    periods: [],
    colors: ['#45B7D1'],
    calculate: calculateBBI,
  },
  NONE: {
    label: '无指标',
    periods: [],
    colors: [],
    calculate: () => [],
  },
};

// 副图指标配置
const SUB_INDICATORS = {
  MACD: { label: 'MACD (平滑异同移动平均)' },
  MF: { label: '资金流 (主力净流入)' },
  RSI: { label: 'RSI (相对强弱指数)' },
  KDJ: { label: 'KDJ (随机指标)' },
  WR: { label: 'WR (威廉指标)' },
  OBV: { label: 'OBV (累积量)' },
  CCI: { label: 'CCI (顺势指标)' },
  DMI: { label: 'DMI/ADX (趋向指标)' },
  MFI: { label: 'MFI (资金流量指数)' },
  VOL: { label: 'VOL (成交量)' },
};

interface KLineChartProps {
  data: any;
  code: string;
  subChartType?: string;
  onSubChartTypeChange?: (type: string) => void;
  mainChartType?: string;
  onMainChartTypeChange?: (type: string) => void;
}

export default function KLineChart({ 
  data, 
  code, 
  subChartType = 'MACD',
  onSubChartTypeChange,
  mainChartType = 'MA',
  onMainChartTypeChange
}: KLineChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesMap = useRef<{ 
    [key: string]: any 
  }>({});

  const [tooltip, setTooltip] = useState<any>(null);
  const [macdIndicators, setMacdIndicators] = useState<any>(null);
  const [mfIndicators, setMfIndicators] = useState<any>(null);
  const [maIndicators, setMaIndicators] = useState<any>(null);
  const [volumeMaIndicators, setVolumeMaIndicators] = useState<any>(null);
  const [rsiIndicators, setRsiIndicators] = useState<any>(null);
  const [kdjIndicators, setKdjIndicators] = useState<any>(null);
  const [wrIndicators, setWrIndicators] = useState<any>(null);
  const [obvIndicators, setObvIndicators] = useState<any>(null);
  const [cciIndicators, setCciIndicators] = useState<any>(null);
  const [dmiIndicators, setDmiIndicators] = useState<any>(null);
  const [mfiIndicators, setMfiIndicators] = useState<any>(null);
  const [priceExtremes, setPriceExtremes] = useState<any>(null);
  const [volumeMax, setVolumeMax] = useState<any>(null);
  const [extremesPositions, setExtremesPositions] = useState<any>(null);
  const [isMobile, setIsMobile] = useState(false);
  
  // 使用 ref 跟踪 mainChartType 的最新值（用于回调函数中）
  const mainChartTypeRef = useRef(mainChartType);
  useEffect(() => {
    mainChartTypeRef.current = mainChartType;
  }, [mainChartType]);
  
  // 配置菜单状态
  const [mainMenuOpen, setMainMenuOpen] = useState(false);
  const [subMenuOpen, setSubMenuOpen] = useState(false);
  const mainMenuRef = useRef<HTMLDivElement>(null);
  const subMenuRef = useRef<HTMLDivElement>(null);

  // 点击外部关闭菜单
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (mainMenuRef.current && !mainMenuRef.current.contains(event.target as Node)) {
        setMainMenuOpen(false);
      }
      if (subMenuRef.current && !subMenuRef.current.contains(event.target as Node)) {
        setSubMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // 主图指标切换处理
  const handleMainSelect = useCallback((type: string) => {
    setMainMenuOpen(false);
    if (onMainChartTypeChange) {
      onMainChartTypeChange(type);
    }
  }, [onMainChartTypeChange]);

  // 副图指标切换处理
  const handleSubSelect = useCallback((type: string) => {
    setSubMenuOpen(false);
    if (onSubChartTypeChange) {
      onSubChartTypeChange(type);
    }
  }, [onSubChartTypeChange]);

  useEffect(() => {
    if (!chartContainerRef.current || !data) return;

    // 检测是否为移动端
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768);
    };
    checkMobile();

    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#ffffff' }, textColor: '#334155' },
      grid: { vertLines: { color: '#f1f5f9' }, horzLines: { color: '#f1f5f9' } },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      rightPriceScale: { borderColor: '#e2e8f0' },
      timeScale: { borderColor: '#e2e8f0', timeVisible: true, secondsVisible: false },
      localization: { locale: 'zh-CN', dateFormat: 'yyyy-MM-dd' },
    });

    const candlestickSeries = chart.addCandlestickSeries({
      priceScaleId: 'right', upColor: '#ef4444', downColor: '#22c55e', borderVisible: false, wickUpColor: '#ef4444', wickDownColor: '#22c55e',
    });

    const maPeriods = [5, 10, 20, 30, 60, 120];
    const maColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'];
    const maSeries = maPeriods.map((period, index) =>
      chart.addLineSeries({ priceScaleId: 'right', color: maColors[index], lineWidth: 1, lastValueVisible: false, priceLineVisible: false })
    );

    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: 'volume', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false,
    });

    const volumeMAPeriods = [5, 10, 20];
    const volumeMAColors = ['#FF6B6B', '#4ECDC4', '#45B7D1'];
    const volumeMASeries = volumeMAPeriods.map((period, index) =>
      chart.addLineSeries({ priceScaleId: 'volume', color: volumeMAColors[index], lineWidth: 1, lastValueVisible: false, priceLineVisible: false })
    );

    // MACD 系列
    const macdLine = chart.addLineSeries({ priceScaleId: 'subchart', color: '#ef4444', lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    const signalLine = chart.addLineSeries({ priceScaleId: 'subchart', color: '#22c55e', lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    const histogramSeries = chart.addHistogramSeries({ priceScaleId: 'subchart', baseLineColor: '#e2e8f0', lastValueVisible: false, priceLineVisible: false });

    // 资金流柱状图
    const mfSeries = chart.addHistogramSeries({
      priceScaleId: 'subchart',
      baseLineColor: '#e2e8f0',
      baseLineVisible: true,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => formatMoney(price)
      },
      lastValueVisible: false, priceLineVisible: false,
    });

    // 资金流 20 日累计趋势折线 (黄色)
    const mfTrendLine = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#f59e0b',
      lineWidth: 2,
      lastValueVisible: false, priceLineVisible: false,
    });

    seriesMap.current = { 
      macdLine, 
      signalLine, 
      histogramSeries, 
      mfSeries, 
      mfTrendLine,
      maSeries,
      volumeMASeries,
    };

    chart.priceScale('right').applyOptions({ scaleMargins: { top: 0.05, bottom: 0.40 } });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.60, bottom: 0.25 } });
    chart.priceScale('subchart').applyOptions({ scaleMargins: { top: 0.75, bottom: 0.0 } });

    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const volumeData = data.map((item: any) => ({ time: item.time, value: item.volume, color: item.close >= item.open ? '#ef4444' : '#22c55e' }));

    // 主图极值和量能最大值将在可视范围内动态计算
    setPriceExtremes(null);
    setVolumeMax(null);

    const mfData = data.map((item: any) => {
      const val = item.main_net || 0;
      return { time: item.time, value: val, color: val >= 0 ? 'rgba(239, 68, 68, 0.85)' : 'rgba(34, 197, 94, 0.85)' };
    });

    candlestickSeries.setData(formattedData);
    volumeSeries.setData(volumeData);
    maPeriods.forEach((period, index) => maSeries[index].setData(calculateMA(formattedData, period)));

    // 计算极值在图表中的位置（基于可视范围）
    const calculatePositions = () => {
      if (!chartContainerRef.current) return;

      const positions: any = {};
      const timeScale = chart.timeScale();
      const visibleRange = timeScale.getVisibleRange();

      if (visibleRange) {
        const { from, to } = visibleRange;

        // 主图最高价位置（基于可视范围）
        let maxPrice = -Infinity;
        let maxPriceTime: any = null;

        // 主图最低价位置（基于可视范围）
        let minPrice = Infinity;
        let minPriceTime: any = null;

        // 量能最大值位置（基于可视范围）
        let maxVol = 0;
        let maxVolTime: any = null;

        formattedData.forEach(d => {
          const timeValue = typeof d.time === 'number' ? d.time : (d.time as any).businessDay || d.time;
          if (timeValue >= from && timeValue <= to) {
            if (d.high > maxPrice) {
              maxPrice = d.high;
              maxPriceTime = d.time;
            }
            if (d.low < minPrice) {
              minPrice = d.low;
              minPriceTime = d.time;
            }
          }
        });

        volumeData.forEach(d => {
          const timeValue = typeof d.time === 'number' ? d.time : (d.time as any).businessDay || d.time;
          if (timeValue >= from && timeValue <= to) {
            if (d.value > maxVol) {
              maxVol = d.value;
              maxVolTime = d.time;
            }
          }
        });

        // 主图最高价位置
        if (maxPriceTime !== null) {
          const x = timeScale.timeToCoordinate(maxPriceTime);
          const y = candlestickSeries.priceToCoordinate(maxPrice);
          if (x !== null && y !== null) {
            positions.maxPrice = { x, y, value: maxPrice, label: '高' };
          }
        }

        // 主图最低价位置
        if (minPriceTime !== null) {
          const x = timeScale.timeToCoordinate(minPriceTime);
          const y = candlestickSeries.priceToCoordinate(minPrice);
          if (x !== null && y !== null) {
            positions.minPrice = { x, y, value: minPrice, label: '低' };
          }
        }

        // 量能最大值位置
        if (maxVolTime !== null) {
          const x = timeScale.timeToCoordinate(maxVolTime);
          const y = volumeSeries.priceToCoordinate(maxVol);
          if (x !== null && y !== null) {
            positions.maxVolume = { x, y, value: maxVol, label: '最大' };
          }
        }
      }

      setExtremesPositions(positions);
    };

    // 初始计算位置
    setTimeout(calculatePositions, 100);

    // 监听图表尺寸变化和可见范围变化
    const handleVisibleRangeChange = () => {
      calculatePositions();
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
    chart.timeScale().subscribeVisibleTimeRangeChange(handleVisibleRangeChange);
    volumeMAPeriods.forEach((period, index) => volumeMASeries[index].setData(calculateVolumeMA(volumeData, period)));

    const macdData = calculateMACD(formattedData);
    macdLine.setData(macdData.macdLine);
    signalLine.setData(macdData.signalLine);
    histogramSeries.setData(macdData.histogram);

    mfSeries.setData(mfData);
    // 渲染 20 日累计趋势线
    mfTrendLine.setData(calculateRollingSum(mfData, 20));

    // 根据初始 subChartType 设置副图系列的可见性
    const isMacd = subChartType === 'MACD';
    macdLine.applyOptions({ visible: isMacd });
    signalLine.applyOptions({ visible: isMacd });
    histogramSeries.applyOptions({ visible: isMacd });
    mfSeries.applyOptions({ visible: !isMacd });
    mfTrendLine.applyOptions({ visible: !isMacd });

    // 根据屏幕宽度设置 Y 轴可见性
    const updateYAxisVisibility = () => {
      const mobile = window.innerWidth < 768;
      setIsMobile(mobile);
      if (chartRef.current) {
        chartRef.current.priceScale('right').applyOptions({ visible: !mobile });
        chartRef.current.priceScale('volume').applyOptions({ visible: !mobile });
        chartRef.current.priceScale('subchart').applyOptions({ visible: !mobile });
      }
    };
    
    // 初始设置 Y 轴可见性
    updateYAxisVisibility();
    
    // 监听窗口大小变化
    const handleResize = () => {
      updateYAxisVisibility();
      calculatePositions();
    };
    window.addEventListener('resize', handleResize);

    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time || !param.seriesData.size) { setTooltip(null); return; }
      const cdData = param.seriesData.get(candlestickSeries);
      const volData = param.seriesData.get(volumeSeries);

      if (cdData && typeof cdData === 'object' && 'open' in cdData) {
        const timeValue = typeof param.time === 'number' ? param.time : (param.time as any).businessDay || param.time;
        const date = new Date(timeValue * 1000);

        setTooltip({
          time: date.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' }),
          open: cdData.open as number, high: cdData.high as number, low: cdData.low as number, close: cdData.close as number,
          volume: (volData as any)?.value || 0,
          changePercent: (((cdData.close as number) - (cdData.open as number)) / (cdData.open as number)) * 100,
          position: param.point.x < (chartContainerRef.current?.clientWidth || 0) / 2 ? 'right' : 'left',
        });

        setMacdIndicators({
          dif: (param.seriesData.get(macdLine) as any)?.value || 0,
          dea: (param.seriesData.get(signalLine) as any)?.value || 0,
          macd: (param.seriesData.get(histogramSeries) as any)?.value || 0,
        });

        setMfIndicators({
          net: (param.seriesData.get(mfSeries) as any)?.value || 0,
          trend: (param.seriesData.get(mfTrendLine) as any)?.value || 0,
        });

        // 获取主图指标最新值（根据当前选择的指标类型显示对应名称）
        const mainChartValues: any = {};
        maSeries.forEach((series, index) => {
          const value = (param.seriesData.get(series) as any)?.value;
          if (value !== undefined && value !== 0) {
            // 根据当前主图指标类型生成对应的标签（使用 ref 获取最新值）
            let label: string;
            const currentType = mainChartTypeRef.current || 'MA';
            if (currentType === 'MA' || currentType === 'EMA') {
              label = `${currentType}${maPeriods[index]}`;
            } else if (currentType === 'BOLL') {
              const bollLabels = ['中轨', '上轨', '下轨'];
              label = bollLabels[index] || `BOLL${index}`;
            } else {
              label = `Line${index}`;
            }
            mainChartValues[label] = { value, color: maColors[index] };
          }
        });
        setMaIndicators(mainChartValues);

        // 获取量能 MA 最新值
        const volumeMaValues: any = {};
        volumeMASeries.forEach((series, index) => {
          const value = (param.seriesData.get(series) as any)?.value;
          if (value !== undefined) {
            volumeMaValues[`VMA${volumeMAPeriods[index]}`] = { value, color: volumeMAColors[index] };
          }
        });
        setVolumeMaIndicators(volumeMaValues);
      }
    });

    chart.timeScale().fitContent();
    chartRef.current = chart;

    const resizeObserver = new ResizeObserver(() => {
      if (chartContainerRef.current) chart.applyOptions({ width: chartContainerRef.current.clientWidth, height: chartContainerRef.current.clientHeight });
    });
    resizeObserver.observe(chartContainerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      window.removeEventListener('resize', handleResize);
    };
  }, [data]);

  // 响应 subChartType 变化
  useEffect(() => {
    if (!seriesMap.current.mfSeries) return;

    const isMacd = subChartType === 'MACD';
    seriesMap.current.macdLine.applyOptions({ visible: isMacd });
    seriesMap.current.signalLine.applyOptions({ visible: isMacd });
    seriesMap.current.histogramSeries.applyOptions({ visible: isMacd });

    seriesMap.current.mfSeries.applyOptions({ visible: !isMacd });
    seriesMap.current.mfTrendLine.applyOptions({ visible: !isMacd });
  }, [subChartType]);

  // 响应 mainChartType 变化 - 重新计算并设置主图指标
  useEffect(() => {
    if (!seriesMap.current.maSeries || !chartRef.current) return;

    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const maSeries = seriesMap.current.maSeries;

    // 清除旧指标数据
    maSeries.forEach((series: any) => series.setData([]));

    if (mainChartType === 'MA' || mainChartType === 'EMA') {
      const config = MAIN_INDICATORS[mainChartType];
      config.periods.forEach((period, index) => {
        if (maSeries[index]) {
          const dataPoints = config.calculate(formattedData, period);
          maSeries[index].setData(dataPoints);
        }
      });
    } else if (mainChartType === 'BOLL') {
      // BOLL 需要特殊处理，这里暂时只显示中轨
      const bollData = calculateBoll(formattedData, 20);
      if (maSeries[0]) maSeries[0].setData(bollData.middle);
      if (maSeries[1]) maSeries[1].setData(bollData.upper);
      if (maSeries[2]) maSeries[2].setData(bollData.lower);
      // 隐藏多余的线
      for (let i = 3; i < maSeries.length; i++) {
        maSeries[i].setData([]);
      }
    } else if (mainChartType === 'NONE') {
      maSeries.forEach((series: any) => series.setData([]));
    }
  }, [mainChartType, data]);

  return (
    <div className="w-full h-full relative bg-white">
      <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />

      {/* 主图指标配置按钮和数据显示 - 左上角 */}
      <div ref={mainMenuRef} className="absolute top-2 left-2 md:left-4 z-20 flex items-center gap-2">
        <div className="relative">
          <button
            onClick={() => setMainMenuOpen(!mainMenuOpen)}
            className="p-1.5 rounded-md hover:bg-slate-100 transition-colors bg-white/90 backdrop-blur shadow-sm"
          >
            <SettingsIcon className="w-4 h-4 text-slate-500" />
          </button>
      
          {mainMenuOpen && (
            <div className="absolute left-0 top-full mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 min-w-[100px]">
              {Object.entries(MAIN_INDICATORS).map(([key, config]) => (
                <button
                  key={key}
                  onClick={() => handleMainSelect(key)}
                  className={`w-full px-3 py-1.5 text-left text-sm hover:bg-slate-50 transition-colors ${
                    mainChartType === key ? 'text-blue-600 font-medium bg-blue-50' : 'text-slate-600'
                  }`}
                >
                  {config.label}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
      
      {/* 主图 MA 指标动态显示 - 紧跟配置按钮 */}
      <div className="absolute top-2 left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none" style={{ marginLeft: '48px' }}>
        {maIndicators && Object.entries(maIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{item.value.toFixed(2)}</span>
          </span>
        ))}
      </div>
      
      {/* 副图指标配置按钮 - 副图区域左上角 */}
      <div ref={subMenuRef} className="absolute top-[calc(75%-1.5rem)] left-2 md:left-4 z-20">
        <div className="relative">
          <button
            onClick={() => setSubMenuOpen(!subMenuOpen)}
            className="p-1.5 rounded-md hover:bg-slate-100 transition-colors bg-white/90 backdrop-blur shadow-sm"
          >
            <SettingsIcon className="w-4 h-4 text-slate-500" />
          </button>
      
          {subMenuOpen && (
            <div className="absolute left-0 top-full mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 min-w-[100px]">
              {Object.entries(SUB_INDICATORS).map(([key, config]) => (
                <button
                  key={key}
                  onClick={() => handleSubSelect(key)}
                  className={`w-full px-3 py-1.5 text-left text-sm hover:bg-slate-50 transition-colors ${
                    subChartType === key ? 'text-blue-600 font-medium bg-blue-50' : 'text-slate-600'
                  }`}
                >
                  {config.label}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* 主图极值显示 */}
      {extremesPositions && (
        <>
          {extremesPositions.maxPrice && (
            <div
              className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-red-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
              style={{
                left: `${extremesPositions.maxPrice.x}px`,
                top: `${extremesPositions.maxPrice.y - 30}px`,
                transform: 'translateX(-50%)'
              }}
            >
              <div className="flex items-center gap-1">
                <span className="text-red-600 font-bold">{extremesPositions.maxPrice.label}</span>
                <span className="font-mono text-red-600">{extremesPositions.maxPrice.value.toFixed(2)}</span>
              </div>
              <div className="absolute left-1/2 -bottom-2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-red-200 transform -translate-x-1/2"></div>
            </div>
          )}
          {extremesPositions.minPrice && (
            <div
              className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-green-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
              style={{
                left: `${extremesPositions.minPrice.x}px`,
                top: `${extremesPositions.minPrice.y + 10}px`,
                transform: 'translateX(-50%)'
              }}
            >
              <div className="flex items-center gap-1">
                <span className="text-green-600 font-bold">{extremesPositions.minPrice.label}</span>
                <span className="font-mono text-green-600">{extremesPositions.minPrice.value.toFixed(2)}</span>
              </div>
              <div className="absolute left-1/2 -top-2 w-0 h-0 border-l-4 border-r-4 border-b-4 border-transparent border-b-green-200 transform -translate-x-1/2"></div>
            </div>
          )}
        </>
      )}


      {/* 量能极值显示 */}
      {extremesPositions && extremesPositions.maxVolume && (
        <div
          className="absolute z-10 bg-white/95 backdrop-blur px-2 py-1 rounded border border-slate-200 shadow-sm text-[9px] md:text-xs pointer-events-none"
          style={{
            left: `${extremesPositions.maxVolume.x}px`,
            top: `${extremesPositions.maxVolume.y - 30}px`,
            transform: 'translateX(-50%)'
          }}
        >
          <div className="flex items-center gap-1">
            <span className="text-slate-600 font-bold">{extremesPositions.maxVolume.label}</span>
            <span className="font-mono text-slate-900">{formatVolume(extremesPositions.maxVolume.value)}</span>
          </div>
          <div className="absolute left-1/2 -bottom-2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-slate-300 transform -translate-x-1/2"></div>
        </div>
      )}

      {/* 量能 MA 指标动态显示 */}
      <div className="absolute top-[calc(60%-1.2rem)] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none">
        {volumeMaIndicators && Object.entries(volumeMaIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{formatVolume(item.value)}</span>
          </span>
        ))}
      </div>

      {/* MACD / 资金流 指标动态显示 - 紧跟副图配置按钮 */}
      <div className="absolute top-[calc(75%-1.2rem)] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none" style={{ marginLeft: '48px' }}>
        {subChartType === 'MACD' && macdIndicators && (
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">DIF: <span className="font-mono text-[#ef4444]">{macdIndicators.dif.toFixed(2)}</span></span>
            <span className="text-slate-500">DEA: <span className="font-mono text-[#22c55e]">{macdIndicators.dea.toFixed(2)}</span></span>
            <span className="text-slate-500">MACD: <span className={`font-mono ${macdIndicators.macd >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>{macdIndicators.macd.toFixed(2)}</span></span>
          </div>
        )}
        {subChartType === 'MF' && mfIndicators && (
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">
              单日主力: <span className={`font-mono ${mfIndicators.net >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>
                {formatMoney(mfIndicators.net)}
              </span>
            </span>
            <span className="text-slate-500">
              20 日趋势: <span className="font-mono text-[#f59e0b]">
                {formatMoney(mfIndicators.trend)}
              </span>
            </span>
          </div>
        )}
      </div>

      {/* 主图悬浮框 */}
      {tooltip && (
        <div className={`absolute top-8 z-20 bg-white/95 backdrop-blur px-3 py-2 rounded-lg border border-slate-200 shadow-lg text-[10px] md:text-xs pointer-events-none ${
          tooltip.position === 'left' ? 'left-4' : 'right-12'
        }`}>
          <div className="font-bold text-slate-900 mb-1">{tooltip.time}</div>
          <div className="space-y-0.5">
            <div className="flex justify-between gap-4"><span className="text-slate-500">开盘:</span><span className="font-mono text-slate-900">{tooltip.open.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">收盘:</span><span className="font-mono text-slate-900">{tooltip.close.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">成交量:</span><span className="font-mono text-slate-900">{formatVolume(tooltip.volume)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">涨幅:</span><span className={`font-mono ${tooltip.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>{tooltip.changePercent >= 0 ? '+' : ''}{tooltip.changePercent.toFixed(2)}%</span></div>
          </div>
        </div>
      )}
    </div>
  );
}

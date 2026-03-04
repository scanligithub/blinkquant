'use client';
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';

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

function calculateEMA(data: number[], period: number): number[] {
  const result: number[] = [];
  const multiplier = 2 / (period + 1);
  for (let i = 0; i < period - 1 && i < data.length; i++) result.push(0);
  if (data.length < period) return result;
  
  let sum = 0;
  for (let i = 0; i < period; i++) sum += data[i];
  result.push(sum / period);
  
  for (let i = period; i < data.length; i++) {
    const ema = (data[i] - result[i - 1]) * multiplier + result[i - 1];
    result.push(ema);
  }
  return result;
}

function calculateMACD(data: any[], fastPeriod: number = 12, slowPeriod: number = 26, signalPeriod: number = 9) {
  const closes = data.map(item => item.close);
  const fastEMA = calculateEMA(closes, fastPeriod);
  const slowEMA = calculateEMA(closes, slowPeriod);
  
  const macdLine: LineData[] = [];
  for (let i = slowPeriod - 1; i < data.length; i++) {
    const dif = fastEMA[i] - slowEMA[i];
    if (!isNaN(dif) && isFinite(dif)) macdLine.push({ time: data[i].time, value: dif });
  }
  
  const macdValues = macdLine.map(item => item.value);
  const signalEMA = calculateEMA(macdValues, signalPeriod);
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

export default function KLineChart({ data, code, subChartType = 'MACD' }: { data: any, code: string, subChartType?: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesMap = useRef<{ [key: string]: any }>({});
  
  const [tooltip, setTooltip] = useState<any>(null);
  const [macdIndicators, setMacdIndicators] = useState<any>(null);
  const [mfIndicators, setMfIndicators] = useState<any>(null);
  const [maIndicators, setMaIndicators] = useState<any>(null);
  const [volumeMaIndicators, setVolumeMaIndicators] = useState<any>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !data) return;

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
        // 【修改】：使用智能格式化器，而不是写死的除以1亿
        formatter: (price: number) => formatMoney(price)
      },
      lastValueVisible: false, priceLineVisible: false,
    });

    // 【新增】：资金流 20日累计趋势折线 (黄色)
    const mfTrendLine = chart.addLineSeries({
      priceScaleId: 'subchart',
      color: '#f59e0b', // 琥珀黄
      lineWidth: 2,
      lastValueVisible: false, priceLineVisible: false,
    });

    seriesMap.current = { macdLine, signalLine, histogramSeries, mfSeries, mfTrendLine };

    chart.priceScale('right').applyOptions({ scaleMargins: { top: 0.05, bottom: 0.40 } });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.60, bottom: 0.25 } });
    chart.priceScale('subchart').applyOptions({ scaleMargins: { top: 0.75, bottom: 0.0 } }); 

    const formattedData = data.map((item: any) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
    const volumeData = data.map((item: any) => ({ time: item.time, value: item.volume, color: item.close >= item.open ? '#ef4444' : '#22c55e' }));
    
    const mfData = data.map((item: any) => {
      const val = item.main_net || 0;
      return { time: item.time, value: val, color: val >= 0 ? 'rgba(239, 68, 68, 0.85)' : 'rgba(34, 197, 94, 0.85)' };
    });

    candlestickSeries.setData(formattedData);
    volumeSeries.setData(volumeData);
    maPeriods.forEach((period, index) => maSeries[index].setData(calculateMA(formattedData, period)));
    volumeMAPeriods.forEach((period, index) => volumeMASeries[index].setData(calculateVolumeMA(volumeData, period)));

    const macdData = calculateMACD(formattedData);
    macdLine.setData(macdData.macdLine);
    signalLine.setData(macdData.signalLine);
    histogramSeries.setData(macdData.histogram);
    
    mfSeries.setData(mfData);
    // 渲染 20 日累计趋势线
    mfTrendLine.setData(calculateRollingSum(mfData, 20));

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

        // 获取主图MA最新值
        const maValues: any = {};
        maSeries.forEach((series, index) => {
          const value = (param.seriesData.get(series) as any)?.value;
          if (value !== undefined) {
            maValues[`MA${maPeriods[index]}`] = { value, color: maColors[index] };
          }
        });
        setMaIndicators(maValues);

        // 获取量能MA最新值
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

    return () => { resizeObserver.disconnect(); chart.remove(); };
  }, [data]);

  useEffect(() => {
    if (!seriesMap.current.mfSeries) return;
    
    const isMacd = subChartType === 'MACD';
    seriesMap.current.macdLine.applyOptions({ visible: isMacd });
    seriesMap.current.signalLine.applyOptions({ visible: isMacd });
    seriesMap.current.histogramSeries.applyOptions({ visible: isMacd });
    
    seriesMap.current.mfSeries.applyOptions({ visible: !isMacd });
    seriesMap.current.mfTrendLine.applyOptions({ visible: !isMacd }); // 同步控制趋势线显隐
  }, [subChartType]);

  return (
    <div className="w-full h-full relative bg-white">
      <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />
      
      {/* 主图 MA 指标动态显示 */}
      <div className="absolute top-2 left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none">
        {maIndicators && Object.entries(maIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{item.value.toFixed(2)}</span>
          </span>
        ))}
      </div>

      {/* 量能 MA 指标动态显示 */}
      <div className="absolute top-[40%] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none">
        {volumeMaIndicators && Object.entries(volumeMaIndicators).map(([key, item]: [string, any]) => (
          <span key={key} className="text-slate-500 mr-2">
            {key}: <span className="font-mono" style={{ color: item.color }}>{formatVolume(item.value)}</span>
          </span>
        ))}
      </div>

      {/* MACD / 资金流 指标动态显示 */}
      <div className="absolute top-[75%] left-2 md:left-4 z-10 bg-transparent px-2 py-1 rounded-lg text-[9px] md:text-xs pointer-events-none">
        {subChartType === 'MACD' && macdIndicators && (
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">DIF: <span className="font-mono text-[#ef4444]">{macdIndicators.dif.toFixed(2)}</span></span>
            <span className="text-slate-500">DEA: <span className="font-mono text-[#22c55e]">{macdIndicators.dea.toFixed(2)}</span></span>
            <span className="text-slate-500">MACD: <span className={`font-mono ${macdIndicators.macd >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>{macdIndicators.macd.toFixed(2)}</span></span>
          </div>
        )}
        {/* 【修改】：调用 formatMoney 处理悬浮图例中的数值 */}
        {subChartType === 'MF' && mfIndicators && (
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">
              单日主力: <span className={`font-mono ${mfIndicators.net >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>
                {formatMoney(mfIndicators.net)}
              </span>
            </span>
            <span className="text-slate-500">
              20日趋势: <span className="font-mono text-[#f59e0b]">
                {formatMoney(mfIndicators.trend)}
              </span>
            </span>
          </div>
        )}
      </div>
      
      {/* 主图悬浮框 */}
      {tooltip && (
        <div className={`absolute top-2 z-20 bg-white/95 backdrop-blur px-3 py-2 rounded-lg border border-slate-200 shadow-lg text-[10px] md:text-xs pointer-events-none ${
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

'use client';
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';

// 计算价格移动平均线
function calculateMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      continue; 
    }
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close;
    }
    result.push({
      time: data[i].time,
      value: sum / period,
    });
  }
  return result;
}

// 计算量能移动平均线
function calculateVolumeMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      continue; 
    }
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += data[i - j].value;
    }
    result.push({
      time: data[i].time,
      value: sum / period,
    });
  }
  return result;
}

// 计算 EMA
function calculateEMA(data: number[], period: number): number[] {
  const result: number[] = [];
  const multiplier = 2 / (period + 1);
  
  for (let i = 0; i < period - 1 && i < data.length; i++) {
    result.push(0);
  }
  
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

// 计算 MACD
function calculateMACD(data: any[], fastPeriod: number = 12, slowPeriod: number = 26, signalPeriod: number = 9) {
  const closes = data.map(item => item.close);
  const fastEMA = calculateEMA(closes, fastPeriod);
  const slowEMA = calculateEMA(closes, slowPeriod);
  
  const macdLine: LineData[] = [];
  for (let i = slowPeriod - 1; i < data.length; i++) {
    const dif = fastEMA[i] - slowEMA[i];
    if (!isNaN(dif) && isFinite(dif)) {
      macdLine.push({ time: data[i].time, value: dif });
    }
  }
  
  const macdValues = macdLine.map(item => item.value);
  const signalEMA = calculateEMA(macdValues, signalPeriod);
  const signalLine: LineData[] = [];
  for (let i = signalPeriod - 1; i < macdLine.length; i++) {
    const dea = signalEMA[i];
    if (!isNaN(dea) && isFinite(dea)) {
      signalLine.push({ time: macdLine[i].time, value: dea });
    }
  }
  
  const histogram: any[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    const macdValue = macdLine[i + signalPeriod - 1]?.value || 0;
    const signalValue = signalLine[i]?.value || 0;
    const diff = macdValue - signalValue;
    
    // 【修复1】MACD柱状图必须允许为负数，去掉原代码中的 Math.abs(diff)
    // 通常国产软件习惯将柱子乘2，这里我们遵循 (DIF - DEA) * 2 的习惯放大展示，也可不乘
    histogram.push({
      time: signalLine[i].time,
      value: diff * 2, 
      color: diff >= 0 ? '#ef4444' : '#22c55e', 
    });
  }
  
  return { macdLine, signalLine, histogram };
}

export default function KLineChart({ data, code }: { data: any, code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  
  const [tooltip, setTooltip] = useState<any>(null);
  const [maIndicators, setMaIndicators] = useState<any>(null);
  const [vmaIndicators, setVmaIndicators] = useState<any>(null);
  const [macdIndicators, setMacdIndicators] = useState<any>(null);

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

    // 【排版修复 A】主图表 (蜡烛图) 占图表上方 65% 的区域 (留出底部35%)
    chart.priceScale('right').applyOptions({
      scaleMargins: { top: 0.05, bottom: 0.35 },
    });

    const candlestickSeries = chart.addCandlestickSeries({
      priceScaleId: 'right',
      upColor: '#ef4444', downColor: '#22c55e', borderVisible: false,
      wickUpColor: '#ef4444', wickDownColor: '#22c55e',
    });

    // 添加MA均线
    const maPeriods = [5, 10, 20, 30, 60, 120];
    const maColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'];
    const maSeries = maPeriods.map((period, index) => 
      chart.addLineSeries({
        priceScaleId: 'right',
        color: maColors[index],
        lineWidth: 1,
        lastValueVisible: false,
        priceLineVisible: false,
      })
    );

    // 【排版修复 B】量能图 独立赋予一个 'volume' ID，占据 65% ~ 80% 区间
    const volumeScaleId = 'volume';
    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: volumeScaleId,
      priceFormat: { type: 'volume' },
      lastValueVisible: false, priceLineVisible: false,
    });
    chart.priceScale(volumeScaleId).applyOptions({
      scaleMargins: { top: 0.65, bottom: 0.20 },
    });

    const volumeMAPeriods = [5, 10, 20, 30, 60];
    const volumeMAColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'];
    const volumeMASeries = volumeMAPeriods.map((period, index) => 
      chart.addLineSeries({
        priceScaleId: volumeScaleId, // 绑定到量能区域
        color: volumeMAColors[index],
        lineWidth: 1,
        lastValueVisible: false, priceLineVisible: false,
      })
    );

    // 【排版修复 C】MACD图 占据 80% ~ 100% 区间
    const macdPriceScaleId = 'macd';
    chart.priceScale(macdPriceScaleId).applyOptions({
      scaleMargins: { top: 0.80, bottom: 0 },
    });

    const macdLine = chart.addLineSeries({
      priceScaleId: macdPriceScaleId, color: '#ef4444', lineWidth: 1,
      lastValueVisible: false, priceLineVisible: false,
    });

    const signalLine = chart.addLineSeries({
      priceScaleId: macdPriceScaleId, color: '#22c55e', lineWidth: 1,
      lastValueVisible: false, priceLineVisible: false,
    });

    // 【修复2】去掉了 priceFormat: { type: 'volume' }，让 MACD 柱子正常显示正负值
    const histogramSeries = chart.addHistogramSeries({
      priceScaleId: macdPriceScaleId,
      lastValueVisible: false, priceLineVisible: false,
    });

    let formattedData: any[] = [];
    let volumeData: any[] = [];

    if (Array.isArray(data)) {
        formattedData = data.map(item => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close }));
        volumeData = data.map(item => ({ time: item.time, value: item.volume, color: item.close >= item.open ? '#ef4444' : '#22c55e' }));
    } else if (data && typeof data === 'object' && Array.isArray(data.date)) {
        for (let i = 0; i < data.date.length; i++) {
            formattedData.push({ time: data.date[i], open: data.open[i], high: data.high[i], low: data.low[i], close: data.close[i] });
            volumeData.push({ time: data.date[i], value: data.volume[i], color: data.close[i] >= data.open[i] ? '#ef4444' : '#22c55e' });
        }
    }

    candlestickSeries.setData(formattedData);
    volumeSeries.setData(volumeData);
    
    maPeriods.forEach((period, index) => maSeries[index].setData(calculateMA(formattedData, period)));
    volumeMAPeriods.forEach((period, index) => volumeMASeries[index].setData(calculateVolumeMA(volumeData, period)));

    const macdData = calculateMACD(formattedData);
    macdLine.setData(macdData.macdLine);
    signalLine.setData(macdData.signalLine);
    histogramSeries.setData(macdData.histogram);

    // 标记点逻辑保持不变
    const updateMarkers = () => {
      if (formattedData.length === 0 || volumeData.length === 0) return;
      const visibleRange = chart.timeScale().getVisibleRange();
      if (!visibleRange) return;

      const visibleData = formattedData.filter(item => {
        const time = typeof item.time === 'number' ? item.time : (item.time as any).timestamp;
        return time >= visibleRange.from && time <= visibleRange.to;
      });

      if (visibleData.length === 0) return;

      let maxPrice = -Infinity, minPrice = Infinity;
      let maxTime: Time | null = null, minTime: Time | null = null;
      visibleData.forEach(item => {
        if (item.high > maxPrice) { maxPrice = item.high; maxTime = item.time; }
        if (item.low < minPrice) { minPrice = item.low; minTime = item.time; }
      });

      const markers = [];
      if (maxTime) markers.push({ time: maxTime, position: 'aboveBar' as const, color: '#ef4444', shape: 'arrowDown' as const, text: `最高 ${maxPrice.toFixed(2)}` });
      if (minTime) markers.push({ time: minTime, position: 'belowBar' as const, color: '#22c55e', shape: 'arrowUp' as const, text: `最低 ${minPrice.toFixed(2)}` });
      candlestickSeries.setMarkers(markers);

      let maxVolume = -Infinity;
      let maxVolumeTime: Time | null = null;
      volumeData.forEach(item => {
        const time = typeof item.time === 'number' ? item.time : (item.time as any).timestamp;
        if (time >= visibleRange.from && time <= visibleRange.to) {
          if (item.value > maxVolume) { maxVolume = item.value; maxVolumeTime = item.time; }
        }
      });

      const volumeMarkers = [];
      if (maxVolumeTime) volumeMarkers.push({ time: maxVolumeTime, position: 'aboveBar' as const, color: '#f59e0b', shape: 'arrowDown' as const, text: `最大量 ${(maxVolume / 10000).toFixed(2)}万` });
      volumeSeries.setMarkers(volumeMarkers);
    };

    updateMarkers();
    chart.timeScale().subscribeVisibleLogicalRangeChange(updateMarkers);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    const handleResize = () => {
      if (chartContainerRef.current) chart.applyOptions({ width: chartContainerRef.current.clientWidth, height: chartContainerRef.current.clientHeight });
    };
    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);

    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time || !param.seriesData.size) {
        setTooltip(null);
        return;
      }

      const candlestickData = param.seriesData.get(candlestickSeries);
      const volData = param.seriesData.get(volumeSeries);

      if (candlestickData && typeof candlestickData === 'object' && 'open' in candlestickData) {
        const timeValue = typeof param.time === 'number' ? param.time : (param.time as any).businessDay || param.time;
        const date = new Date(timeValue * 1000);
        
        const open = candlestickData.open as number;
        const close = candlestickData.close as number;

        setTooltip({
          time: date.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' }),
          open: open, high: candlestickData.high as number, low: candlestickData.low as number, close: close,
          volume: volData && 'value' in volData ? volData.value as number : 0,
          changePercent: ((close - open) / open) * 100,
          position: param.point.x < (chartContainerRef.current?.clientWidth || 0) / 2 ? 'right' : 'left',
        });
        
        setMaIndicators({
          ma5: (param.seriesData.get(maSeries[0]) as any)?.value || 0,
          ma10: (param.seriesData.get(maSeries[1]) as any)?.value || 0,
          ma20: (param.seriesData.get(maSeries[2]) as any)?.value || 0,
          ma30: (param.seriesData.get(maSeries[3]) as any)?.value || 0,
          ma60: (param.seriesData.get(maSeries[4]) as any)?.value || 0,
          ma120: (param.seriesData.get(maSeries[5]) as any)?.value || 0,
        });
        
        setVmaIndicators({
          vma5: (param.seriesData.get(volumeMASeries[0]) as any)?.value || 0,
          vma10: (param.seriesData.get(volumeMASeries[1]) as any)?.value || 0,
          vma20: (param.seriesData.get(volumeMASeries[2]) as any)?.value || 0,
          vma30: (param.seriesData.get(volumeMASeries[3]) as any)?.value || 0,
          vma60: (param.seriesData.get(volumeMASeries[4]) as any)?.value || 0,
        });

        setMacdIndicators({
          dif: (param.seriesData.get(macdLine) as any)?.value || 0,
          dea: (param.seriesData.get(signalLine) as any)?.value || 0,
          macd: (param.seriesData.get(histogramSeries) as any)?.value || 0,
        });
      }
    });

    return () => { resizeObserver.disconnect(); chart.remove(); };
  }, [data]);

  return (
    <div className="w-full h-full relative bg-white">
      <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />
      
      {/* 价格MA */}
      {maIndicators && (
        <div className="absolute top-2 md:top-4 left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 rounded-lg text-[9px] md:text-xs">
          <div className="flex items-center gap-1.5 md:gap-3 flex-wrap">
            <span className="text-slate-500">MA5: <span className="font-mono text-[#FF6B6B]">{maIndicators.ma5.toFixed(2)}</span></span>
            <span className="text-slate-500">MA10: <span className="font-mono text-[#4ECDC4]">{maIndicators.ma10.toFixed(2)}</span></span>
            <span className="text-slate-500">MA20: <span className="font-mono text-[#45B7D1]">{maIndicators.ma20.toFixed(2)}</span></span>
            <span className="text-slate-500">MA30: <span className="font-mono text-[#96CEB4]">{maIndicators.ma30.toFixed(2)}</span></span>
            <span className="text-slate-500">MA60: <span className="font-mono text-[#FFEAA7]">{maIndicators.ma60.toFixed(2)}</span></span>
            <span className="text-slate-500">MA120: <span className="font-mono text-[#DDA0DD]">{maIndicators.ma120.toFixed(2)}</span></span>
          </div>
        </div>
      )}
      
      {/* 【同步修改CSS：量能顶端在 65%】 */}
      {vmaIndicators && (
        <div className="absolute top-[65%] left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 rounded-lg text-[9px] md:text-xs">
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">VMA5: <span className="font-mono text-[#FF6B6B]">{(vmaIndicators.vma5 / 10000).toFixed(2)}万</span></span>
            <span className="text-slate-500">VMA10: <span className="font-mono text-[#4ECDC4]">{(vmaIndicators.vma10 / 10000).toFixed(2)}万</span></span>
            <span className="text-slate-500">VMA20: <span className="font-mono text-[#45B7D1]">{(vmaIndicators.vma20 / 10000).toFixed(2)}万</span></span>
          </div>
        </div>
      )}
      
      {/* 【同步修改CSS：MACD顶端在 80%】 */}
      {macdIndicators && (
        <div className="absolute top-[80%] left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 rounded-lg text-[9px] md:text-xs">
          <div className="flex items-center gap-1.5 md:gap-3">
            <span className="text-slate-500">DIF: <span className="font-mono text-[#ef4444]">{macdIndicators.dif.toFixed(2)}</span></span>
            <span className="text-slate-500">DEA: <span className="font-mono text-[#22c55e]">{macdIndicators.dea.toFixed(2)}</span></span>
            <span className="text-slate-500">MACD: <span className={`font-mono ${macdIndicators.macd >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>{macdIndicators.macd.toFixed(2)}</span></span>
          </div>
        </div>
      )}
      
      {tooltip && (
        <div className={`absolute top-2 md:top-4 z-20 bg-white/95 backdrop-blur px-2 md:px-4 py-2 md:py-3 rounded-lg border border-slate-200 shadow-lg text-[10px] md:text-sm ${
          tooltip.position === 'left' ? 'left-2 md:left-4' : 'right-2 md:right-4'
        }`}>
          <div className="font-bold text-slate-900 mb-1">{tooltip.time}</div>
          <div className="space-y-0.5">
            <div className="flex justify-between gap-4"><span className="text-slate-500">开盘:</span><span className="font-mono text-slate-900">{tooltip.open.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">最高:</span><span className="font-mono text-red-600">{tooltip.high.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">最低:</span><span className="font-mono text-green-600">{tooltip.low.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">收盘:</span><span className="font-mono text-slate-900">{tooltip.close.toFixed(2)}</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">涨幅:</span><span className={`font-mono ${tooltip.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>{tooltip.changePercent >= 0 ? '+' : ''}{tooltip.changePercent.toFixed(2)}%</span></div>
            <div className="flex justify-between gap-4"><span className="text-slate-500">成交量:</span><span className="font-mono text-slate-900">{(tooltip.volume / 10000).toFixed(2)}万</span></div>
          </div>
        </div>
      )}
    </div>
  );
}

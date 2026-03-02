'use client';
import { createChart, ColorType, IChartApi, PriceScaleMode, LineData, Time } from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';

// 计算价格移动平均线
function calculateMA(data: any[], period: number): LineData[] {
  const result: LineData[] = [];
  for (let i = 0; i < data.length; i++) {
    if (i < period - 1) {
      continue; // 数据不足，跳过
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
      continue; // 数据不足，跳过
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

// 计算 EMA (指数移动平均)
function calculateEMA(data: number[], period: number): number[] {
  const result: number[] = [];
  const multiplier = 2 / (period + 1);
  
  // 前面 period-1 个数据点没有 EMA 值，用 null 填充
  for (let i = 0; i < period - 1 && i < data.length; i++) {
    result.push(0);
  }
  
  if (data.length < period) {
    return result;
  }
  
  // 第一个 EMA 使用 SMA
  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += data[i];
  }
  result.push(sum / period);
  
  // 后续 EMA 使用公式
  for (let i = period; i < data.length; i++) {
    const ema = (data[i] - result[i - 1]) * multiplier + result[i - 1];
    result.push(ema);
  }
  
  return result;
}

// 计算 MACD
function calculateMACD(data: any[], fastPeriod: number = 12, slowPeriod: number = 26, signalPeriod: number = 9) {
  const closes = data.map(item => item.close);
  
  // 计算 EMA
  const fastEMA = calculateEMA(closes, fastPeriod);
  const slowEMA = calculateEMA(closes, slowPeriod);
  
  // 计算 MACD 线 (DIF) - 从 slowPeriod 开始
  const macdLine: LineData[] = [];
  for (let i = slowPeriod - 1; i < data.length; i++) {
    const dif = fastEMA[i] - slowEMA[i];
    if (!isNaN(dif) && isFinite(dif)) {
      macdLine.push({
        time: data[i].time,
        value: dif,
      });
    }
  }
  
  // 计算 DEA 线 (信号线) - 从 macdLine 的 signalPeriod 开始
  const macdValues = macdLine.map(item => item.value);
  const signalEMA = calculateEMA(macdValues, signalPeriod);
  const signalLine: LineData[] = [];
  for (let i = signalPeriod - 1; i < macdLine.length; i++) {
    const dea = signalEMA[i];
    if (!isNaN(dea) && isFinite(dea)) {
      signalLine.push({
        time: macdLine[i].time,
        value: dea,
      });
    }
  }
  
  // 计算 MACD 柱状图 (MACD - DEA)
  const histogram: any[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    const macdValue = macdLine[i + signalPeriod - 1]?.value || 0;
    const signalValue = signalLine[i]?.value || 0;
    const diff = macdValue - signalValue;
    histogram.push({
      time: signalLine[i].time,
      value: Math.abs(diff),
      color: diff >= 0 ? '#ef4444' : '#22c55e', // 上涨红色，下跌绿色
    });
  }
  
  return { macdLine, signalLine, histogram };
}

export default function KLineChart({ data, code }: { data: any, code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  
  // Tooltip状态 - 只显示基本OHLCV数据
  const [tooltip, setTooltip] = useState<{
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    changePercent: number;
    position: 'left' | 'right'; // tooltip显示位置
  } | null>(null);
  
  // 价格MA指标状态 - 显示在K线图区域左上角
  const [maIndicators, setMaIndicators] = useState<{
    ma5: number;
    ma10: number;
    ma20: number;
    ma30: number;
    ma60: number;
    ma120: number;
  } | null>(null);
  
  // 量能MA指标状态 - 显示在量能图区域左上角
  const [vmaIndicators, setVmaIndicators] = useState<{
    vma5: number;
    vma10: number;
    vma20: number;
    vma30: number;
    vma60: number;
  } | null>(null);

  // MACD指标状态 - 显示在MACD副图区域左上角
  const [macdIndicators, setMacdIndicators] = useState<{
    dif: number;
    dea: number;
    macd: number;
  } | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !data) return;

    // --- 浅色主题配置 ---
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#ffffff' }, // 白色背景
        textColor: '#334155' // 深灰文字
      },
      grid: {
        vertLines: { color: '#f1f5f9' }, // 极浅网格
        horzLines: { color: '#f1f5f9' }
      },
      // 【修复】：使用容器真实高宽，去掉死值 400
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      rightPriceScale: {
        borderColor: '#e2e8f0', // 边框颜色
   // (removed invalid scaleMargins from rightPriceScale)
      },
      timeScale: {
        borderColor: '#e2e8f0',
        timeVisible: true,
        secondsVisible: false,
      },
      localization: {
        locale: 'zh-CN',
        dateFormat: 'yyyy-MM-dd',
      },
    });
      // 为蜡烛图设置价格尺度，占据顶部 40% 的空间
      // 顶部留出 5% 空间给标记点显示
      chart.priceScale('right').applyOptions({
        scaleMargins: { top: 0.05, bottom: 0.55 },
      });

    const candlestickSeries = chart.addCandlestickSeries({
      priceScaleId: 'right',
      upColor: '#ef4444',
      downColor: '#22c55e',
      borderVisible: false,
      wickUpColor: '#ef4444',
      wickDownColor: '#22c55e',
    });

    // 添加MA均线系列
    const maPeriods = [5, 10, 20, 30, 60, 120];
    const maColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD'];
    const maSeries: any[] = [];

    maPeriods.forEach((period, index) => {
      const maLine = chart.addLineSeries({
        priceScaleId: 'right',
        color: maColors[index],
        lineWidth: 1,
        lastValueVisible: false, // 隐藏右侧坐标轴上的彩色数值标签
        priceLineVisible: false, // 隐藏当前价格水平虚线
      });
      maSeries.push(maLine);
    });

    // 新增量能柱（Histogram）系列，使用独立的 price scale
    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: '',
      priceFormat: {
        type: 'volume',
      },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    // 设置量能图的 scaleMargins，占据中间 15% 区域 (55%-70%)
    chart.priceScale('').applyOptions({
      scaleMargins: { top: 0.55, bottom: 0.25 },
    });

    // 添加量能MA均线系列
    const volumeMAPeriods = [5, 10, 20, 30, 60];
    const volumeMAColors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'];
    const volumeMASeries: any[] = [];

    volumeMAPeriods.forEach((period, index) => {
      const volumeMALine = chart.addLineSeries({
        priceScaleId: '',
        color: volumeMAColors[index],
        lineWidth: 1,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      volumeMASeries.push(volumeMALine);
    });

    // 添加 MACD 副图
    const macdPriceScaleId = 'macd';
    chart.priceScale(macdPriceScaleId).applyOptions({
      scaleMargins: { top: 0.75, bottom: 0 },
    });

    const macdLine = chart.addLineSeries({
      priceScaleId: macdPriceScaleId,
      color: '#ef4444',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });

    const signalLine = chart.addLineSeries({
      priceScaleId: macdPriceScaleId,
      color: '#22c55e',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
    });

    const histogramSeries = chart.addHistogramSeries({
      priceScaleId: macdPriceScaleId,
      priceFormat: { type: 'volume' },
      lastValueVisible: false,
      priceLineVisible: false,
    });

    let formattedData = [];
    let volumeData = [];

    if (Array.isArray(data)) {
        formattedData = data.map(item => ({
            time: item.time,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
        }));
        // 同时生成 volume 数据
        volumeData = data.map(item => ({
          time: item.time,
          value: item.volume,
          color: item.close >= item.open ? '#ef4444' : '#22c55e', // 上涨红色，下跌绿色
        }));
    } else if (data && typeof data === 'object' && Array.isArray(data.date)) {
        const len = data.date.length;
        for (let i = 0; i < len; i++) {
            formattedData.push({
                time: data.date[i],
                open: data.open[i],
                high: data.high[i],
                low: data.low[i],
                close: data.close[i],
            });
            volumeData.push({
              time: data.date[i],
              value: data.volume[i],
              color: data.close[i] >= data.open[i] ? '#ef4444' : '#22c55e', // 上涨红色，下跌绿色
            });
        }
    }

    candlestickSeries.setData(formattedData);
    
    // 计算并设置MA均线数据
    maPeriods.forEach((period, index) => {
      const maData = calculateMA(formattedData, period);
      maSeries[index].setData(maData);
    });

    // 设置量能柱数据
    volumeSeries.setData(volumeData);
    
    // 计算并设置量能MA均线数据
    volumeMAPeriods.forEach((period, index) => {
      const volumeMAData = calculateVolumeMA(volumeData, period);
      volumeMASeries[index].setData(volumeMAData);
    });

    // 计算并设置 MACD 数据
    const macdData = calculateMACD(formattedData);
    macdLine.setData(macdData.macdLine);
    signalLine.setData(macdData.signalLine);
    histogramSeries.setData(macdData.histogram);

    // 添加最高价、最低价和最大量能标记点（仅显示当前视图内的极值）
    const updateMarkers = () => {
      if (formattedData.length === 0 || volumeData.length === 0) return;

      // 获取当前可见的时间范围
      const visibleRange = chart.timeScale().getVisibleRange();
      if (!visibleRange) return;

      // 找出可见范围内的K线数据
      const visibleData = formattedData.filter(item => {
        const time = typeof item.time === 'number' ? item.time : (item.time as any).timestamp;
        return time >= visibleRange.from && time <= visibleRange.to;
      });

      if (visibleData.length === 0) return;

      // 找到可见范围内的最高价和最低价
      let maxPrice = -Infinity;
      let minPrice = Infinity;
      let maxTime: Time | null = null;
      let minTime: Time | null = null;

      visibleData.forEach(item => {
        if (item.high > maxPrice) {
          maxPrice = item.high;
          maxTime = item.time;
        }
        if (item.low < minPrice) {
          minPrice = item.low;
          minTime = item.time;
        }
      });

      // 创建价格标记点 - 使用箭头指向极值K线
      const markers = [];
      if (maxTime !== null) {
        markers.push({
          time: maxTime,
          position: 'aboveBar' as const,
          color: '#ef4444',
          shape: 'arrowDown' as const,
          text: `最高 ${maxPrice.toFixed(2)}`,
        });
      }
      if (minTime !== null) {
        markers.push({
          time: minTime,
          position: 'belowBar' as const,
          color: '#22c55e',
          shape: 'arrowUp' as const,
          text: `最低 ${minPrice.toFixed(2)}`,
        });
      }

      candlestickSeries.setMarkers(markers);

      // 找出可见范围内的最大量能值
      let maxVolume = -Infinity;
      let maxVolumeTime: Time | null = null;

      volumeData.forEach(item => {
        const time = typeof item.time === 'number' ? item.time : (item.time as any).timestamp;
        if (time >= visibleRange.from && time <= visibleRange.to) {
          if (item.value > maxVolume) {
            maxVolume = item.value;
            maxVolumeTime = item.time;
          }
        }
      });

      // 创建量能标记点
      const volumeMarkers = [];
      if (maxVolumeTime !== null) {
        volumeMarkers.push({
          time: maxVolumeTime,
          position: 'aboveBar' as const,
          color: '#f59e0b',
          shape: 'arrowDown' as const,
          text: `最大量 ${(maxVolume / 10000).toFixed(2)}万`,
        });
      }

      volumeSeries.setMarkers(volumeMarkers);
    };

    // 初始化标记点
    updateMarkers();

    // 监听可见范围变化，更新标记点
    chart.timeScale().subscribeVisibleLogicalRangeChange(updateMarkers);

    chart.timeScale().fitContent();
    chartRef.current = chart;

    // 【新增】：监听容器缩放事件，全屏时自动撑满屏幕
    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };
    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(chartContainerRef.current);

    // 订阅光标移动事件
    chart.subscribeCrosshairMove((param) => {
      if (!param.point || !param.time || !param.seriesData.size) {
        setTooltip(null);
        return;
      }

      const candlestickData = param.seriesData.get(candlestickSeries);
      const volumeData = param.seriesData.get(volumeSeries);

      if (candlestickData && typeof candlestickData === 'object' && 'open' in candlestickData) {
        const timeValue = typeof param.time === 'number' ? param.time : (param.time as any).businessDay || param.time;
        const date = new Date(timeValue * 1000);
        const timeStr = date.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' });
        
        const open = candlestickData.open as number;
        const close = candlestickData.close as number;
        const changePercent = ((close - open) / open) * 100;

        // 获取MA指标值
        const ma5Data = param.seriesData.get(maSeries[0]);
        const ma10Data = param.seriesData.get(maSeries[1]);
        const ma20Data = param.seriesData.get(maSeries[2]);
        const ma30Data = param.seriesData.get(maSeries[3]);
        const ma60Data = param.seriesData.get(maSeries[4]);
        const ma120Data = param.seriesData.get(maSeries[5]);

        const ma5 = ma5Data && typeof ma5Data === 'object' && 'value' in ma5Data ? ma5Data.value as number : 0;
        const ma10 = ma10Data && typeof ma10Data === 'object' && 'value' in ma10Data ? ma10Data.value as number : 0;
        const ma20 = ma20Data && typeof ma20Data === 'object' && 'value' in ma20Data ? ma20Data.value as number : 0;
        const ma30 = ma30Data && typeof ma30Data === 'object' && 'value' in ma30Data ? ma30Data.value as number : 0;
        const ma60 = ma60Data && typeof ma60Data === 'object' && 'value' in ma60Data ? ma60Data.value as number : 0;
        const ma120 = ma120Data && typeof ma120Data === 'object' && 'value' in ma120Data ? ma120Data.value as number : 0;

        // 获取量能MA指标值
        const vma5Data = param.seriesData.get(volumeMASeries[0]);
        const vma10Data = param.seriesData.get(volumeMASeries[1]);
        const vma20Data = param.seriesData.get(volumeMASeries[2]);
        const vma30Data = param.seriesData.get(volumeMASeries[3]);
        const vma60Data = param.seriesData.get(volumeMASeries[4]);

        const vma5 = vma5Data && typeof vma5Data === 'object' && 'value' in vma5Data ? vma5Data.value as number : 0;
        const vma10 = vma10Data && typeof vma10Data === 'object' && 'value' in vma10Data ? vma10Data.value as number : 0;
        const vma20 = vma20Data && typeof vma20Data === 'object' && 'value' in vma20Data ? vma20Data.value as number : 0;
        const vma30 = vma30Data && typeof vma30Data === 'object' && 'value' in vma30Data ? vma30Data.value as number : 0;
        const vma60 = vma60Data && typeof vma60Data === 'object' && 'value' in vma60Data ? vma60Data.value as number : 0;
        
        // 根据光标位置决定tooltip显示位置
        const chartWidth = chartContainerRef.current?.clientWidth || 0;
        const cursorX = param.point.x;
        const tooltipPosition = cursorX < chartWidth / 2 ? 'right' : 'left';
        
        // 设置tooltip - 只显示基本OHLCV数据
        setTooltip({
          time: timeStr,
          open: open,
          high: candlestickData.high as number,
          low: candlestickData.low as number,
          close: close,
          volume: volumeData && typeof volumeData === 'object' && 'value' in volumeData ? volumeData.value as number : 0,
          changePercent: changePercent,
          position: tooltipPosition,
        });
        
        // 设置价格MA指标 - 显示在K线图区域左上角
        setMaIndicators({
          ma5,
          ma10,
          ma20,
          ma30,
          ma60,
          ma120,
        });
        
        // 设置量能MA指标 - 显示在量能图区域左上角
        setVmaIndicators({
          vma5,
          vma10,
          vma20,
          vma30,
          vma60,
        });

        // 获取MACD指标值
        const macdLineData = param.seriesData.get(macdLine);
        const signalLineData = param.seriesData.get(signalLine);
        const histogramData = param.seriesData.get(histogramSeries);

        const dif = macdLineData && typeof macdLineData === 'object' && 'value' in macdLineData ? macdLineData.value as number : 0;
        const dea = signalLineData && typeof signalLineData === 'object' && 'value' in signalLineData ? signalLineData.value as number : 0;
        const macd = histogramData && typeof histogramData === 'object' && 'value' in histogramData ? histogramData.value as number : 0;

        // 设置MACD指标 - 显示在MACD副图区域左上角
        setMacdIndicators({
          dif,
          dea,
          macd,
        });
      }
    });

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [data]);

  return (
    // 【修复】：外层 div 必须占满父容器 (w-full h-full)，并去掉 padding (p-4)
    <div className="w-full h-full relative bg-white">
      {/* 这里的 inset-0 和 absolute 让绘图区死死钉在边框上 */}
      <div ref={chartContainerRef} className="absolute inset-0 w-full h-full" />
      
      {/* 价格MA指标 - 显示在K线图区域左上角，排成一排 */}
      {maIndicators && (
        <div className="absolute top-2 md:top-4 left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 md:py-2 rounded-lg border-none shadow-none text-[9px] md:text-xs">
          <div className="flex items-center gap-1.5 md:gap-3 flex-wrap">
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA5:</span>
              <span className="font-mono text-[#FF6B6B]">{maIndicators.ma5.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA10:</span>
              <span className="font-mono text-[#4ECDC4]">{maIndicators.ma10.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA20:</span>
              <span className="font-mono text-[#45B7D1]">{maIndicators.ma20.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA30:</span>
              <span className="font-mono text-[#96CEB4]">{maIndicators.ma30.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA60:</span>
              <span className="font-mono text-[#FFEAA7]">{maIndicators.ma60.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MA120:</span>
              <span className="font-mono text-[#DDA0DD]">{maIndicators.ma120.toFixed(2)}</span>
            </div>
          </div>
        </div>
      )}
      
      {/* 量能MA指标 - 显示在量能图区域左上角（图表高度的55%位置） */}
      {vmaIndicators && (
        <div className="absolute top-[55%] left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 md:py-2 rounded-lg border-none shadow-none text-[9px] md:text-xs">
          <div className="grid grid-cols-3 md:grid-cols-5 gap-x-1.5 md:gap-x-3 gap-y-0.5 md:gap-y-1">
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">VMA5:</span>
              <span className="font-mono text-[#FF6B6B]">{(vmaIndicators.vma5 / 10000).toFixed(2)}万</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">VMA10:</span>
              <span className="font-mono text-[#4ECDC4]">{(vmaIndicators.vma10 / 10000).toFixed(2)}万</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">VMA20:</span>
              <span className="font-mono text-[#45B7D1]">{(vmaIndicators.vma20 / 10000).toFixed(2)}万</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1 hidden md:flex">
              <span className="text-slate-500">VMA30:</span>
              <span className="font-mono text-[#96CEB4]">{(vmaIndicators.vma30 / 10000).toFixed(2)}万</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1 hidden md:flex">
              <span className="text-slate-500">VMA60:</span>
              <span className="font-mono text-[#FFEAA7]">{(vmaIndicators.vma60 / 10000).toFixed(2)}万</span>
            </div>
          </div>
        </div>
      )}
      
      {/* MACD指标 - 显示在MACD副图区域左上角（图表高度的75%位置） */}
      {macdIndicators && (
        <div className="absolute top-[75%] left-2 md:left-4 z-10 bg-transparent px-2 md:px-3 py-1 md:py-2 rounded-lg border-none shadow-none text-[9px] md:text-xs">
          <div className="flex items-center gap-1.5 md:gap-3">
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">DIF:</span>
              <span className="font-mono text-[#ef4444]">{macdIndicators.dif.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">DEA:</span>
              <span className="font-mono text-[#22c55e]">{macdIndicators.dea.toFixed(2)}</span>
            </div>
            <div className="flex items-center gap-0.5 md:gap-1">
              <span className="text-slate-500">MACD:</span>
              <span className={`font-mono ${macdIndicators.macd >= 0 ? 'text-[#ef4444]' : 'text-[#22c55e]'}`}>{macdIndicators.macd.toFixed(2)}</span>
            </div>
          </div>
        </div>
      )}
      
      {/* Tooltip - 只显示基本OHLCV数据，位置动态调整 */}
      {tooltip && (
        <div className={`absolute top-2 md:top-4 z-20 bg-white/95 backdrop-blur px-2 md:px-4 py-2 md:py-3 rounded-lg border border-slate-200 shadow-lg text-[10px] md:text-sm ${
          tooltip.position === 'left' ? 'left-2 md:left-4' : 'right-2 md:right-4'
        }`}>
          <div className="font-bold text-slate-900 mb-1 md:mb-2">{tooltip.time}</div>
          <div className="space-y-0.5 md:space-y-1">
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">开盘:</span>
              <span className="font-mono text-slate-900">{tooltip.open.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">最高:</span>
              <span className="font-mono text-red-600">{tooltip.high.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">最低:</span>
              <span className="font-mono text-green-600">{tooltip.low.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">收盘:</span>
              <span className="font-mono text-slate-900">{tooltip.close.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">涨跌幅:</span>
              <span className={`font-mono ${tooltip.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>
                {tooltip.changePercent >= 0 ? '+' : ''}{tooltip.changePercent.toFixed(2)}%
              </span>
            </div>
            <div className="flex justify-between gap-2 md:gap-4">
              <span className="text-slate-500">成交量:</span>
              <span className="font-mono text-slate-900">{(tooltip.volume / 10000).toFixed(2)}万</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

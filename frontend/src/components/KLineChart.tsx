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

export default function KLineChart({ data, code }: { data: any, code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [tooltip, setTooltip] = useState<{
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    changePercent: number;
    ma5: number;
    ma10: number;
    ma20: number;
    ma30: number;
    ma60: number;
    vma5: number;
    vma10: number;
    vma20: number;
    vma30: number;
    vma60: number;
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
      width: chartContainerRef.current.clientWidth,
      height: 400,
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
      // 为蜡烛图设置价格尺度，占据顶部 80% 的空间
      chart.priceScale('right').applyOptions({
        scaleMargins: { top: 0, bottom: 0.2 },
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
        title: `MA${period}`,
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
    // 设置量能图的 scaleMargins，将其压在底部 20% 区域
    chart.priceScale('').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
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
        title: `VMA${period}`,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      volumeMASeries.push(volumeMALine);
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

    chart.timeScale().fitContent();
    chartRef.current = chart;

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

        const ma5 = ma5Data && typeof ma5Data === 'object' && 'value' in ma5Data ? ma5Data.value as number : 0;
        const ma10 = ma10Data && typeof ma10Data === 'object' && 'value' in ma10Data ? ma10Data.value as number : 0;
        const ma20 = ma20Data && typeof ma20Data === 'object' && 'value' in ma20Data ? ma20Data.value as number : 0;
        const ma30 = ma30Data && typeof ma30Data === 'object' && 'value' in ma30Data ? ma30Data.value as number : 0;
        const ma60 = ma60Data && typeof ma60Data === 'object' && 'value' in ma60Data ? ma60Data.value as number : 0;

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
        
        setTooltip({
          time: timeStr,
          open: open,
          high: candlestickData.high as number,
          low: candlestickData.low as number,
          close: close,
          volume: volumeData && typeof volumeData === 'object' && 'value' in volumeData ? volumeData.value as number : 0,
          changePercent: changePercent,
          ma5: ma5,
          ma10: ma10,
          ma20: ma20,
          ma30: ma30,
          ma60: ma60,
          vma5: vma5,
          vma10: vma10,
          vma20: vma20,
          vma30: vma30,
          vma60: vma60,
        });
      }
    });

    return () => chart.remove();
  }, [data]);

  return (
    <div className="relative bg-white rounded-xl p-4 border border-slate-200 shadow-none">
      <div ref={chartContainerRef} />
      {tooltip && (
        <div className="absolute top-4 left-4 z-20 bg-white/95 backdrop-blur px-4 py-3 rounded-lg border border-slate-200 shadow-lg text-sm">
          <div className="font-bold text-slate-900 mb-2">{tooltip.time}</div>
          <div className="space-y-1">
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">开盘:</span>
              <span className="font-mono text-slate-900">{tooltip.open.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">最高:</span>
              <span className="font-mono text-red-600">{tooltip.high.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">最低:</span>
              <span className="font-mono text-green-600">{tooltip.low.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">收盘:</span>
              <span className="font-mono text-slate-900">{tooltip.close.toFixed(2)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">涨跌幅:</span>
              <span className={`font-mono ${tooltip.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>
                {tooltip.changePercent >= 0 ? '+' : ''}{tooltip.changePercent.toFixed(2)}%
              </span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-slate-500">成交量:</span>
              <span className="font-mono text-slate-900">{(tooltip.volume / 10000).toFixed(2)}万</span>
            </div>
            <div className="border-t border-slate-200 pt-2 mt-2">
              <div className="text-slate-500 text-xs mb-1">技术指标</div>
              <div className="grid grid-cols-3 gap-x-4 gap-y-1">
                <div className="flex justify-between">
                  <span className="text-slate-500">MA5:</span>
                  <span className="font-mono text-yellow-600">{tooltip.ma5.toFixed(2)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">MA10:</span>
                  <span className="font-mono text-purple-600">{tooltip.ma10.toFixed(2)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">MA20:</span>
                  <span className="font-mono text-blue-600">{tooltip.ma20.toFixed(2)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">MA30:</span>
                  <span className="font-mono text-cyan-600">{tooltip.ma30.toFixed(2)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">MA60:</span>
                  <span className="font-mono text-orange-600">{tooltip.ma60.toFixed(2)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">VMA5:</span>
                  <span className="font-mono text-red-400">{(tooltip.vma5 / 10000).toFixed(2)}万</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">VMA10:</span>
                  <span className="font-mono text-teal-400">{(tooltip.vma10 / 10000).toFixed(2)}万</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">VMA20:</span>
                  <span className="font-mono text-sky-400">{(tooltip.vma20 / 10000).toFixed(2)}万</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">VMA30:</span>
                  <span className="font-mono text-emerald-400">{(tooltip.vma30 / 10000).toFixed(2)}万</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">VMA60:</span>
                  <span className="font-mono text-amber-400">{(tooltip.vma60 / 10000).toFixed(2)}万</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

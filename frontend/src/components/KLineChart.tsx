'use client';
import { createChart, ColorType, IChartApi } from 'lightweight-charts';
import { useEffect, useRef } from 'react';

export default function KLineChart({ data, code }: { data: any, code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

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
      },
    });
      // 为蜡烛图设置价格尺度，占据顶部 70% 的空间
      chart.priceScale('right').applyOptions({
        scaleMargins: { top: 0, bottom: 0.3 },
      });
      // 为量能柱创建独立的价格尺度，占据底部 30% 的空间
      chart.priceScale('volume').applyOptions({
        scaleMargins: { top: 0.7, bottom: 0 },
      });

    const candlestickSeries = chart.addCandlestickSeries({
      priceScaleId: 'right',
      upColor: '#ef4444',
      downColor: '#22c55e',
      borderVisible: false,
      wickUpColor: '#ef4444',
      wickDownColor: '#22c55e',
    });
    // 新增量能柱（Histogram）系列，使用独立的 volume 价格尺度
    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: 'volume',
      priceFormat: {
        type: 'volume',
      },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    // 已删除重复的 volumeSeries 声明

    let formattedData = [];
    let volumeData = [];

    if (Array.isArray(data)) {
        // 计算价格范围用于归一化
        const allPrices = data.flatMap(item => [item.open, item.high, item.low, item.close]);
        const minPrice = Math.min(...allPrices);
        const maxPrice = Math.max(...allPrices);
        const priceRange = maxPrice - minPrice;
        
        // 计算最大成交量用于归一化
        const maxVolume = Math.max(...data.map(item => item.volume));
        
        formattedData = data.map(item => ({
            time: item.time,
            // 归一化价格：乘以70%加上固定的0.3
            open: ((item.open - minPrice) / priceRange) * 0.7 + 0.3,
            high: ((item.high - minPrice) / priceRange) * 0.7 + 0.3,
            low: ((item.low - minPrice) / priceRange) * 0.7 + 0.3,
            close: ((item.close - minPrice) / priceRange) * 0.7 + 0.3,
        }));
        // 同时生成 volume 数据，归一化到 0-0.3 范围
        volumeData = data.map(item => ({
          time: item.time,
          value: (item.volume / maxVolume) * 0.3,
          color: item.close >= item.open ? '#ef4444' : '#22c55e', // 上涨红色，下跌绿色
        }));
    } else if (data && typeof data === 'object' && Array.isArray(data.date)) {
        const len = data.date.length;
        // 计算价格范围用于归一化
        const allPrices = [...data.open, ...data.high, ...data.low, ...data.close];
        const minPrice = Math.min(...allPrices);
        const maxPrice = Math.max(...allPrices);
        const priceRange = maxPrice - minPrice;
        
        // 计算最大成交量用于归一化
        const maxVolume = Math.max(...data.volume);
        
        for (let i = 0; i < len; i++) {
            formattedData.push({
                time: data.date[i],
                // 归一化价格：乘以70%加上固定的0.3
                open: ((data.open[i] - minPrice) / priceRange) * 0.7 + 0.3,
                high: ((data.high[i] - minPrice) / priceRange) * 0.7 + 0.3,
                low: ((data.low[i] - minPrice) / priceRange) * 0.7 + 0.3,
                close: ((data.close[i] - minPrice) / priceRange) * 0.7 + 0.3,
            });
            volumeData.push({
              time: data.date[i],
              value: (data.volume[i] / maxVolume) * 0.3,
              color: data.close[i] >= data.open[i] ? '#ef4444' : '#22c55e', // 上涨红色，下跌绿色
            });
        }
    }

    candlestickSeries.setData(formattedData);
    // 设置量能柱数据
    volumeSeries.setData(volumeData);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    return () => chart.remove();
  }, [data]);

  return (
    <div className="relative bg-white rounded-xl p-4 border border-slate-200 shadow-none">
      <div ref={chartContainerRef} />
    </div>
  );
}

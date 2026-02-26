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
      },
      timeScale: {
        borderColor: '#e2e8f0',
      },
    });
    // 为量能柱添加独立的价格尺度，留出底部空间
    // 为量能柱设置占底部 30% 的空间（上部 70% 留给蜡烛图）
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.7, bottom: 0 },
    });
    // 为主价格尺度设置上部空间，留出底部给量能柱
    // 为主价格尺度（右侧）设置上部空间，留出底部给量能柱
    // 为主价格尺度（右侧）设置底部 30% 的空间，留给量能柱
    // 为主价格尺度（默认右侧）设置底部 30% 的空间，留给量能柱
    // 为主价格尺度（默认右侧）设置底部 30% 的空间，留给量能柱
    chart.priceScale('right').applyOptions({
      scaleMargins: { top: 0, bottom: 0.3 },
    });

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#ef4444',
      downColor: '#22c55e',
      borderVisible: false,
      wickUpColor: '#ef4444',
      wickDownColor: '#22c55e',
    });
    // 新增量能柱（Histogram）系列
    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: 'volume',
      // 使用默认的柱宽和颜色，可根据需求自行调整
    });
    // 已删除重复的 volumeSeries 声明

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

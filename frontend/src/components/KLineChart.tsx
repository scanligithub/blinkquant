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

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#ef4444', 
      downColor: '#22c55e', 
      borderVisible: false,
      wickUpColor: '#ef4444', 
      wickDownColor: '#22c55e',
    });

    let formattedData = [];

    if (Array.isArray(data)) {
        formattedData = data.map(item => ({
            time: item.date,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
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
        }
    }

    candlestickSeries.setData(formattedData);
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

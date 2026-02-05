'use client';
import { createChart, ColorType, IChartApi } from 'lightweight-charts';
import { useEffect, useRef } from 'react';

export default function KLineChart({ data, code }: { data: any, code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !data) return;

    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#0f172a' }, textColor: '#94a3b8' },
      grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
      width: chartContainerRef.current.clientWidth,
      height: 400,
    });

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#ef4444', downColor: '#22c55e', borderVisible: false,
      wickUpColor: '#ef4444', wickDownColor: '#22c55e',
    });

    let formattedData = [];

    // --- 核心修复：在这里判断数据格式 ---
    if (Array.isArray(data)) {
        // 兼容旧格式 (Array of Objects)
        formattedData = data.map(item => ({
            time: item.date,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
        }));
    } else if (data && typeof data === 'object' && Array.isArray(data.date)) {
        // 适配新格式 (Columnar Object: { date: [], open: [], ... })
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
    <div className="relative bg-slate-900 rounded-xl p-4 border border-slate-800">
      <div ref={chartContainerRef} />
    </div>
  );
}

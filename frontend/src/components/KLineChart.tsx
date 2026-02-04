'use client';
import { createChart, ColorType, IChartApi } from 'lightweight-charts';
import { useEffect, useRef } from 'react';

export default function KLineChart({ data, code }: { data: any[], code: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

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

    // 格式化数据：将 backend 的字段映射为 chart 要求的 time, open, high, low, close
    const formattedData = data.map(item => ({
      time: item.date,
      open: item.open,
      high: item.high,
      low: item.low,
      close: item.close,
    }));

    candlestickSeries.setData(formattedData);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    return () => chart.remove();
  }, [data]);

  return (
    <div className="relative bg-gray-900 rounded-xl p-4 border border-gray-800">
      <div className="absolute top-6 left-8 z-10">
        <span className="text-xl font-bold text-white">{code}</span>
        <span className="ml-2 text-sm text-gray-400">History (2005-Present)</span>
      </div>
      <div ref={chartContainerRef} />
    </div>
  );
}

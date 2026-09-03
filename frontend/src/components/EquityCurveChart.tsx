'use client';

import { useEffect, useRef } from 'react';
import { createChart, IChartApi, ISeriesApi, LineData } from 'lightweight-charts';

interface EquityCurveChartProps {
  data: Array<{ date: string; equity: number }>;
}

export default function EquityCurveChart({ data }: EquityCurveChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 200,
      layout: { background: { color: '#fff' }, textColor: '#666' },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
      rightPriceScale: { borderColor: '#e0e0e0' },
      timeScale: { borderColor: '#e0e0e0', timeVisible: false },
    });

    const lineSeries = chart.addLineSeries({
      color: '#2196F3',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    const seriesData: LineData[] = data.map((d) => ({
      time: d.date as string,
      value: d.equity,
    }));

    lineSeries.setData(seriesData);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data]);

  if (data.length === 0) {
    return <div className="h-[200px] flex items-center justify-center text-gray-400 text-sm">暂无数据</div>;
  }

  return <div ref={containerRef} className="w-full h-[200px]" />;
}

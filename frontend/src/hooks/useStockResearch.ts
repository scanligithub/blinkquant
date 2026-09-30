'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { applyAdjust } from '@/utils/applyAdjust';
import { parseParquetRecords } from '@/utils/parquet';

export type AdjustMode = 'none' | 'qfq' | 'hfq';

export interface SelectedStock {
  kind: 'stock' | 'sector';
  code: string;
  name?: string;
  data: any;
}

export interface StockListItem {
  code: string;
  name: string;
}

function resampleDailyData(dailyData: any[], targetTimeframe: string): any[] {
  if (targetTimeframe === 'D') return dailyData;

  const grouped = new Map<string, any[]>();

  dailyData.forEach((item) => {
    const date = new Date(item.time * 1000);
    let key: string;

    if (targetTimeframe === 'W') {
      const dayOfWeek = date.getDay();
      const weekStart = new Date(date);
      weekStart.setDate(date.getDate() - dayOfWeek);
      key = weekStart.toISOString().split('T')[0];
    } else {
      key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
    }

    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(item);
  });

  const resampled: any[] = [];

  grouped.forEach((items) => {
    const sortedItems = items.sort((a, b) => a.time - b.time);
    const first = sortedItems[0];
    const last = sortedItems[sortedItems.length - 1];

    resampled.push({
      time: first.time,
      open: first.open,
      high: Math.max(...sortedItems.map((i) => i.high)),
      low: Math.min(...sortedItems.map((i) => i.low)),
      close: last.close,
      volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
      amount: sortedItems.reduce((sum, i) => sum + (i.amount || 0), 0),
      turn: last.turn,
      peTTM: last.peTTM,
      total_mv: last.total_mv,
      float_mv: last.float_mv,
      main_net: sortedItems.reduce((sum, i) => sum + (i.main_net || 0), 0),
    });
  });

  return resampled.sort((a, b) => a.time - b.time);
}

export default function useStockResearch() {
  const [chartTimeframe, setChartTimeframe] = useState('D');
  const [subChartType, setSubChartType] = useState('MACD');
  const [mainChartType, setMainChartType] = useState('MA');
  const [isFullScreen, setIsFullScreen] = useState(false);
  const [showRotateHint, setShowRotateHint] = useState(false);

  const chartWrapperRef = useRef<HTMLDivElement>(null);
  const [adjustMode, setAdjustMode] = useState<AdjustMode>('none');
  const [adjustMenuOpen, setAdjustMenuOpen] = useState(false);
  const adjustMenuRef = useRef<HTMLDivElement>(null);

  const [selectedStock, setSelectedStock] = useState<SelectedStock | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [dailyDataCache, setDailyDataCache] = useState<any[]>([]);
  const [sectorDataCache, setSectorDataCache] = useState<any[]>([]);
  const [sectors, setSectors] = useState<{ code: string; name: string; type: string }[]>([]);
  const [expandedSectors, setExpandedSectors] = useState<Record<string, boolean>>({});
  const lastStockRef = useRef<{ code: string; name: string } | null>(null);

  const adjustedDaily = useMemo(
    () => applyAdjust(dailyDataCache, adjustMode),
    [dailyDataCache, adjustMode]
  );

  const [stockList, setStockList] = useState<StockListItem[]>([]);

  useEffect(() => {
    const saved = localStorage.getItem('klineAdjustMode');
    if (saved === 'none' || saved === 'qfq' || saved === 'hfq') {
      setAdjustMode(saved);
    }
  }, []);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        adjustMenuOpen &&
        adjustMenuRef.current &&
        !adjustMenuRef.current.contains(e.target as Node)
      ) {
        setAdjustMenuOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [adjustMenuOpen]);

  useEffect(() => {
    const handler = () => {
      const fullscreen = !!document.fullscreenElement;
      setIsFullScreen(fullscreen);

      if (!fullscreen) {
        setShowRotateHint(false);
        if (screen.orientation && screen.orientation.unlock) {
          screen.orientation.unlock();
        }
      }
    };

    document.addEventListener('fullscreenchange', handler);
    return () => document.removeEventListener('fullscreenchange', handler);
  }, []);

  useEffect(() => {
    const handleOrientationChange = () => {
      if (window.innerWidth > window.innerHeight) {
        setShowRotateHint(false);
      }
    };

    window.addEventListener('resize', handleOrientationChange);
    window.addEventListener('orientationchange', handleOrientationChange);

    return () => {
      window.removeEventListener('resize', handleOrientationChange);
      window.removeEventListener('orientationchange', handleOrientationChange);
    };
  }, []);

  useEffect(() => {
    const loadStockList = async () => {
      const CACHE_KEY = 'stockListCache_v1';
      const CACHE_EXPIRY_MS = 24 * 60 * 60 * 1000;
      const cachedStr = localStorage.getItem(CACHE_KEY);

      if (cachedStr) {
        try {
          const cachedData = JSON.parse(cachedStr);
          if (
            Date.now() - cachedData.timestamp < CACHE_EXPIRY_MS &&
            Array.isArray(cachedData.list)
          ) {
            setStockList(cachedData.list);
            return;
          }
        } catch {
          console.warn('Cache parse failed, fetching fresh list');
        }
      }

      try {
        const res = await fetch('/api/stock-list');
        if (!res.ok) throw new Error('Failed to load stock list');

        const data = await res.json();
        setStockList(data);
        localStorage.setItem(
          CACHE_KEY,
          JSON.stringify({ timestamp: Date.now(), list: data })
        );
      } catch (err) {
        console.error('Failed to load stock list', err);
      }
    };

    loadStockList();
  }, []);

  const viewStock = useCallback(
    async (code: string) => {
      setChartLoading(true);

      try {
        const res = await fetch(`/api/kline?code=${code}&timeframe=D`);
        if (!res.ok) throw new Error('Fetch failed');

        const buffer = await res.arrayBuffer();
        if (buffer.byteLength === 0) throw new Error('Empty buffer');

        const records = await parquetReadObjects({ file: buffer, compressors });
        if (!records || records.length === 0) throw new Error('Empty records');

        const dailyData = parseParquetRecords(records);
        setDailyDataCache(dailyData);

        const adjusted = applyAdjust(dailyData, adjustMode);
        const resampledData = resampleDailyData(adjusted, chartTimeframe);
        const stock = stockList.find((s) => s.code === code);

        setSelectedStock({
          kind: 'stock',
          code,
          name: stock?.name || code,
          data: resampledData,
        });

        try {
          const sectorRes = await fetch(
            `/api/stock-sectors?code=${encodeURIComponent(code)}`
          );
          if (sectorRes.ok) {
            const sectorJson = await sectorRes.json();
            setSectors(sectorJson.sectors || []);
            setExpandedSectors({});
          } else {
            setSectors([]);
          }
        } catch (e) {
          console.warn('Failed to load stock sectors:', e);
          setSectors([]);
        }
      } catch (err: any) {
        alert(`Failed: ${err.message}`);
      } finally {
        setChartLoading(false);
      }
    },
    [adjustMode, chartTimeframe, stockList]
  );

  const viewSector = useCallback(
    async (sectorCode: string, sectorName: string) => {
      setChartLoading(true);

      try {
        const res = await fetch(
          `/api/sector-kline?code=${encodeURIComponent(sectorCode)}&timeframe=D`
        );
        if (!res.ok) throw new Error('Fetch failed');

        const buffer = await res.arrayBuffer();
        if (buffer.byteLength === 0) throw new Error('Empty buffer');

        const records = await parquetReadObjects({ file: buffer, compressors });
        if (!records || records.length === 0) throw new Error('Empty records');

        const sectorDaily = parseParquetRecords(records);
        setSectorDataCache(sectorDaily);
        setSelectedStock({
          kind: 'sector',
          code: sectorCode,
          name: sectorName,
          data: resampleDailyData(sectorDaily, chartTimeframe),
        });
      } catch (err: any) {
        alert(`Failed: ${err.message}`);
      } finally {
        setChartLoading(false);
      }
    },
    [chartTimeframe]
  );

  const changeChartTimeframe = useCallback(
    (value: string) => {
      setChartTimeframe(value);
      setSelectedStock((prev) => {
        if (!prev) return prev;

        if (prev.kind === 'sector') {
          return sectorDataCache.length > 0
            ? { ...prev, data: resampleDailyData(sectorDataCache, value) }
            : prev;
        }

        return adjustedDaily.length > 0
          ? { ...prev, data: resampleDailyData(adjustedDaily, value) }
          : prev;
      });
    },
    [adjustedDaily, sectorDataCache]
  );

  const changeAdjustMode = useCallback(
    (value: AdjustMode) => {
      setAdjustMode(value);
      localStorage.setItem('klineAdjustMode', value);
      setAdjustMenuOpen(false);

      if (selectedStock?.kind === 'stock' && dailyDataCache.length > 0) {
        const adjusted = applyAdjust(dailyDataCache, value);
        setSelectedStock((prev) =>
          prev
            ? { ...prev, data: resampleDailyData(adjusted, chartTimeframe) }
            : prev
        );
      }
    },
    [chartTimeframe, dailyDataCache, selectedStock?.kind]
  );

  const isIOS = () =>
    /iPad|iPhone|iPod/.test(navigator.userAgent) ||
    (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

  const isMobile = () => window.innerWidth < 768;

  const toggleFullscreen = useCallback(async () => {
    if (!document.fullscreenElement) {
      try {
        await chartWrapperRef.current?.requestFullscreen();

        if (isMobile()) {
          if (isIOS()) {
            setShowRotateHint(true);
          } else {
            try {
              await (screen.orientation as any).lock('landscape');
            } catch (e) {
              console.log('Orientation lock not supported:', e);
            }
          }
        }
      } catch (e) {
        console.log('Fullscreen request failed:', e);
      }
      return;
    }

    try {
      await document.exitFullscreen();
      setShowRotateHint(false);
      if (screen.orientation && screen.orientation.unlock) {
        screen.orientation.unlock();
      }
    } catch (e) {
      console.log('Exit fullscreen failed:', e);
    }
  }, []);

  const returnToStock = useCallback(() => {
    const last = lastStockRef.current;
    if (!last) return;

    setSelectedStock({
      kind: 'stock',
      code: last.code,
      name: last.name,
      data: resampleDailyData(adjustedDaily, chartTimeframe),
    });
  }, [adjustedDaily, chartTimeframe]);

  return {
    chartTimeframe,
    subChartType,
    mainChartType,
    setSubChartType,
    setMainChartType,
    changeChartTimeframe,
    isFullScreen,
    showRotateHint,
    chartWrapperRef,
    adjustMode,
    adjustMenuOpen,
    adjustMenuRef,
    changeAdjustMode,
    setAdjustMenuOpen,
    selectedStock,
    chartLoading,
    dailyDataCache,
    adjustedDaily,
    sectorDataCache,
    sectors,
    expandedSectors,
    setExpandedSectors,
    lastStockRef,
    stockList,
    viewStock,
    viewSector,
    toggleFullscreen,
    returnToStock,
  };
}

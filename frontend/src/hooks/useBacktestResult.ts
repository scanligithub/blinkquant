'use client';

import { useCallback, useEffect, useState } from 'react';
import {
  downloadArtifact,
  loadEquityCurve,
  loadTradesPage,
  loadPositionsPage,
  PREVIEW_PAGE_SIZE,
  type EquityPoint,
  type PositionRow,
  type TradeRow,
  type ArtifactName,
} from '@/lib/backtestArtifacts';

export function useBacktestResult(taskId: number | null) {
  const [equity, setEquity] = useState<EquityPoint[] | null>(null);
  const [trades, setTrades] = useState<TradeRow[]>([]);
  const [tradesTotal, setTradesTotal] = useState(0);
  const [tradesOffset, setTradesOffset] = useState(0);
  const [positions, setPositions] = useState<PositionRow[]>([]);
  const [positionsTotal, setPositionsTotal] = useState(0);
  const [positionsOffset, setPositionsOffset] = useState(0);
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setEquity(null);
    setTrades([]);
    setTradesTotal(0);
    setTradesOffset(0);
    setPositions([]);
    setPositionsTotal(0);
    setPositionsOffset(0);
    setError(null);
    setLoading(null);
  }, [taskId]);

  const loadEquity = useCallback(async () => {
    if (taskId == null) return;
    setLoading('equity_curve');
    setError(null);
    try {
      const rows = await loadEquityCurve(taskId);
      setEquity(rows);
      return rows;
    } catch (e: any) {
      setError(e?.message || 'load equity failed');
      throw e;
    } finally {
      setLoading(null);
    }
  }, [taskId]);

  const loadTradesPageFn = useCallback(
    async (offset = 0) => {
      if (taskId == null) return;
      setLoading('trades');
      setError(null);
      try {
        const page = await loadTradesPage(taskId, { limit: PREVIEW_PAGE_SIZE, offset });
        setTrades(page.rows);
        setTradesTotal(page.total);
        setTradesOffset(page.offset);
      } catch (e: any) {
        setError(e?.message || 'load trades failed');
      } finally {
        setLoading(null);
      }
    },
    [taskId]
  );

  const loadPositionsPageFn = useCallback(
    async (offset = 0) => {
      if (taskId == null) return;
      setLoading('positions_daily');
      setError(null);
      try {
        const page = await loadPositionsPage(taskId, { limit: PREVIEW_PAGE_SIZE, offset });
        setPositions(page.rows);
        setPositionsTotal(page.total);
        setPositionsOffset(page.offset);
      } catch (e: any) {
        setError(e?.message || 'load positions failed');
      } finally {
        setLoading(null);
      }
    },
    [taskId]
  );

  const download = useCallback(
    (name: ArtifactName) => {
      if (taskId == null) return;
      return downloadArtifact(taskId, name);
    },
    [taskId]
  );

  return {
    equity,
    trades,
    tradesTotal,
    tradesOffset,
    tradesPageSize: PREVIEW_PAGE_SIZE,
    loadTradesPage: loadTradesPageFn,
    tradesNext: () => loadTradesPageFn(tradesOffset + PREVIEW_PAGE_SIZE),
    tradesPrev: () => loadTradesPageFn(Math.max(0, tradesOffset - PREVIEW_PAGE_SIZE)),
    positions,
    positionsTotal,
    positionsOffset,
    positionsPageSize: PREVIEW_PAGE_SIZE,
    loadPositionsPage: loadPositionsPageFn,
    positionsNext: () => loadPositionsPageFn(positionsOffset + PREVIEW_PAGE_SIZE),
    positionsPrev: () => loadPositionsPageFn(Math.max(0, positionsOffset - PREVIEW_PAGE_SIZE)),
    loading,
    error,
    loadEquity,
    download,
  };
}

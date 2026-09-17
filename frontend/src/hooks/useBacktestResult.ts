'use client';

import { useCallback, useState } from 'react';
import {
  downloadArtifact,
  loadEquityCurve,
  loadPositions,
  loadTrades,
  type EquityPoint,
  type PositionRow,
  type TradeRow,
  type ArtifactName,
} from '@/lib/backtestArtifacts';

export function useBacktestResult(taskId: number | null) {
  const [equity, setEquity] = useState<EquityPoint[] | null>(null);
  const [trades, setTrades] = useState<TradeRow[] | null>(null);
  const [positions, setPositions] = useState<PositionRow[] | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadEquity = useCallback(async () => {
    if (taskId == null) return;
    if (equity) return equity;
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
  }, [taskId, equity]);

  const loadTradesTab = useCallback(async () => {
    if (taskId == null) return;
    if (trades) return trades;
    setLoading('trades');
    setError(null);
    try {
      const rows = await loadTrades(taskId);
      setTrades(rows);
      return rows;
    } catch (e: any) {
      setError(e?.message || 'load trades failed');
      throw e;
    } finally {
      setLoading(null);
    }
  }, [taskId, trades]);

  const loadPositionsTab = useCallback(async () => {
    if (taskId == null) return;
    if (positions) return positions;
    setLoading('positions_daily');
    setError(null);
    try {
      const rows = await loadPositions(taskId);
      setPositions(rows);
      return rows;
    } catch (e: any) {
      setError(e?.message || 'load positions failed');
      throw e;
    } finally {
      setLoading(null);
    }
  }, [taskId, positions]);

  const download = useCallback(
    (name: ArtifactName) => {
      if (taskId == null) return;
      return downloadArtifact(taskId, name);
    },
    [taskId]
  );

  const reset = useCallback(() => {
    setEquity(null);
    setTrades(null);
    setPositions(null);
    setError(null);
  }, []);

  return {
    equity,
    trades,
    positions,
    loading,
    error,
    loadEquity,
    loadTradesTab,
    loadPositionsTab,
    download,
    reset,
  };
}

'use client';

import { useEffect, useState } from 'react';
import EquityCurveChart from './EquityCurveChart';
import TradesTable from './TradesTable';
import PositionsTable from './PositionsTable';
import { useBacktestResult } from '@/hooks/useBacktestResult';
import { downloadArtifact, type ArtifactName } from '@/lib/backtestArtifacts';

interface LegacyBacktestResult {
  formula: string;
  start_date: string;
  signal_end_date: string;
  valuation_end_date: string;
  initial_cash: number;
  equity_curve: Array<{ date: string; equity: number; cash: number; positions_value: number }>;
  trades: Array<{
    signal_date: string;
    execution_date: string;
    code: string;
    side: string;
    qty: number;
    price: number;
    fee: number;
  }>;
  positions_daily: Array<{
    date: string;
    code: string;
    qty: number;
    cost: number;
    market_value: number;
  }>;
  metrics: {
    total_return: number;
    cagr: number;
    sharpe: number;
    max_drawdown: number;
    total_days: number;
  };
}

interface Summary {
  total_return?: number;
  max_drawdown?: number;
  n_trades?: number;
  n_positions?: number;
  final_equity?: number;
}

interface BacktestResultsProps {
  result?: LegacyBacktestResult | null;
  taskId?: number | null;
  summary?: Summary | null;
}

function PaginationBar({
  total,
  offset,
  pageSize,
  loading,
  onPrev,
  onNext,
  taskId,
  artifactName,
  fileName,
}: {
  total: number;
  offset: number;
  pageSize: number;
  loading: boolean;
  onPrev: () => void;
  onNext: () => void;
  taskId: number;
  artifactName: ArtifactName;
  fileName: string;
}) {
  const from = total === 0 ? 0 : offset + 1;
  const to = Math.min(offset + pageSize, total);
  const hasPrev = offset > 0;
  const hasNext = offset + pageSize < total;

  return (
    <div className="flex items-center justify-between text-xs text-gray-500 mb-2">
      <span>
        显示 {from}–{to} / 共 {total.toLocaleString()} 条
        {total > pageSize ? '（预览，完整数据请下载）' : ''}
      </span>
      <div className="flex items-center gap-2">
        <button
          disabled={!hasPrev || loading}
          onClick={onPrev}
          className="px-2 py-1 rounded border text-xs disabled:opacity-40 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          上一页
        </button>
        <button
          disabled={!hasNext || loading}
          onClick={onNext}
          className="px-2 py-1 rounded border text-xs disabled:opacity-40 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          下一页
        </button>
        <button
          onClick={() => downloadArtifact(taskId, artifactName)}
          className="px-2 py-1 rounded border text-xs text-blue-600 hover:bg-blue-50"
        >
          下载{fileName}.parquet
        </button>
      </div>
    </div>
  );
}

export default function BacktestResults({ result, taskId, summary }: BacktestResultsProps) {
  const [tab, setTab] = useState<'equity' | 'trades' | 'positions'>('equity');
  const hook = useBacktestResult(taskId ?? null);

  const isLegacy = !!result && !taskId;
  const m = isLegacy ? result!.metrics : summary;
  const finalEquity = isLegacy
    ? result!.equity_curve[result!.equity_curve.length - 1]?.equity ?? 0
    : (summary?.final_equity ?? 0);

  useEffect(() => {
    setTab('equity');
  }, [taskId]);

  useEffect(() => {
    if (taskId && tab === 'equity') {
      hook.loadEquity().catch(() => {});
    }
    if (taskId && tab === 'trades') {
      hook.loadTradesPage(0);
    }
    if (taskId && tab === 'positions') {
      hook.loadPositionsPage(0);
    }
  }, [taskId, tab]);

  const equityData = isLegacy
    ? result!.equity_curve.map((d) => ({ date: d.date, equity: d.equity }))
    : hook.equity?.map((d) => ({ date: d.date, equity: d.equity })) ?? [];

  const tradesData = isLegacy ? result!.trades : hook.trades;
  const positionsData = isLegacy ? result!.positions_daily : hook.positions;

  const tradesCount = isLegacy ? result!.trades.length : hook.tradesTotal || summary?.n_trades || 0;
  const positionsCount = isLegacy ? result!.positions_daily.length : hook.positionsTotal || summary?.n_positions || 0;

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-2 text-center">
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">最终权益</div>
          <div className="text-sm font-semibold">{finalEquity.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">收益率</div>
          <div className={`text-sm font-semibold ${((m?.total_return ?? 0)) >= 0 ? 'text-red-600' : 'text-green-600'}`}>
            {(((m?.total_return ?? 0)) * 100).toFixed(2)}%
          </div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">最大回撤</div>
          <div className="text-sm font-semibold text-green-600">
            {(((m?.max_drawdown ?? 0)) * 100).toFixed(2)}%
          </div>
        </div>
      </div>

      <div className="flex gap-1 text-xs">
        {(['equity', 'trades', 'positions'] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-3 py-1.5 rounded-lg ${
              tab === t ? 'bg-blue-100 text-blue-700 font-medium' : 'text-gray-500 hover:bg-gray-100'
            }`}
          >
            {t === 'equity' ? '权益曲线' : t === 'trades' ? `交易 (${tradesCount.toLocaleString()})` : `持仓 (${positionsCount.toLocaleString()})`}
          </button>
        ))}
      </div>

      {tab === 'equity' && (
        <>
          {hook.loading === 'equity_curve' && (
            <div className="h-[200px] flex items-center justify-center text-gray-400 text-sm">加载权益曲线…</div>
          )}
          {!hook.loading && equityData.length === 0 && (
            <div className="h-[200px] flex items-center justify-center text-gray-400 text-sm">暂无数据</div>
          )}
          {equityData.length > 0 && <EquityCurveChart data={equityData} />}
        </>
      )}
      {tab === 'trades' && (
        <>
          {taskId && (
            <PaginationBar
              total={tradesCount}
              offset={hook.tradesOffset}
              pageSize={hook.tradesPageSize}
              loading={hook.loading === 'trades'}
              onPrev={hook.tradesPrev}
              onNext={hook.tradesNext}
              taskId={taskId}
              artifactName="trades"
              fileName="成交"
            />
          )}
          {hook.loading === 'trades' && (
            <div className="text-center text-gray-400 text-sm py-4">加载交易记录…</div>
          )}
          {!hook.loading && tradesData.length === 0 && (
            <div className="text-center text-gray-400 text-sm py-4">暂无交易</div>
          )}
          {tradesData.length > 0 && <TradesTable trades={tradesData} />}
        </>
      )}
      {tab === 'positions' && (
        <>
          {taskId && (
            <PaginationBar
              total={positionsCount}
              offset={hook.positionsOffset}
              pageSize={hook.positionsPageSize}
              loading={hook.loading === 'positions_daily'}
              onPrev={hook.positionsPrev}
              onNext={hook.positionsNext}
              taskId={taskId}
              artifactName="positions_daily"
              fileName="持仓"
            />
          )}
          {hook.loading === 'positions_daily' && (
            <div className="text-center text-gray-400 text-sm py-4">加载持仓数据…</div>
          )}
          {!hook.loading && positionsData.length === 0 && (
            <div className="text-center text-gray-400 text-sm py-4">暂无持仓</div>
          )}
          {positionsData.length > 0 && <PositionsTable positions={positionsData} />}
        </>
      )}

      {hook.error && (
        <div className="text-xs text-red-500 bg-red-50 rounded p-2">
          {hook.error}
        </div>
      )}
    </div>
  );
}

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
    sortino?: number;
    calmar?: number;
    drawdown_duration?: number;
    turnover?: number;
    total_fees?: number;
    buy_count?: number;
    sell_count?: number;
  };
}

interface Summary {
  total_return?: number;
  max_drawdown?: number;
  cagr?: number;
  sharpe?: number;
  sortino?: number;
  calmar?: number;
  drawdown_duration?: number;
  turnover?: number;
  total_fees?: number;
  buy_count?: number;
  sell_count?: number;
  n_trades?: number;
  n_positions?: number;
  final_equity?: number;
  initial_cash?: number;
}

interface TaskMeta {
  id: number;
  user_id: string;
  status: string;
  payload: {
    formula?: string;
    start_date?: string;
    end_signal_date?: string;
    signal_end_date?: string;
    end_date?: string;
    initial_cash?: number;
  };
  assigned_node?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  result_uri?: string | null;
  result_bytes?: number | null;
}

function formatDuration(started?: string | null, finished?: string | null): string {
  if (!started || !finished) return '—';
  const a = Date.parse(started.includes('T') ? started : started.replace(' ', 'T') + 'Z');
  const b = Date.parse(finished.includes('T') ? finished : finished.replace(' ', 'T') + 'Z');
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return '—';
  const sec = Math.round((b - a) / 1000);
  if (sec < 60) return `${sec} 秒`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return s ? `${m} 分 ${s} 秒` : `${m} 分钟`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm ? `${h} 小时 ${rm} 分` : `${h} 小时`;
}

function ParamRow({
  label,
  value,
  mono,
}: {
  label: string;
  value: string | number | null | undefined;
  mono?: boolean;
}) {
  return (
    <div className="grid grid-cols-[7rem_1fr] gap-2 py-1.5 border-b border-gray-50 last:border-0 text-xs">
      <div className="text-gray-500 shrink-0">{label}</div>
      <div className={`text-gray-800 break-all ${mono ? 'font-mono text-[11px]' : ''}`}>
        {value ?? '—'}
      </div>
    </div>
  );
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
  const [tab, setTab] = useState<'equity' | 'trades' | 'positions' | 'params'>('equity');
  const hook = useBacktestResult(taskId ?? null);
  const [taskMeta, setTaskMeta] = useState<TaskMeta | null>(null);
  const [metaLoading, setMetaLoading] = useState(false);
  const [metaError, setMetaError] = useState<string | null>(null);

  const isLegacy = !!result && !taskId;
  const m = isLegacy ? result!.metrics : summary;
  const finalEquity = isLegacy
    ? result!.equity_curve[result!.equity_curve.length - 1]?.equity ?? 0
    : (summary?.final_equity ?? 0);

  useEffect(() => {
    setTab('equity');
    setTaskMeta(null);
    setMetaError(null);
  }, [taskId]);

  useEffect(() => {
    if (!taskId) return;
    let cancelled = false;
    setMetaLoading(true);
    setMetaError(null);
    fetch(`/api/v1/tasks/${taskId}`, { cache: 'no-store' })
      .then(async (res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        if (!cancelled) setTaskMeta(data as TaskMeta);
      })
      .catch((e) => {
        if (!cancelled) setMetaError(e?.message || '加载任务参数失败');
      })
      .finally(() => {
        if (!cancelled) setMetaLoading(false);
      });
    return () => {
      cancelled = true;
    };
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
      <div className="grid grid-cols-3 sm:grid-cols-4 gap-2 text-center">
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">最终权益</div>
          <div className="text-sm font-semibold">{finalEquity.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">总收益</div>
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
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">年化</div>
          <div className="text-sm font-semibold">
            {m?.cagr != null ? `${(m.cagr * 100).toFixed(2)}%` : '—'}
          </div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">夏普</div>
          <div className="text-sm font-semibold">{m?.sharpe != null ? m.sharpe.toFixed(2) : '—'}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">索提诺</div>
          <div className="text-sm font-semibold">{m?.sortino != null ? m.sortino.toFixed(2) : '—'}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">卡尔玛</div>
          <div className="text-sm font-semibold">{m?.calmar != null ? m.calmar.toFixed(2) : '—'}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">回撤天数</div>
          <div className="text-sm font-semibold">{m?.drawdown_duration ?? '—'}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">换手</div>
          <div className="text-sm font-semibold">{m?.turnover != null ? m.turnover.toFixed(2) : '—'}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">总费用</div>
          <div className="text-sm font-semibold">
            {m?.total_fees != null ? m.total_fees.toLocaleString(undefined, { maximumFractionDigits: 0 }) : '—'}
          </div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">买 / 卖</div>
          <div className="text-sm font-semibold">
            {(m?.buy_count ?? '—')} / {(m?.sell_count ?? '—')}
          </div>
        </div>
      </div>

      <div className="flex gap-1 text-xs">
        {(
          [
            { id: 'equity' as const, label: '权益曲线' },
            { id: 'trades' as const, label: `交易 (${tradesCount.toLocaleString()})` },
            { id: 'positions' as const, label: `持仓 (${positionsCount.toLocaleString()})` },
            { id: 'params' as const, label: '回测参数' },
          ] as const
        ).map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-3 py-1.5 rounded-lg ${
              tab === t.id ? 'bg-blue-100 text-blue-700 font-medium' : 'text-gray-500 hover:bg-gray-100'
            }`}
          >
            {t.label}
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
      {tab === 'params' && (
        <div className="bg-gray-50 rounded-lg px-3 py-2">
          {metaLoading && (
            <div className="text-center text-gray-400 text-sm py-4">加载回测参数…</div>
          )}
          {metaError && <div className="text-xs text-red-500 py-2">{metaError}</div>}
          {!metaLoading && !metaError && (
            <div>
              {(() => {
                const p = taskMeta?.payload;
                const formula = p?.formula ?? (isLegacy ? result!.formula : undefined);
                const start = p?.start_date ?? (isLegacy ? result!.start_date : undefined);
                const end =
                  p?.end_signal_date ??
                  p?.signal_end_date ??
                  p?.end_date ??
                  (isLegacy ? result!.signal_end_date : undefined);
                const cash =
                  p?.initial_cash ??
                  summary?.initial_cash ??
                  (isLegacy ? result!.initial_cash : undefined);
                const period = start && end ? `${start} → ${end}` : start || end || '—';
                const duration = taskMeta
                  ? formatDuration(taskMeta.started_at, taskMeta.finished_at)
                  : '—';
                return (
                  <>
                    <ParamRow label="策略公式" value={formula || '—'} mono />
                    <ParamRow label="回测区间" value={period} />
                    <ParamRow
                      label="初始资金"
                      value={
                        cash != null
                          ? Number(cash).toLocaleString(undefined, { maximumFractionDigits: 0 })
                          : '—'
                      }
                    />
                    <ParamRow label="任务 ID" value={taskId ?? taskMeta?.id ?? '—'} mono />
                    <ParamRow label="发起用户" value={taskMeta?.user_id ?? '—'} mono />
                    <ParamRow label="执行节点" value={taskMeta?.assigned_node ?? '—'} />
                    <ParamRow label="任务状态" value={taskMeta?.status ?? (isLegacy ? 'legacy' : '—')} />
                    <ParamRow label="开始时间" value={taskMeta?.started_at ?? '—'} />
                    <ParamRow label="结束时间" value={taskMeta?.finished_at ?? '—'} />
                    <ParamRow label="运行耗时" value={duration} />
                    <ParamRow
                      label="成交 / 持仓"
                      value={`${(summary?.n_trades ?? tradesCount).toLocaleString()} 笔 / ${(summary?.n_positions ?? positionsCount).toLocaleString()} 行`}
                    />
                    {taskMeta?.result_uri && (
                      <ParamRow label="结果路径" value={taskMeta.result_uri} mono />
                    )}
                    {taskMeta?.result_bytes != null && (
                      <ParamRow
                        label="结果大小"
                        value={`${(taskMeta.result_bytes / 1024).toFixed(1)} KB`}
                      />
                    )}
                  </>
                );
              })()}
            </div>
          )}
        </div>
      )}

      {hook.error && (
        <div className="text-xs text-red-500 bg-red-50 rounded p-2">
          {hook.error}
        </div>
      )}
    </div>
  );
}

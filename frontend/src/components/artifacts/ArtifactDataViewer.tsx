'use client';

import { useEffect, useMemo, useState } from 'react';
import { downloadArtifact, type ArtifactName } from '@/lib/backtestArtifacts';

const PAGE_SIZE = 100;
const MAX_EQUITY_POINTS = 5000;

interface ArtifactPage {
  rows: Array<Record<string, unknown>>;
  total: number;
  offset: number;
  limit: number;
}

interface EquityPoint {
  date: string;
  equity: number;
  cash?: number;
  positions_value?: number;
}

interface Column {
  key: string;
  label: string;
}

async function fetchArtifactPage(
  artifactId: number,
  name: ArtifactName,
  offset: number,
  limit: number,
): Promise<ArtifactPage> {
  const params = new URLSearchParams({
    name,
    fmt: 'json',
    limit: String(limit),
    offset: String(offset),
  });
  const res = await fetch('/api/artifacts/' + artifactId + '/part?' + params.toString(), {
    cache: 'no-store',
  });
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error('登录已过期，请重新登录。');
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(String(data.error || name + ' 数据读取失败（HTTP ' + res.status + '）'));
  }
  const rows = Array.isArray(data.rows) ? data.rows as Array<Record<string, unknown>> : [];
  return {
    rows,
    total: Number(data.total ?? rows.length),
    offset: Number(data.offset ?? offset),
    limit: Number(data.limit ?? limit),
  };
}

function dateLabel(value: unknown): string {
  if (value == null) return '—';
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (typeof value === 'number' && value > 1e11) {
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) return date.toISOString().slice(0, 10);
  }
  const text = String(value);
  return text.length >= 10 ? text.slice(0, 10) : text;
}

function formatCell(key: string, value: unknown): string {
  if (value == null || value === '') return '—';
  if (key === 'side') {
    const side = String(value).toUpperCase();
    if (side === 'BUY' || side === 'B') return '买入';
    if (side === 'SELL' || side === 'S') return '卖出';
    return String(value);
  }
  if (key === 'date' || key === 'signal_date' || key === 'execution_date') return dateLabel(value);
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '—';
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  return String(value);
}

function EquityCurveChart({ artifactId }: { artifactId: number }) {
  const [rows, setRows] = useState<EquityPoint[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const first = await fetchArtifactPage(artifactId, 'equity_curve', 0, 1);
        const targetOffset = Math.max(0, first.total - MAX_EQUITY_POINTS);
        const result = first.total <= 1
          ? first
          : await fetchArtifactPage(artifactId, 'equity_curve', targetOffset, MAX_EQUITY_POINTS);
        const nextRows = result.rows.map((row) => ({
          date: dateLabel(row.date),
          equity: Number(row.equity),
          cash: row.cash == null ? undefined : Number(row.cash),
          positions_value: row.positions_value == null ? undefined : Number(row.positions_value),
        })).filter((row) => Number.isFinite(row.equity));
        if (active) {
          setRows(nextRows);
          setTotal(first.total);
        }
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause.message : '权益曲线加载失败');
      } finally {
        if (active) setLoading(false);
      }
    };
    void load();
    return () => { active = false; };
  }, [artifactId]);

  const chart = useMemo(() => {
    if (!rows.length) return null;
    const width = 960;
    const height = 280;
    const padX = 28;
    const padY = 24;
    const values = rows.map((point) => point.equity);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const coords = rows.map((point, index) => {
      const x = padX + ((width - padX * 2) * index) / Math.max(1, rows.length - 1);
      const y = padY + ((height - padY * 2) * (max - point.equity)) / range;
      return x.toFixed(2) + ',' + y.toFixed(2);
    });
    return { width, height, min, max, points: coords.join(' ') };
  }, [rows]);

  return (
    <div className="rounded-xl border border-slate-200 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="font-bold text-slate-800">收益曲线视图</h3>
          <p className="mt-1 text-xs text-slate-500">按权益曲线中的账户总权益绘制。</p>
        </div>
        {rows.length > 0 && (
          <div className="text-right text-xs text-slate-500">
            <div>最新权益 <span className="font-mono font-semibold text-slate-800">{rows[rows.length - 1].equity.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span></div>
            <div className="mt-1">区间 {rows[0].date || '—'} → {rows[rows.length - 1].date || '—'}</div>
          </div>
        )}
      </div>
      {loading ? (
        <div className="py-12 text-center text-sm text-slate-400">正在加载收益曲线…</div>
      ) : error ? (
        <div className="py-8 text-center text-sm text-rose-600">{error}</div>
      ) : !chart ? (
        <div className="py-10 text-center text-sm text-slate-400">没有可显示的权益曲线数据。</div>
      ) : (
        <>
          <div className="mt-4 w-full overflow-x-auto">
            <svg viewBox="0 0 960 280" role="img" aria-label="账户总权益收益曲线" className="block w-full min-w-[560px]">
              <line x1="28" y1="24" x2="28" y2="256" stroke="#e2e8f0" strokeWidth="1" />
              <line x1="28" y1="256" x2="932" y2="256" stroke="#e2e8f0" strokeWidth="1" />
              <line x1="28" y1="24" x2="932" y2="24" stroke="#f1f5f9" strokeWidth="1" />
              <polyline points={chart.points} fill="none" stroke="#2563eb" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
            </svg>
          </div>
          <div className="mt-1 flex justify-between gap-3 text-xs text-slate-500">
            <span>最低权益：{chart.min.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
            <span>最高权益：{chart.max.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
          </div>
          {total > MAX_EQUITY_POINTS && (
            <p className="mt-2 text-xs text-slate-400">为控制浏览器内存，图表只绘制最近 {MAX_EQUITY_POINTS.toLocaleString()} 个点；明细表每页只加载 {PAGE_SIZE} 条。</p>
          )}
        </>
      )}
    </div>
  );
}

function PagedArtifactTable({
  artifactId,
  name,
  title,
  description,
  columns,
}: {
  artifactId: number;
  name: Extract<ArtifactName, 'trades' | 'positions_daily'>;
  title: string;
  description: string;
  columns: Column[];
}) {
  const [rows, setRows] = useState<Array<Record<string, unknown>>>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    fetchArtifactPage(artifactId, name, 0, PAGE_SIZE)
      .then((page) => {
        if (!active) return;
        setRows(page.rows);
        setTotal(page.total);
        setOffset(page.offset);
      })
      .catch((cause) => {
        if (active) setError(cause instanceof Error ? cause.message : '明细加载失败');
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => { active = false; };
  }, [artifactId, name]);

  const goToPage = async (nextOffset: number) => {
    if (loading || nextOffset < 0 || nextOffset >= total) return;
    setLoading(true);
    setError('');
    try {
      const page = await fetchArtifactPage(artifactId, name, nextOffset, PAGE_SIZE);
      setRows(page.rows);
      setTotal(page.total);
      setOffset(page.offset);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '明细加载失败');
    } finally {
      setLoading(false);
    }
  };

  const from = total === 0 ? 0 : offset + 1;
  const to = offset + rows.length;

  return (
    <div className="rounded-xl border border-slate-200 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="font-bold text-slate-800">{title}</h3>
          <p className="mt-1 text-xs text-slate-500">{description}</p>
        </div>
        <span className="text-xs font-mono text-slate-500">共 {total.toLocaleString()} 条</span>
      </div>
      {error && <div className="mt-3 text-sm text-rose-600">{error}</div>}
      <div className="mt-3 overflow-x-auto rounded-lg border border-slate-100">
        <table className="w-full min-w-[680px] text-left text-xs">
          <thead className="bg-slate-50 text-slate-500">
            <tr>
              {columns.map((column) => <th key={column.key} className="whitespace-nowrap px-3 py-2 font-semibold">{column.label}</th>)}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rows.map((row, index) => (
              <tr key={String(row.execution_date || row.date || '') + '-' + String(row.code || '') + '-' + String(offset + index)} className="text-slate-700">
                {columns.map((column) => <td key={column.key} className="whitespace-nowrap px-3 py-2 font-mono">{formatCell(column.key, row[column.key])}</td>)}
              </tr>
            ))}
            {!loading && rows.length === 0 && !error && (
              <tr><td colSpan={columns.length} className="px-3 py-10 text-center text-slate-400">没有明细记录</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
        <span className="text-xs text-slate-500">
          {loading ? '正在加载…' : '显示第 ' + from.toLocaleString() + '–' + to.toLocaleString() + ' 条'}
        </span>
        <div className="flex items-center gap-2">
          <button type="button" onClick={() => void goToPage(offset - PAGE_SIZE)} disabled={loading || offset === 0} className="rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-slate-600 disabled:opacity-40">上一页</button>
          <button type="button" onClick={() => void goToPage(offset + PAGE_SIZE)} disabled={loading || offset + rows.length >= total} className="rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-blue-600 disabled:opacity-40">下一页（最多 {PAGE_SIZE} 条）</button>
        </div>
      </div>
    </div>
  );
}

const TRADE_COLUMNS: Column[] = [
  { key: 'signal_date', label: '信号日期' },
  { key: 'execution_date', label: '成交日期' },
  { key: 'code', label: '股票代码' },
  { key: 'side', label: '方向' },
  { key: 'qty', label: '成交数量' },
  { key: 'price', label: '成交价格' },
  { key: 'fee', label: '费用' },
];

const POSITION_COLUMNS: Column[] = [
  { key: 'date', label: '日期' },
  { key: 'code', label: '股票代码' },
  { key: 'qty', label: '持仓数量' },
  { key: 'cost', label: '持仓成本' },
  { key: 'market_value', label: '持仓市值' },
];

export default function ArtifactDataViewer({
  artifactId,
  resultUri,
}: {
  artifactId: number;
  resultUri?: string | null;
}) {
  return (
    <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5 space-y-4">
      <div>
        <h2 className="font-bold text-slate-800">Artifact 数据</h2>
        <p className="mt-1 text-xs text-slate-400">表格采用分页读取，每次最多 100 条；不会一次性把全部成交和持仓记录载入浏览器。</p>
      </div>
      <div className="flex flex-wrap gap-2">
        {(['equity_curve', 'trades', 'positions_daily'] as const).map((name) => (
          <button
            key={name}
            type="button"
            onClick={() => void downloadArtifact(artifactId, name, { artifact: true })}
            className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50"
          >
            {name === 'equity_curve' ? '下载权益曲线' : name === 'trades' ? '下载成交明细' : '下载持仓明细'}
          </button>
        ))}
      </div>
      {resultUri && <div className="text-xs font-mono text-slate-400 break-all">引用：{resultUri}</div>}
      <EquityCurveChart artifactId={artifactId} />
      <PagedArtifactTable
        artifactId={artifactId}
        name="trades"
        title="成交明细"
        description="每页最多显示 100 条，可翻页查看全部成交。"
        columns={TRADE_COLUMNS}
      />
      <PagedArtifactTable
        artifactId={artifactId}
        name="positions_daily"
        title="持仓明细"
        description="每页最多显示 100 条，可翻页查看全部每日持仓。"
        columns={POSITION_COLUMNS}
      />
    </section>
  );
}

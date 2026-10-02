'use client';

import { useEffect, useMemo, useState } from 'react';

type AtomTrace = {
  atom_id?: string;
  field?: string;
  window?: number | string | null;
  value?: number | null;
  operator?: string | null;
  threshold?: number | null;
  passed?: boolean;
  source?: string;
};

type ExecutionTrace = {
  execution_date?: string;
  price?: number;
  side?: string;
  qty?: number;
  fee?: number;
};

type CodeTrace = {
  code: string;
  passed?: boolean;
  triggered?: boolean;
  targeted?: boolean;
  target_weight?: number | null;
  selection_reason?: string;
  atoms?: AtomTrace[];
  execution?: ExecutionTrace | null;
  executions?: ExecutionTrace[];
};

type SignalTrace = {
  schema_version?: string;
  engine_version?: string;
  signal_date?: string;
  formula?: string;
  traces?: CodeTrace[];
};

const REASON_LABELS: Record<string, string> = {
  FORMULA_REJECTED: '公式未通过',
  TRIGGER_NOT_FIRED: '触发条件未触发',
  TOP_N_EXCLUDED: 'Top-N 排除',
  TARGET_SELECTED: '进入目标组合',
};

function boolLabel(value?: boolean) {
  return value ? '是' : '否';
}

function fmtNumber(value: unknown, digits = 4) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function reasonClass(reason?: string) {
  switch (reason) {
    case 'TARGET_SELECTED':
      return 'bg-emerald-50 text-emerald-700 border-emerald-100';
    case 'FORMULA_REJECTED':
      return 'bg-red-50 text-red-700 border-red-100';
    case 'TRIGGER_NOT_FIRED':
      return 'bg-amber-50 text-amber-700 border-amber-100';
    case 'TOP_N_EXCLUDED':
      return 'bg-slate-100 text-slate-600 border-slate-200';
    default:
      return 'bg-blue-50 text-blue-700 border-blue-100';
  }
}

export default function SignalTracePanel({ artifactId }: { artifactId: number }) {
  const [trace, setTrace] = useState<SignalTrace | null>(null);
  const [loading, setLoading] = useState(true);
  const [available, setAvailable] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const [reason, setReason] = useState('ALL');
  const [page, setPage] = useState(0);

  useEffect(() => {
    if (!artifactId) return;
    let mounted = true;
    setLoading(true);
    setError(null);
    fetch('/api/artifacts/' + artifactId + '/part?name=signal_trace&fmt=json', { cache: 'no-store' })
      .then(async (res) => {
        if (res.status === 404) {
          if (mounted) setAvailable(false);
          return null;
        }
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data?.error || ('SignalTrace HTTP ' + res.status));
        return data as SignalTrace;
      })
      .then((data) => {
        if (!mounted) return;
        if (data) {
          setTrace(data);
          setAvailable(true);
        }
      })
      .catch((e) => {
        if (mounted) setError(e instanceof Error ? e.message : 'SignalTrace 加载失败');
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });
    return () => { mounted = false; };
  }, [artifactId]);

  const rows = useMemo(() => {
    if (Array.isArray(trace?.traces)) return trace.traces;
    if (trace?.traces && typeof trace.traces === 'object') {
      const merged: CodeTrace[] = [];
      for (const value of Object.values(trace.traces as Record<string, any>)) {
        if (value && Array.isArray(value.traces)) merged.push(...value.traces);
      }
      const seen = new Set<string>();
      return merged.filter((row) => {
        if (!row?.code || seen.has(row.code)) return false;
        seen.add(row.code);
        return true;
      });
    }
    return [];
  }, [trace]);

  const summary = useMemo(() => rows.reduce((acc, row) => {
    acc.total += 1;
    if (row.passed) acc.passed += 1;
    if (row.triggered) acc.triggered += 1;
    if (row.targeted) acc.targeted += 1;
    const key = row.selection_reason || 'UNKNOWN';
    acc.reasons[key] = (acc.reasons[key] || 0) + 1;
    return acc;
  }, {
    total: 0,
    passed: 0,
    triggered: 0,
    targeted: 0,
    reasons: {} as Record<string, number>,
  }), [rows]);

  const filteredRows = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return rows.filter((row) => {
      const matchesQuery = !needle
        || row.code.toLowerCase().includes(needle)
        || String(row.selection_reason || '').toLowerCase().includes(needle)
        || (row.atoms || []).some((atom) => String(atom.source || atom.atom_id || '').toLowerCase().includes(needle));
      const matchesReason = reason === 'ALL' || row.selection_reason === reason;
      return matchesQuery && matchesReason;
    });
  }, [rows, query, reason]);

  const pageSize = 50;
  const pageCount = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const visibleRows = filteredRows.slice(page * pageSize, (page + 1) * pageSize);

  useEffect(() => {
    setPage(0);
  }, [query, reason]);

  const download = async () => {
    const res = await fetch('/api/artifacts/' + artifactId + '/part?name=signal_trace&fmt=json', { cache: 'no-store' });
    if (!res.ok) {
      alert('SignalTrace 下载失败');
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'artifact_' + artifactId + '_signal_trace.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
        <h2 className="font-bold text-slate-800">SignalTrace</h2>
        <div className="mt-4 text-sm text-slate-400">加载中...</div>
      </section>
    );
  }

  if (!available) {
    return (
      <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
        <div className="flex items-center justify-between gap-3">
          <h2 className="font-bold text-slate-800">SignalTrace</h2>
          <span className="text-xs px-2.5 py-1 rounded-full bg-slate-100 text-slate-500">未启用</span>
        </div>
        <div className="mt-2 text-xs text-slate-400">本次回测未启用 SignalTrace，历史结果文件中没有该可选产物。</div>
      </section>
    );
  }

  if (error || !trace) {
    return (
      <section className="bg-white rounded-2xl border border-red-100 shadow-sm p-5">
        <h2 className="font-bold text-slate-800">SignalTrace</h2>
        <div className="mt-2 text-sm text-red-600">{error || 'SignalTrace 不可用'}</div>
      </section>
    );
  }

  return (
    <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="font-bold text-slate-800">SignalTrace</h2>
          <div className="mt-1 text-xs text-slate-400">
            PIT 候选、公式结果、触发、Top-N/目标组合与执行证据
          </div>
        </div>
        <button
          type="button"
          onClick={() => void download()}
          className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50"
        >
          下载 SignalTrace JSON
        </button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mt-4">
        <div className="rounded-xl bg-slate-50 p-3"><div className="text-xs text-slate-400">候选数</div><div className="font-mono font-bold mt-1">{summary.total}</div></div>
        <div className="rounded-xl bg-slate-50 p-3"><div className="text-xs text-slate-400">公式通过</div><div className="font-mono font-bold mt-1">{summary.passed}</div></div>
        <div className="rounded-xl bg-slate-50 p-3"><div className="text-xs text-slate-400">已触发</div><div className="font-mono font-bold mt-1">{summary.triggered}</div></div>
        <div className="rounded-xl bg-slate-50 p-3"><div className="text-xs text-slate-400">进入目标组合</div><div className="font-mono font-bold mt-1">{summary.targeted}</div></div>
        <div className="rounded-xl bg-slate-50 p-3"><div className="text-xs text-slate-400">日期</div><div className="font-mono font-bold mt-1">{trace.signal_date || '—'}</div></div>
      </div>

      <div className="mt-4 rounded-xl border border-blue-100 bg-blue-50/50 p-3">
        <div className="text-xs font-bold text-blue-700">Trace 元数据</div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2 text-xs">
          <div><span className="text-slate-400">Schema：</span><span className="font-mono">{trace.schema_version || '—'}</span></div>
          <div><span className="text-slate-400">Engine：</span><span className="font-mono">{trace.engine_version || '—'}</span></div>
          <div className="md:col-span-3"><span className="text-slate-400">Formula：</span><span className="font-mono break-all">{trace.formula || '—'}</span></div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="搜索代码 / 原子 / 原因"
          className="min-w-[220px] flex-1 px-3 py-2 rounded-lg border border-slate-200 text-xs bg-white"
        />
        <select value={reason} onChange={(e) => setReason(e.target.value)} className="px-3 py-2 rounded-lg border border-slate-200 text-xs bg-white">
          <option value="ALL">全部原因</option>
          {Object.entries(REASON_LABELS).map(([key, label]) => (
            <option key={key} value={key}>{label} · {summary.reasons[key] || 0}</option>
          ))}
        </select>
      </div>

      <div className="mt-3 overflow-x-auto">
        {visibleRows.length === 0 ? (
          <div className="py-8 text-center text-sm text-slate-400">没有符合条件的 Trace</div>
        ) : (
          <div className="space-y-2 min-w-[760px]">
            {visibleRows.map((row) => (
              <details key={row.code + ':' + (row.selection_reason || '')} className="rounded-xl border border-slate-200">
                <summary className="cursor-pointer list-none px-3 py-3">
                  <div className="grid grid-cols-[130px_80px_80px_80px_120px_1fr] gap-2 items-center text-xs">
                    <div className="font-mono font-bold text-slate-800">{row.code}</div>
                    <div><span className="text-slate-400">公式</span> {boolLabel(row.passed)}</div>
                    <div><span className="text-slate-400">触发</span> {boolLabel(row.triggered)}</div>
                    <div><span className="text-slate-400">目标</span> {boolLabel(row.targeted)}</div>
                    <div className={'px-2 py-1 rounded-md border ' + reasonClass(row.selection_reason)}>
                      {REASON_LABELS[row.selection_reason || ''] || row.selection_reason || '—'}
                    </div>
                    <div className="text-right font-mono text-slate-500">
                      权重 {row.target_weight == null ? '—' : fmtNumber(Number(row.target_weight) * 100, 2) + '%'}
                    </div>
                  </div>
                </summary>

                <div className="border-t border-slate-100 p-3 space-y-3">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                    <div><div className="text-slate-400">passed</div><div className="font-mono mt-1">{String(row.passed ?? false)}</div></div>
                    <div><div className="text-slate-400">triggered</div><div className="font-mono mt-1">{String(row.triggered ?? false)}</div></div>
                    <div><div className="text-slate-400">targeted</div><div className="font-mono mt-1">{String(row.targeted ?? false)}</div></div>
                    <div><div className="text-slate-400">target_weight</div><div className="font-mono mt-1">{row.target_weight == null ? 'null' : fmtNumber(row.target_weight, 6)}</div></div>
                  </div>

                  {Array.isArray(row.atoms) && row.atoms.length > 0 && (
                    <div>
                      <div className="text-xs font-bold text-slate-700 mb-2">公式原子</div>
                      <div className="space-y-1.5">
                        {row.atoms.map((atom, index) => (
                          <div key={(atom.atom_id || 'atom') + ':' + index} className="grid grid-cols-[150px_90px_70px_1fr_80px] gap-2 px-3 py-2 rounded-lg bg-slate-50 text-xs">
                            <div className="font-mono">{atom.atom_id || '—'}</div>
                            <div>{atom.field || '—'}{atom.window != null ? '(' + atom.window + ')' : ''}</div>
                            <div className="font-mono">{atom.operator || '—'}</div>
                            <div className="font-mono break-all">
                              {fmtNumber(atom.value, 6)}{atom.threshold != null ? ' vs ' + fmtNumber(atom.threshold, 6) : ''}
                              {atom.source ? <div className="text-[10px] text-slate-400 mt-0.5">{atom.source}</div> : null}
                            </div>
                            <div className={atom.passed ? 'text-emerald-600 font-bold' : 'text-red-600 font-bold'}>
                              {atom.passed ? 'PASS' : 'FAIL'}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {row.execution && (
                    <div>
                      <div className="text-xs font-bold text-slate-700 mb-2">执行</div>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs rounded-lg bg-slate-50 p-3">
                        <div><span className="text-slate-400">日期</span><div className="font-mono mt-1">{row.execution.execution_date || '—'}</div></div>
                        <div><span className="text-slate-400">方向</span><div className="font-mono mt-1">{row.execution.side || '—'}</div></div>
                        <div><span className="text-slate-400">价格</span><div className="font-mono mt-1">{fmtNumber(row.execution.price, 4)}</div></div>
                        <div><span className="text-slate-400">数量</span><div className="font-mono mt-1">{fmtNumber(row.execution.qty, 0)}</div></div>
                        <div><span className="text-slate-400">费用</span><div className="font-mono mt-1">{fmtNumber(row.execution.fee, 4)}</div></div>
                      </div>
                    </div>
                  )}

                  {Array.isArray(row.executions) && row.executions.length > 0 && (
                    <div>
                      <div className="text-xs font-bold text-slate-700 mb-2">全部执行记录</div>
                      <div className="space-y-1">
                        {row.executions.map((exec, index) => (
                          <div key={index} className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs rounded-lg bg-slate-50 p-2">
                            <div>{exec.execution_date || '—'}</div>
                            <div>{exec.side || '—'}</div>
                            <div className="font-mono">{fmtNumber(exec.price, 4)}</div>
                            <div className="font-mono">{fmtNumber(exec.qty, 0)}</div>
                            <div className="font-mono">{fmtNumber(exec.fee, 4)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </details>
            ))}
          </div>
        )}
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 mt-4 text-xs text-slate-400">
        <span>显示 {filteredRows.length === 0 ? 0 : page * pageSize + 1}–{Math.min((page + 1) * pageSize, filteredRows.length)} / {filteredRows.length} 条（候选全集共 {rows.length} 条）</span>
        <div className="flex items-center gap-2">
          <button type="button" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))} className="px-2.5 py-1.5 rounded-lg border border-slate-200 disabled:opacity-40">上一页</button>
          <span>{page + 1} / {pageCount}</span>
          <button type="button" disabled={page >= pageCount - 1} onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))} className="px-2.5 py-1.5 rounded-lg border border-slate-200 disabled:opacity-40">下一页</button>
        </div>
      </div>
    </section>
  );
}

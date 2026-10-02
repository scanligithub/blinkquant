'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import type { Task } from '@/hooks/useCluster';

type ArtifactKind = 'all' | 'selection' | 'backtest';

const TYPE_LABELS: Record<string, string> = { selection: '选股成果', backtest: '回测成果' };

function fmtTime(value?: string | null) {
  return value ? new Date(value).toLocaleString() : '—';
}

function selectionMeta(task: Task) {
  const payload = (task as any).payload || {};
  const source = payload.selection_strategy_snapshot;
  return {
    formula: String(payload.formula || ''),
    timeframe: String(payload.timeframe || 'D'),
    date: payload.date ? String(payload.date) : '最近可用交易日',
    source: source && source.id ? source : null,
  };
}

function backtestMeta(task: Task) {
  const payload = (task as any).payload || {};
  const strategy = payload.strategy || {};
  const entry = strategy.entry || {};
  const source = payload.source_selection_strategy;
  return {
    formula: String(entry.condition || ''),
    timeframe: String(entry.timeframe || 'D'),
    start: payload.start_date ? String(payload.start_date) : '—',
    end: payload.end_signal_date ? String(payload.end_signal_date) : '—',
    source: source && source.id ? source : null,
  };
}

export default function ArtifactLibraryPage({ kind = 'all' }: { kind?: ArtifactKind }) {
  const router = useRouter();
  const fileRef = useRef<HTMLInputElement>(null);
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(0);
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const pageSize = 20;
  const [storage, setStorage] = useState<{ usedBytes: number; quotaBytes: number; total: number; filteredTotal: number; selectionTotal: number; backtestTotal: number }>({ usedBytes: 0, quotaBytes: 0, total: 0, filteredTotal: 0, selectionTotal: 0, backtestTotal: 0 });

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const query = new URLSearchParams({ limit: String(pageSize), offset: String(page * pageSize) });
      if (kind !== 'all') query.set('artifact_type', kind);
      if (search) query.set('q', search);
      const res = await fetch('/api/artifacts?' + query.toString(), { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载成果失败');
      const done = Array.isArray(json?.artifacts) ? json.artifacts : [];
      setTasks(done);
      setStorage({
        usedBytes: Number(json?.global_used_bytes ?? json?.used_bytes ?? 0),
        quotaBytes: Number(json?.quota_bytes || 0),
        total: Number(json?.global_total ?? json?.total ?? done.length),
        filteredTotal: Number(json?.total || 0),
        selectionTotal: Number(json?.selection_total || 0),
        backtestTotal: Number(json?.backtest_total || 0),
      });
    } catch (e) {
      console.error('load artifacts failed', e);
      setTasks([]);
      setStorage({ usedBytes: 0, quotaBytes: 0, total: 0, filteredTotal: 0, selectionTotal: 0, backtestTotal: 0 });
    } finally {
      setLoading(false);
    }
  }, [kind, page, pageSize, search]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (json.user) {
          setUser(json.user);
          setAuthLoading(false);
        } else {
          router.replace('/login');
        }
      } catch {
        if (mounted) router.replace('/login');
      }
    })();
    return () => { mounted = false; };
  }, [router]);

  useEffect(() => { if (user) void load(); }, [user, load]);
  useEffect(() => {
    if (!user) return;
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [user, load]);

  useEffect(() => {
    setPage(0);
  }, [kind, search]);

  useEffect(() => {
    const lastPage = Math.max(0, Math.ceil(storage.filteredTotal / pageSize) - 1);
    if (page > lastPage) setPage(lastPage);
  }, [page, storage.filteredTotal, pageSize]);


  const exportArtifact = async (artifactId: number) => {
    try {
      const res = await fetch('/api/artifacts/' + artifactId + '/export', { cache: 'no-store' });
      if (!res.ok) throw new Error('导出失败');
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = \`blinkquant_artifact_\${artifactId}.zip\`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      alert(error instanceof Error ? error.message : '导出失败');
    }
  };

  const importArtifact = async (file: File) => {
    try {
      const form = new FormData();
      form.set('file', file);
      const res = await fetch('/api/artifacts/import', { method: 'POST', body: form });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '导入失败');
      await load();
      alert(\`已导入成果：\${json.title || '新成果'}\`);
    } catch (error) {
      alert(error instanceof Error ? error.message : '导入失败');
    } finally {
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const filtered = tasks;
  const pageCount = Math.max(1, Math.ceil(storage.filteredTotal / pageSize));
  const selectionCount = storage.selectionTotal;
  const backtestCount = storage.backtestTotal;

  const storagePct = storage.quotaBytes > 0 ? Math.min(100, (storage.usedBytes / storage.quotaBytes) * 100) : 0;
  const fmtBytes = (n: number) => n < 1024 * 1024
    ? Math.round(n / 1024) + ' KB'
    : n < 1024 * 1024 * 1024
      ? (n / 1024 / 1024).toFixed(1) + ' MB'
      : (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';

  const tabs: Array<{ key: ArtifactKind; label: string; count?: number }> = [
    { key: 'all', label: '全部成果', count: tasks.length },
    { key: 'selection', label: '选股成果', count: selectionCount },
    { key: 'backtest', label: '回测成果', count: backtestCount },
  ];

  return (
    <AppShell user={user} onLogout={async () => {
      await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
      router.replace('/login');
    }}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-7xl mx-auto space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-black">成果库</h1>
              <p className="text-sm text-slate-500 mt-1">按历史任务浏览已完成的选股与回测成果；成果详情保留任务输入和策略版本快照。</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <input ref={fileRef} type="file" accept=".zip,application/zip" className="hidden" onChange={(e) => { const file = e.target.files?.[0]; if (file) void importArtifact(file); }} />
              <button type="button" onClick={() => fileRef.current?.click()} className="px-3 py-2 rounded-xl bg-white border border-slate-200 text-sm font-bold text-slate-600 hover:bg-slate-50">导入成果</button>
              <button type="button" onClick={() => void load()} className="px-3 py-2 rounded-xl bg-white border border-slate-200 text-sm font-bold text-slate-600 hover:bg-slate-50">刷新</button>
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <Link href="/artifacts" className="bg-white rounded-2xl border border-slate-200 p-4 hover:border-blue-200">
              <div className="text-xs text-slate-400">全部成果</div>
              <div className="text-2xl font-black mt-1">{storage.total}</div>
            </Link>
            <Link href="/artifacts/selections" className="bg-white rounded-2xl border border-slate-200 p-4 hover:border-blue-200">
              <div className="text-xs text-slate-400">选股成果</div>
              <div className="text-2xl font-black mt-1">{selectionCount}</div>
            </Link>
            <Link href="/artifacts/backtests" className="bg-white rounded-2xl border border-slate-200 p-4 hover:border-blue-200">
              <div className="text-xs text-slate-400">回测成果</div>
              <div className="text-2xl font-black mt-1">{backtestCount}</div>
            </Link>
          </div>

          <div className="bg-white rounded-2xl border border-slate-200 p-4">
            <div className="flex items-center justify-between text-xs text-slate-500">
              <span>成果存储</span><span>{fmtBytes(storage.usedBytes)} / {fmtBytes(storage.quotaBytes)}</span>
            </div>
            <div className="mt-2 h-2 rounded-full bg-slate-100 overflow-hidden">
              <div className="h-full bg-blue-600" style={{ width: storagePct + '%' }} />
            </div>
            <div className="mt-2 text-xs text-slate-400">{storage.total} 个成果；已删除的成果不占用成果库容量。</div>
          </div>

          <section className="bg-white rounded-2xl border border-slate-200 p-4">
            <form
              onSubmit={(e) => { e.preventDefault(); setPage(0); setSearch(searchInput.trim()); }}
              className="flex flex-col sm:flex-row gap-2"
            >
              <input
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                placeholder="搜索成果名称、公式或来源策略…"
                className="flex-1 px-3 py-2 rounded-xl border border-slate-200 bg-white text-sm outline-none focus:border-blue-400"
              />
              <button type="submit" className="px-4 py-2 rounded-xl bg-slate-900 text-white text-sm font-bold">搜索</button>
              {search && <button type="button" onClick={() => { setSearchInput(''); setSearch(''); setPage(0); }} className="px-4 py-2 rounded-xl border border-slate-200 text-sm font-bold text-slate-600">清除</button>}
            </form>
            {search && <div className="mt-2 text-xs text-slate-400">当前搜索：{search} · 统计卡片仍显示用户全部成果数量。</div>}
          </section>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex flex-wrap items-center gap-2">
              {tabs.map(tab => (
                <Link
                  key={tab.key}
                  href={tab.key === 'all' ? '/artifacts' : tab.key === 'selection' ? '/artifacts/selections' : '/artifacts/backtests'}
                  className={`px-3 py-1.5 text-xs font-bold rounded-lg ${kind === tab.key ? 'bg-blue-600 text-white' : 'bg-slate-100 text-slate-600'}`}
                >
                  {tab.label}{typeof tab.count === 'number' ? ' · ' + tab.count : ''}
                </Link>
              ))}
            </div>

            {loading ? <div className="p-12 text-center text-slate-400">加载中...</div> : filtered.length === 0 ? (
              <div className="p-12 text-center text-slate-400">暂无已完成的{kind === 'selection' ? '选股' : kind === 'backtest' ? '回测' : ''}成果</div>
            ) : (
              <div className="divide-y divide-slate-100">
                {filtered.map(task => {
                  if (task.task_type === 'selection') {
                    const meta = selectionMeta(task);
                    return (
                      <div key={task.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                        <div className="flex-1 min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <Link href={'/artifacts/selections/' + task.id} className="font-bold text-slate-800 hover:text-blue-600">{(task as any).title || ('选股成果 #' + task.id)}</Link>
                            <span className="text-[10px] px-2 py-1 rounded-full bg-blue-50 text-blue-700">选股成果</span>
                            {meta.source && <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-600">策略 v{meta.source.version_no}</span>}
                          </div>
                          <div className="text-xs font-mono text-slate-500 mt-2 break-all">{meta.formula || '—'}</div>
                          <div className="text-xs text-slate-400 mt-2">周期：{meta.timeframe} · 日期：{meta.date} · 完成：{fmtTime(task.finished_at)}</div>
                          {meta.source && <div className="text-xs text-blue-600 mt-1">来源策略：{meta.source.name} · v{meta.source.version_no}</div>}
                        </div>
                        <div className="flex gap-2 shrink-0">
                          <Link href={'/artifacts/selections/' + task.id} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">查看成果</Link>
                          <button type="button" onClick={() => void exportArtifact(Number(task.id))} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">导出</button>
                          <button type="button" onClick={async () => {
                            const title = window.prompt('成果名称', (task as any).title || ('选股成果 #' + task.id));
                            if (!title || !title.trim()) return;
                            const res = await fetch('/api/artifacts/' + task.id, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: title.trim() }) });
                            if (res.ok) void load();
                          }} className="px-3 py-2 text-xs font-bold text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50">重命名</button>
                          <button type="button" onClick={async () => {
                            if (!window.confirm('删除该成果？此操作不会删除原任务。')) return;
                            const res = await fetch('/api/artifacts/' + task.id, { method: 'DELETE' });
                            if (res.ok) void load(); else alert('删除失败');
                          }} className="px-3 py-2 text-xs font-bold text-red-600 border border-red-200 rounded-lg hover:bg-red-50">删除</button>
                        </div>
                      </div>
                    );
                  }

                  const meta = backtestMeta(task);
                  return (
                    <div key={task.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link href={'/artifacts/backtests/' + task.id} className="font-bold text-slate-800 hover:text-blue-600">
                            {(task as any).title || task.strategy_template_name || ('回测成果 #' + task.id)}
                          </Link>
                          <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700">回测成果</span>
                          {task.strategy_template_version && <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-600">策略 v{task.strategy_template_version}</span>}
                        </div>
                        <div className="text-xs font-mono text-slate-500 mt-2 break-all">{meta.formula || '—'}</div>
                        <div className="text-xs text-slate-400 mt-2">区间：{meta.start} → {meta.end} · 完成：{fmtTime(task.finished_at)}</div>
                        {task.result_summary && (
                          <div className="text-xs text-slate-600 mt-1">
                            {task.result_summary.total_return != null && <>收益 {(task.result_summary.total_return * 100).toFixed(2)}%</>}
                            {task.result_summary.max_drawdown != null && <> · 最大回撤 {(task.result_summary.max_drawdown * 100).toFixed(2)}%</>}
                            {task.result_summary.n_trades != null && <> · {task.result_summary.n_trades} 笔</>}
                          </div>
                        )}
                        {meta.source && <div className="text-xs text-blue-600 mt-1">基础选股策略：{meta.source.name} · v{meta.source.version_no}</div>}
                      </div>
                      <div className="flex gap-2 shrink-0">
                        <Link href={'/artifacts/backtests/' + task.id} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">查看成果</Link>
                        <button type="button" onClick={async () => {
                          const title = window.prompt('成果名称', (task as any).title || (task.strategy_template_name || ('回测成果 #' + task.id)));
                          if (!title || !title.trim()) return;
                          const res = await fetch('/api/artifacts/' + task.id, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: title.trim() }) });
                          if (res.ok) void load();
                        }} className="px-3 py-2 text-xs font-bold text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50">重命名</button>
                        <button type="button" onClick={async () => {
                          if (!window.confirm('删除该成果？此操作不会删除原任务。')) return;
                          const res = await fetch('/api/artifacts/' + task.id, { method: 'DELETE' });
                          if (res.ok) void load(); else alert('删除失败');
                        }} className="px-3 py-2 text-xs font-bold text-red-600 border border-red-200 rounded-lg hover:bg-red-50">删除</button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
            {!loading && storage.filteredTotal > 0 && (
              <div className="px-5 py-4 border-t border-slate-100 flex flex-wrap items-center justify-between gap-3">
                <div className="text-xs text-slate-500">
                  共 {storage.filteredTotal} 条 · 第 {page + 1}/{pageCount} 页 · 每页 {pageSize} 条
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setPage(current => Math.max(0, current - 1))}
                    disabled={page <= 0}
                    className="px-3 py-2 text-xs font-bold rounded-lg border border-slate-200 disabled:opacity-40"
                  >上一页</button>
                  <button
                    type="button"
                    onClick={() => setPage(current => Math.min(pageCount - 1, current + 1))}
                    disabled={page >= pageCount - 1}
                    className="px-3 py-2 text-xs font-bold rounded-lg border border-slate-200 disabled:opacity-40"
                  >下一页</button>
                </div>
              </div>
            )}
          </section>
        </div>
      </main>
    </AppShell>
  );
}

'use client';

import { useCallback, useEffect, useState } from 'react';
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
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/v1/tasks', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载成果失败');
      const done = (Array.isArray(json?.tasks) ? json.tasks : [])
        .filter((task: Task) => task.status === 'done' && (task.task_type === 'selection' || task.task_type === 'backtest'));
      setTasks(done);
    } catch (e) {
      console.error('load artifacts failed', e);
      setTasks([]);
    } finally {
      setLoading(false);
    }
  }, []);

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

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const filtered = kind === 'all' ? tasks : tasks.filter(task => task.task_type === kind);
  const selectionCount = tasks.filter(t => t.task_type === 'selection').length;
  const backtestCount = tasks.filter(t => t.task_type === 'backtest').length;

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
            <button type="button" onClick={() => void load()} className="px-3 py-2 rounded-xl bg-white border border-slate-200 text-sm font-bold text-slate-600 hover:bg-slate-50">刷新</button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <Link href="/artifacts" className="bg-white rounded-2xl border border-slate-200 p-4 hover:border-blue-200">
              <div className="text-xs text-slate-400">全部成果</div>
              <div className="text-2xl font-black mt-1">{tasks.length}</div>
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
                            <Link href={'/artifacts/selections/' + task.id} className="font-bold text-slate-800 hover:text-blue-600">选股成果 #{task.id}</Link>
                            <span className="text-[10px] px-2 py-1 rounded-full bg-blue-50 text-blue-700">选股成果</span>
                            {meta.source && <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-600">策略 v{meta.source.version_no}</span>}
                          </div>
                          <div className="text-xs font-mono text-slate-500 mt-2 break-all">{meta.formula || '—'}</div>
                          <div className="text-xs text-slate-400 mt-2">周期：{meta.timeframe} · 日期：{meta.date} · 完成：{fmtTime(task.finished_at)}</div>
                          {meta.source && <div className="text-xs text-blue-600 mt-1">来源策略：{meta.source.name} · v{meta.source.version_no}</div>}
                        </div>
                        <Link href={'/artifacts/selections/' + task.id} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 shrink-0">查看成果</Link>
                      </div>
                    );
                  }

                  const meta = backtestMeta(task);
                  return (
                    <div key={task.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link href={'/artifacts/backtests/' + task.id} className="font-bold text-slate-800 hover:text-blue-600">
                            {task.strategy_template_name || ('回测成果 #' + task.id)}
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
                      <Link href={'/artifacts/backtests/' + task.id} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 shrink-0">查看成果</Link>
                    </div>
                  );
                })}
              </div>
            )}
          </section>
        </div>
      </main>
    </AppShell>
  );
}

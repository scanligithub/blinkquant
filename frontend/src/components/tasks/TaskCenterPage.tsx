'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import type { Task } from '@/hooks/useCluster';

const STATUS_LABELS: Record<string, string> = {
  pending: '等待中',
  queued: '排队中',
  running: '运行中',
  done: '完成',
  failed: '失败',
  cancelled: '已取消',
  preempted: '被抢占',
};

const STATUS_CLASSES: Record<string, string> = {
  pending: 'bg-slate-100 text-slate-700',
  queued: 'bg-blue-100 text-blue-700',
  running: 'bg-green-100 text-green-700',
  done: 'bg-emerald-100 text-emerald-700',
  failed: 'bg-red-100 text-red-700',
  cancelled: 'bg-slate-100 text-slate-500',
  preempted: 'bg-orange-100 text-orange-700',
};

const TYPE_LABELS: Record<string, string> = {
  selection: '选股',
  backtest: '回测',
};

function taskTitle(task: Task) {
  if (task.strategy_template_name) return task.strategy_template_name;
  if (task.task_type === 'selection') return '选股任务 #' + task.id;
  return '回测任务 #' + task.id;
}

function fmtTime(value: string | null) {
  return value ? new Date(value).toLocaleString() : '—';
}

export default function TaskCenterPage() {
  const router = useRouter();
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [status, setStatus] = useState('all');
  const [type, setType] = useState('all');
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<number | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/v1/tasks', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载任务失败');
      setTasks(Array.isArray(json?.tasks) ? json.tasks : []);
    } catch (e) {
      console.error('load tasks failed', e);
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

  useEffect(() => {
    if (user) void load();
  }, [user, load]);

  useEffect(() => {
    if (!user) return;
    const id = window.setInterval(() => void load(), 3000);
    return () => window.clearInterval(id);
  }, [user, load]);

  const cancel = async (taskId: number) => {
    setBusyId(taskId);
    try {
      const res = await fetch('/api/v1/tasks/' + taskId, { method: 'POST' });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '取消失败');
      await load();
    } catch (e) {
      alert(e instanceof Error ? e.message : '取消失败');
    } finally {
      setBusyId(null);
    }
  };

  const rerun = async (taskId: number) => {
    setBusyId(taskId);
    try {
      const res = await fetch('/api/v1/tasks/' + taskId + '/rerun', { method: 'POST' });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '重跑失败');
      await load();
      if (json.task_id) router.push('/tasks/' + json.task_id);
    } catch (e) {
      alert(e instanceof Error ? e.message : '重跑失败');
    } finally {
      setBusyId(null);
    }
  };

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const filtered = tasks.filter((task) => {
    if (status !== 'all' && task.status !== status) return false;
    if (type !== 'all' && task.task_type !== type) return false;
    return true;
  });

  const runningCount = tasks.filter(t => t.status === 'running').length;
  const queuedCount = tasks.filter(t => ['pending', 'queued'].includes(t.status)).length;
  const doneCount = tasks.filter(t => t.status === 'done').length;

  return (
    <AppShell user={user} onLogout={async () => {
      await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
      router.replace('/login');
    }}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-7xl mx-auto space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-black">任务中心</h1>
              <p className="text-sm text-slate-500 mt-1">统一查看选股与回测任务的执行状态、进度、节点和历史输入。</p>
            </div>
            <button type="button" onClick={() => void load()} className="px-3 py-2 rounded-xl bg-white border border-slate-200 text-sm font-bold text-slate-600 hover:bg-slate-50">刷新</button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div className="bg-white rounded-2xl border border-slate-200 p-4"><div className="text-xs text-slate-400">运行中</div><div className="text-2xl font-black text-green-600 mt-1">{runningCount}</div></div>
            <div className="bg-white rounded-2xl border border-slate-200 p-4"><div className="text-xs text-slate-400">等待/排队</div><div className="text-2xl font-black text-blue-600 mt-1">{queuedCount}</div></div>
            <div className="bg-white rounded-2xl border border-slate-200 p-4"><div className="text-xs text-slate-400">已完成</div><div className="text-2xl font-black text-emerald-600 mt-1">{doneCount}</div></div>
          </div>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex flex-wrap items-center gap-2">
              {['all', 'selection', 'backtest'].map(v => (
                <button key={v} type="button" onClick={() => setType(v)} className={`px-3 py-1.5 text-xs font-bold rounded-lg ${type === v ? 'bg-blue-600 text-white' : 'bg-slate-100 text-slate-600'}`}>
                  {v === 'all' ? '全部' : TYPE_LABELS[v]}
                </button>
              ))}
              <span className="mx-1 w-px h-5 bg-slate-200" />
              <select value={status} onChange={e => setStatus(e.target.value)} className="px-3 py-1.5 text-xs rounded-lg border border-slate-200 bg-white">
                <option value="all">全部状态</option>
                {Object.entries(STATUS_LABELS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
              </select>
              <span className="ml-auto text-xs text-slate-400">{filtered.length} 个任务</span>
            </div>

            {loading ? <div className="p-12 text-center text-slate-400">加载中...</div> : filtered.length === 0 ? (
              <div className="p-12 text-center text-slate-400">暂无符合条件的任务</div>
            ) : (
              <div className="divide-y divide-slate-100">
                {filtered.map(task => (
                  <div key={task.id} className="p-5 hover:bg-slate-50/70">
                    <div className="flex flex-col lg:flex-row lg:items-center gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link href={'/tasks/' + task.id} className="font-bold text-slate-800 hover:text-blue-600">{taskTitle(task)}</Link>
                          <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-500">{TYPE_LABELS[task.task_type] || task.task_type}</span>
                          <span className={`text-[10px] px-2 py-1 rounded-full font-bold ${STATUS_CLASSES[task.status] || 'bg-slate-100 text-slate-600'}`}>{STATUS_LABELS[task.status] || task.status}</span>
                          {task.strategy_template_version && <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700">策略 v{task.strategy_template_version}</span>}
                        </div>

                        <div className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-xs text-slate-500">
                          <span>任务 #{task.id}</span>
                          <span>节点：{task.assigned_node || '—'}</span>
                          <span>创建：{fmtTime(task.created_at)}</span>
                          {task.started_at && <span>开始：{fmtTime(task.started_at)}</span>}
                          {task.finished_at && <span>结束：{fmtTime(task.finished_at)}</span>}
                        </div>

                        {task.status === 'running' && (
                          <div className="mt-3 max-w-xl">
                            <div className="flex justify-between text-[11px] text-slate-500 mb-1">
                              <span>{task.progress?.current_date ? '算至 ' + task.progress.current_date : (task.progress?.stage || '计算中…')}</span>
                              <span>{typeof task.progress_pct === 'number' ? task.progress_pct.toFixed(0) + '%' : '—'}</span>
                            </div>
                            <div className="h-2 rounded-full bg-slate-100 overflow-hidden">
                              <div className="h-full bg-blue-500" style={{ width: `${Math.min(100, Math.max(0, task.progress_pct ?? 0))}%` }} />
                            </div>
                          </div>
                        )}

                        {task.task_type === 'backtest' && task.result_summary && (
                          <div className="mt-2 text-xs text-slate-500">
                            {task.result_summary.total_return != null && <>收益 {(task.result_summary.total_return * 100).toFixed(2)}%</>}
                            {task.result_summary.max_drawdown != null && <> · 回撤 {(task.result_summary.max_drawdown * 100).toFixed(2)}%</>}
                            {task.result_summary.n_trades != null && <> · {task.result_summary.n_trades} 笔</>}
                          </div>
                        )}

                        {task.error && <div className="mt-2 text-xs text-red-600 break-all">{task.error}</div>}
                      </div>

                      <div className="flex flex-wrap items-center gap-2 shrink-0">
                        <Link href={'/tasks/' + task.id} className="px-3 py-2 text-xs font-bold border border-slate-200 rounded-lg text-slate-600 hover:bg-white">详情</Link>
                        {['pending', 'queued', 'running'].includes(task.status) && (
                          <button type="button" disabled={busyId === task.id} onClick={() => void cancel(task.id)} className="px-3 py-2 text-xs font-bold border border-red-200 rounded-lg text-red-600 hover:bg-red-50 disabled:opacity-50">取消</button>
                        )}
                        {task.task_type === 'backtest' && ['done', 'failed', 'cancelled', 'preempted'].includes(task.status) && (
                          <button type="button" disabled={busyId === task.id} onClick={() => void rerun(task.id)} className="px-3 py-2 text-xs font-bold border border-blue-200 rounded-lg text-blue-600 hover:bg-blue-50 disabled:opacity-50">重跑</button>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>
        </div>
      </main>
    </AppShell>
  );
}

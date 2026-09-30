'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import type { Task } from '@/hooks/useCluster';

const STATUS_LABELS: Record<string, string> = {
  pending: '等待中', queued: '排队中', running: '运行中', done: '完成',
  failed: '失败', cancelled: '已取消', preempted: '被抢占',
};

export default function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const router = useRouter();
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [task, setTask] = useState<Task | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const session = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await session.json();
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
    if (!user || !taskId) return;
    let mounted = true;
    const load = async () => {
      try {
        const res = await fetch('/api/v1/tasks/' + taskId, { cache: 'no-store' });
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || '加载任务失败');
        if (mounted) setTask(json);
      } catch (e) {
        console.error('load task detail failed', e);
        if (mounted) setTask(null);
      } finally {
        if (mounted) setLoading(false);
      }
    };
    void load();
    const interval = window.setInterval(load, 3000);
    return () => { mounted = false; window.clearInterval(interval); };
  }, [user, taskId]);

  const logout = async () => {
    await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
    router.replace('/login');
  };

  if (authLoading) return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;

  return (
    <AppShell user={user} onLogout={logout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-5xl mx-auto space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <Link href="/tasks" className="text-sm text-blue-600 hover:underline">← 返回任务中心</Link>
              <h1 className="text-2xl font-black mt-2">任务详情 {task ? '#' + task.id : ''}</h1>
            </div>
            {task && <span className="px-3 py-1.5 rounded-full bg-slate-100 text-slate-600 text-xs font-bold">{STATUS_LABELS[task.status] || task.status}</span>}
          </div>

          {loading ? <div className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-slate-400">加载中...</div> : !task ? (
            <div className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-red-500">任务不存在或无权访问</div>
          ) : (
            <>
              <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                <h2 className="font-bold text-slate-800">执行信息</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-sm">
                  <div><div className="text-xs text-slate-400">类型</div><div className="font-semibold mt-1">{task.task_type === 'selection' ? '选股' : '回测'}</div></div>
                  <div><div className="text-xs text-slate-400">状态</div><div className="font-semibold mt-1">{STATUS_LABELS[task.status] || task.status}</div></div>
                  <div><div className="text-xs text-slate-400">节点</div><div className="font-mono mt-1">{task.assigned_node || '—'}</div></div>
                  <div><div className="text-xs text-slate-400">优先级</div><div className="font-mono mt-1">{(task as any).priority ?? '—'}</div></div>
                  <div><div className="text-xs text-slate-400">创建时间</div><div className="mt-1">{task.created_at ? new Date(task.created_at).toLocaleString() : '—'}</div></div>
                  <div><div className="text-xs text-slate-400">开始时间</div><div className="mt-1">{task.started_at ? new Date(task.started_at).toLocaleString() : '—'}</div></div>
                  <div><div className="text-xs text-slate-400">结束时间</div><div className="mt-1">{task.finished_at ? new Date(task.finished_at).toLocaleString() : '—'}</div></div>
                  <div><div className="text-xs text-slate-400">来源任务</div><div className="mt-1">{task.source_task_id != null ? '#' + task.source_task_id : '—'}</div></div>
                </div>
              </section>

              {task.progress && (
                <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                  <h2 className="font-bold text-slate-800">执行进度</h2>
                  <div className="mt-4">
                    <div className="flex justify-between text-xs text-slate-500">
                      <span>{task.progress.current_date ? '算至 ' + task.progress.current_date : (task.progress.stage || '计算中')}</span>
                      <span>{typeof task.progress_pct === 'number' ? task.progress_pct.toFixed(1) + '%' : '—'}</span>
                    </div>
                    <div className="h-2 mt-2 bg-slate-100 rounded-full overflow-hidden"><div className="h-full bg-blue-500" style={{width: `${Math.min(100, Math.max(0, task.progress_pct ?? 0))}%`}} /></div>
                    <div className="mt-2 text-xs text-slate-400">{task.progress.done_days ?? 0} / {task.progress.total_days ?? 0} 天</div>
                  </div>
                </section>
              )}

              {task.strategy_template_id && (
                <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                  <h2 className="font-bold text-slate-800">策略版本绑定</h2>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-sm">
                    <div><div className="text-xs text-slate-400">策略</div><div className="font-semibold mt-1">{task.strategy_template_name || ('#' + task.strategy_template_id)}</div></div>
                    <div><div className="text-xs text-slate-400">策略 ID</div><div className="font-mono mt-1">#{task.strategy_template_id}</div></div>
                    <div><div className="text-xs text-slate-400">实际版本</div><div className="font-mono mt-1">v{task.strategy_template_version || 1}</div></div>
                    <div><div className="text-xs text-slate-400">绑定更新时间</div><div className="mt-1">{task.strategy_template_updated_at ? new Date(task.strategy_template_updated_at).toLocaleString() : '—'}</div></div>
                  </div>
                </section>
              )}

              {task.error && (
                <section className="bg-red-50 rounded-2xl border border-red-200 p-5">
                  <h2 className="font-bold text-red-800">错误</h2>
                  <pre className="mt-3 text-xs text-red-700 whitespace-pre-wrap break-all">{task.error}</pre>
                </section>
              )}

              {task.status === 'done' && task.task_type === 'selection' && (
                <section className="bg-white rounded-2xl border border-blue-100 shadow-sm p-5">
                  <h2 className="font-bold text-slate-800">选股成果</h2>
                  <p className="text-xs text-slate-400 mt-1">本任务已形成历史选股成果快照。</p>
                  <Link href={'/artifacts/selections/' + task.id} className="inline-block mt-3 px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">查看选股成果</Link>
                </section>
              )}

              {task.status === 'done' && task.task_type === 'backtest' && (
                <section className="bg-white rounded-2xl border border-blue-100 shadow-sm p-5">
                  <h2 className="font-bold text-slate-800">回测成果</h2>
                  <p className="text-xs text-slate-400 mt-1">本任务已形成历史回测成果快照。</p>
                  <Link href={'/artifacts/backtests/' + task.id} className="inline-block mt-3 px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">查看回测成果</Link>
                </section>
              )}

              {task.result_summary && (
                <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                  <h2 className="font-bold text-slate-800">结果摘要</h2>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
                    {task.result_summary.final_equity != null && <div><div className="text-xs text-slate-400">最终权益</div><div className="font-mono font-bold mt-1">{Number(task.result_summary.final_equity).toLocaleString()}</div></div>}
                    {task.result_summary.total_return != null && <div><div className="text-xs text-slate-400">总收益</div><div className="font-mono font-bold mt-1">{(task.result_summary.total_return * 100).toFixed(2)}%</div></div>}
                    {task.result_summary.max_drawdown != null && <div><div className="text-xs text-slate-400">最大回撤</div><div className="font-mono font-bold mt-1">{(task.result_summary.max_drawdown * 100).toFixed(2)}%</div></div>}
                    {task.result_summary.n_trades != null && <div><div className="text-xs text-slate-400">成交笔数</div><div className="font-mono font-bold mt-1">{task.result_summary.n_trades}</div></div>}
                  </div>
                  {task.result_uri && (
                    <div className="mt-4 text-xs text-slate-500">结果文件：<span className="font-mono break-all">{task.result_uri}</span></div>
                  )}
                </section>
              )}

              <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                <h2 className="font-bold text-slate-800">任务输入快照</h2>
                <pre className="mt-4 p-4 rounded-xl bg-slate-950 text-slate-100 text-xs overflow-auto max-h-[520px]">{JSON.stringify((task as any).payload || {}, null, 2)}</pre>
              </section>
            </>
          )}
        </div>
      </main>
    </AppShell>
  );
}

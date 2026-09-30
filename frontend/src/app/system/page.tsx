'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';

interface User { id: string; email: string; role: string }
interface ClusterNode {
  node_id: string;
  name?: string;
  status?: string;
  status_zh?: string;
  display_label?: string;
  current_task_id?: number | null;
  task_type_zh?: string;
  heartbeat_at?: string | null;
  last_error?: string | null;
  generation?: number | null;
}
interface QueueItem {
  id: number;
  task_type_zh?: string;
  status?: string;
  assigned_node?: string | null;
  progress_pct?: number | null;
  formula_preview?: string | null;
  date_range?: string | null;
  created_at?: string | null;
  queued_at?: string | null;
  started_at?: string | null;
}
interface QueueSection { title?: string; count?: number; items?: QueueItem[] }
interface ClusterStatus {
  nodes?: ClusterNode[];
  queueStats?: { pending_selection?: number; pending_backtest?: number; running?: number };
  queues?: { selection?: QueueSection; backtest?: QueueSection; running?: QueueSection };
}

const statusStyles: Record<string, string> = {
  idle: 'bg-emerald-50 text-emerald-700 ring-emerald-200',
  running: 'bg-blue-50 text-blue-700 ring-blue-200',
  draining: 'bg-amber-50 text-amber-700 ring-amber-200',
  unhealthy: 'bg-red-50 text-red-700 ring-red-200',
  maintenance: 'bg-slate-100 text-slate-600 ring-slate-200',
};

function dateTime(value?: string | null) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN', { hour12: false });
}

function statusLabel(node: ClusterNode) {
  return node.status_zh || ({ idle: '空闲', running: '运行中', draining: '排空中', unhealthy: '异常', maintenance: '维护中' } as Record<string, string>)[node.status || ''] || node.status || '未知';
}

function QueuePanel({ title, section }: { title: string; section?: QueueSection }) {
  const items = section?.items || [];
  return (
    <section className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
      <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">
        <h2 className="font-semibold text-slate-900">{title}</h2>
        <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-600">{section?.count ?? items.length}</span>
      </div>
      {items.length === 0 ? (
        <p className="px-5 py-6 text-sm text-slate-400">当前没有任务</p>
      ) : (
        <div className="divide-y divide-slate-100">
          {items.map((item) => (
            <Link key={item.id} href={`/tasks/${item.id}`} className="block px-5 py-4 hover:bg-slate-50">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-semibold text-slate-800">任务 #{item.id}</span>
                <span className="text-xs text-slate-500">{item.task_type_zh || '任务'}</span>
                <span className="ml-auto text-xs text-slate-500">{item.status || '—'}</span>
              </div>
              {item.formula_preview && <p className="mt-1 truncate text-sm text-slate-600">{item.formula_preview}</p>}
              <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-400">
                {item.assigned_node && <span>节点：{item.assigned_node}</span>}
                {item.date_range && <span>区间：{item.date_range}</span>}
                {typeof item.progress_pct === 'number' && <span>进度：{item.progress_pct}%</span>}
                <span>创建：{dateTime(item.created_at || item.queued_at || item.started_at)}</span>
              </div>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}

export default function SystemStatusPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [cluster, setCluster] = useState<ClusterStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

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
        } else router.replace('/login');
      } catch {
        if (mounted) router.replace('/login');
      }
    })();
    return () => { mounted = false; };
  }, [router]);

  const refresh = useCallback(async (quiet = false) => {
    if (quiet) setRefreshing(true);
    else setLoading(true);
    try {
      const res = await fetch('/api/v1/cluster/status', { cache: 'no-store' });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `读取集群状态失败（HTTP ${res.status}）`);
      setCluster(data);
      setError('');
      setUpdatedAt(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : '无法读取集群状态');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    if (!user) return;
    void refresh();
    const timer = window.setInterval(() => { void refresh(true); }, 10000);
    return () => window.clearInterval(timer);
  }, [user, refresh]);

  const logout = useCallback(async () => {
    await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
    router.replace('/login');
  }, [router]);

  const nodes = cluster?.nodes || [];
  const stats = cluster?.queueStats || {};
  const healthyCount = nodes.filter((node) => ['idle', 'running'].includes(node.status || '')).length;
  const nodeCount = nodes.length;

  if (authLoading) {
    return <div className="min-h-screen grid place-items-center text-sm text-slate-500">正在验证登录状态…</div>;
  }

  return (
    <AppShell user={user} onLogout={logout}>
      <main className="mx-auto max-w-7xl space-y-6 px-4 py-8 md:px-8">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-sm font-medium text-blue-600">运行监控</p>
            <h1 className="mt-1 text-2xl font-bold tracking-tight text-slate-900">系统状态</h1>
            <p className="mt-2 text-sm text-slate-500">查看 Node1/2/3 健康状态、任务队列与运行诊断。任务调度仍由 Node1 Scheduler 统一负责。</p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-400">更新于 {dateTime(updatedAt)}</span>
            <button type="button" onClick={() => void refresh(true)} disabled={refreshing} className="rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50 disabled:opacity-50">
              {refreshing ? '刷新中…' : '立即刷新'}
            </button>
          </div>
        </div>

        {error && (
          <div role="alert" className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {error}。请检查登录状态、Node1 Scheduler 与状态 API。
          </div>
        )}

        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">节点可用</p>
            <p className="mt-2 text-3xl font-bold text-slate-900">{loading && !cluster ? '—' : `${healthyCount}/${nodeCount}`}</p>
            <p className="mt-1 text-xs text-slate-400">空闲或运行中</p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">等待选股</p>
            <p className="mt-2 text-3xl font-bold text-blue-700">{stats.pending_selection ?? '—'}</p>
            <p className="mt-1 text-xs text-slate-400">选股任务优先调度</p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">等待回测</p>
            <p className="mt-2 text-3xl font-bold text-violet-700">{stats.pending_backtest ?? '—'}</p>
            <p className="mt-1 text-xs text-slate-400">由 Scheduler 分配执行</p>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">正在运行</p>
            <p className="mt-2 text-3xl font-bold text-emerald-700">{stats.running ?? '—'}</p>
            <p className="mt-1 text-xs text-slate-400">实时运行任务总数</p>
          </div>
        </div>

        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-bold text-slate-900">计算节点</h2>
            <span className="text-xs text-slate-400">自动刷新 · 每 10 秒</span>
          </div>
          {loading && !cluster ? (
            <div className="rounded-2xl border border-slate-200 bg-white p-8 text-center text-sm text-slate-400">正在读取节点状态…</div>
          ) : nodeCount === 0 ? (
            <div className="rounded-2xl border border-slate-200 bg-white p-8 text-center text-sm text-slate-500">状态 API 暂未返回节点信息。</div>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {nodes.map((node) => (
                <article key={node.node_id} className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h3 className="font-semibold text-slate-900">{node.name || node.node_id}</h3>
                      <p className="mt-1 text-xs text-slate-400">{node.node_id}</p>
                    </div>
                    <span className={`rounded-full px-2.5 py-1 text-xs font-semibold ring-1 ${statusStyles[node.status || ''] || 'bg-slate-100 text-slate-600 ring-slate-200'}`}>
                      {statusLabel(node)}
                    </span>
                  </div>
                  <dl className="mt-4 space-y-2 text-sm">
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">当前任务</dt><dd className="text-right font-medium text-slate-800">{node.current_task_id ? <Link className="text-blue-600 hover:underline" href={`/tasks/${node.current_task_id}`}>#{node.current_task_id} · {node.task_type_zh || '任务'}</Link> : '无'}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">心跳时间</dt><dd className="text-right text-slate-600">{dateTime(node.heartbeat_at)}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Generation</dt><dd className="text-right text-slate-600">{node.generation ?? '—'}</dd></div>
                  </dl>
                  {node.last_error && <div className="mt-4 rounded-lg bg-red-50 p-3 text-xs leading-5 text-red-700 break-words">最近错误：{node.last_error}</div>}
                </article>
              ))}
            </div>
          )}
        </section>

        <section className="space-y-3">
          <div>
            <h2 className="text-lg font-bold text-slate-900">任务队列</h2>
            <p className="mt-1 text-sm text-slate-500">此处只读展示运行状态，不提供绕过 Scheduler 的节点控制。</p>
          </div>
          <div className="grid gap-4 xl:grid-cols-3">
            <QueuePanel title="待处理选股" section={cluster?.queues?.selection} />
            <QueuePanel title="待处理回测" section={cluster?.queues?.backtest} />
            <QueuePanel title="运行中任务" section={cluster?.queues?.running} />
          </div>
        </section>

        <div className="flex justify-end">
          <Link href="/tasks" className="text-sm font-semibold text-blue-600 hover:text-blue-700">前往任务中心 →</Link>
        </div>
      </main>
    </AppShell>
  );
}

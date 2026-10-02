'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import type { Task } from '@/hooks/useCluster';
import { downloadArtifact } from '@/lib/backtestArtifacts';

const STATUS_LABELS: Record<string, string> = {
  done: '完成', failed: '失败', cancelled: '已取消', preempted: '被抢占',
};

function fmtTime(value?: string | null) {
  return value ? new Date(value).toLocaleString() : '—';
}

function labelTaskType(value: string) {
  return value === 'selection' ? '选股' : '回测';
}

export default function ArtifactDetailPage({ kind }: { kind: 'selection' | 'backtest' }) {
  const params = useParams<{ artifactId: string }>();
  const router = useRouter();
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [task, setTask] = useState<Task | null>(null);
  const [loading, setLoading] = useState(true);
  const [watchlists, setWatchlists] = useState<Array<{ id: number; name: string; item_count: number }>>([]);
  const [selectedWatchlist, setSelectedWatchlist] = useState<number | ''>('');
  const [savingToWatchlist, setSavingToWatchlist] = useState(false);
  const [savedCount, setSavedCount] = useState<number | null>(null);

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
    if (!user || !params?.artifactId) return;
    let mounted = true;
    const load = async () => {
      setLoading(true);
      try {
        const id = Number(params.artifactId);
        if (!Number.isInteger(id) || id <= 0) throw new Error('invalid artifact id');
        const res = await fetch('/api/artifacts/' + id, { cache: 'no-store' });
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || '成果不存在');
        if (json.task_type !== kind || json.status !== 'done') throw new Error('artifact unavailable');
        if (mounted) setTask(json);
      } catch (e) {
        console.error('load artifact detail failed', e);
        if (mounted) setTask(null);
      } finally {
        if (mounted) setLoading(false);
      }
    };
    void load();
    return () => { mounted = false; };
  }, [user, params?.artifactId, kind]);

  useEffect(() => {
    if (!user || kind !== 'selection') return;
    fetch('/api/watchlists', { cache: 'no-store' })
      .then((r) => r.ok ? r.json() : null)
      .then((d) => {
        const rows = Array.isArray(d?.watchlists) ? d.watchlists : [];
        setWatchlists(rows);
        if (rows[0]) setSelectedWatchlist(Number(rows[0].id));
      })
      .catch(() => undefined);
  }, [user, kind]);

  const addSelectionToWatchlist = async () => {
    if (!selectedWatchlist || !selectionCodes.length) return;
    setSavingToWatchlist(true);
    try {
      const res = await fetch('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ listId: selectedWatchlist, codes: selectionCodes.map((code: unknown) => String(code)) }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || '加入自选股失败');
      setSavedCount(Number(data.added_count || 0));
    } catch (e) {
      alert(e instanceof Error ? e.message : '加入自选股失败');
    } finally {
      setSavingToWatchlist(false);
    }
  };

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const payload = (task as any)?.payload || {};
  const selectionSource = payload.selection_strategy_snapshot;
  const backtestSource = payload.source_selection_strategy;
  const selectionResult = (task as any)?.result || {};
  const selectionCodes = Array.isArray(selectionResult.codes)
    ? selectionResult.codes
    : Array.isArray(selectionResult.data) ? selectionResult.data : [];
  const backtestStrategy = payload.strategy || {};
  const backtestEntry = backtestStrategy.entry || {};
  const backtestUniverse = backtestStrategy.universe || {};
  const extractSelectionStrategy = async () => {
    if (!task) return;
    const defaultName = ((task as any).title || '回测成果') + ' · 选股策略';
    const name = window.prompt('新选股策略名称', defaultName);
    if (!name || !name.trim()) return;
    const res = await fetch('/api/artifacts/' + task.id + '/extract-selection-strategy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      alert(data.error || '提取选股策略失败');
      return;
    }
    const strategyId = Number(data?.strategy?.id);
    if (Number.isInteger(strategyId) && strategyId > 0) {
      router.push('/strategies/selection/' + strategyId);
    } else {
      alert('策略已创建，但无法打开策略详情');
    }
  };

  return (
    <AppShell user={user} onLogout={async () => {
      await fetch('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
      router.replace('/login');
    }}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-6xl mx-auto space-y-5">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <Link href="/artifacts" className="text-sm text-blue-600 hover:underline">← 返回成果库</Link>
              <h1 className="text-2xl font-black mt-2">{task?.title || (kind === 'selection' ? '选股成果' : (task?.strategy_template_name || '回测成果'))} #{task?.id || params?.artifactId}</h1>
              <div className="text-xs text-slate-400 mt-1">这是任务完成时形成的结果快照；本页不读取当前策略的最新内容作为历史结果。</div>
            </div>
            {task && <div className="flex items-center gap-2">
              <button type="button" onClick={async () => {
                const title = window.prompt('成果名称', (task as any).title || (kind === 'selection' ? '选股成果' : task.strategy_template_name || '回测成果'));
                if (!title || !title.trim()) return;
                const res = await fetch('/api/artifacts/' + task.id, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: title.trim() }) });
                if (res.ok) {
                  const updated = await res.json();
                  setTask((prev) => prev ? ({ ...prev, ...updated }) : prev);
                }
              }} className="px-3 py-1.5 rounded-lg border border-slate-200 text-xs font-bold text-slate-600">重命名</button>
              <button type="button" onClick={async () => {
                if (!window.confirm('删除该成果？原任务不会被删除。')) return;
                const res = await fetch('/api/artifacts/' + task.id, { method: 'DELETE' });
                if (res.ok) router.replace('/artifacts');
                else alert('删除失败');
              }} className="px-3 py-1.5 rounded-lg border border-red-200 text-xs font-bold text-red-600">删除</button>
              <span className="px-3 py-1.5 rounded-full bg-emerald-50 text-emerald-700 text-xs font-bold">{STATUS_LABELS[task.status] || task.status}</span>
            </div>}
          </div>

          {loading ? <section className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-slate-400">加载中...</section> : !task ? (
            <section className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-red-500">成果不存在、尚未完成或无权访问</section>
          ) : (
            <>
              <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                <h2 className="font-bold text-slate-800">来源信息</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-sm">
                  <div><div className="text-xs text-slate-400">成果类型</div><div className="mt-1 font-semibold">{labelTaskType(task.task_type)}</div></div>
                  <div><div className="text-xs text-slate-400">所属任务</div>{task.task_exists === false ? (
                    <div className="mt-1 font-mono text-slate-400">任务已删除 · #{task.task_id ?? task.id}</div>
                  ) : (
                    <Link href={'/tasks/' + (task.task_id ?? task.id)} className="mt-1 inline-block font-mono text-blue-600 hover:underline">#{task.task_id ?? task.id}</Link>
                  )}</div>
                  <div><div className="text-xs text-slate-400">来源任务</div>{(task as any).source_task_id == null ? (
                    <div className="mt-1 text-slate-400">无来源任务</div>
                  ) : (
                    <Link href={'/tasks/' + (task as any).source_task_id} className="mt-1 inline-block font-mono text-blue-600 hover:underline">#{(task as any).source_task_id}</Link>
                  )}</div>
                  <div><div className="text-xs text-slate-400">完成时间</div><div className="mt-1">{fmtTime(task.finished_at)}</div></div>
                  <div><div className="text-xs text-slate-400">执行节点</div><div className="mt-1 font-mono">{task.assigned_node || '—'}</div></div>
                </div>
              </section>

              {kind === 'selection' ? (
                <>
                  <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                    <h2 className="font-bold text-slate-800">选股条件快照</h2>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-sm">
                      <div className="col-span-2 md:col-span-4"><div className="text-xs text-slate-400">公式</div><div className="mt-1 font-mono break-all">{payload.formula || '—'}</div></div>
                      <div><div className="text-xs text-slate-400">周期</div><div className="mt-1">{payload.timeframe || 'D'}</div></div>
                      <div><div className="text-xs text-slate-400">信号日期</div><div className="mt-1">{payload.date || selectionResult.date || '最近可用交易日'}</div></div>
                      <div><div className="text-xs text-slate-400">股票数量</div><div className="mt-1 font-mono">{selectionCodes.length}</div></div>
                      <div><div className="text-xs text-slate-400">降级</div><div className="mt-1">{selectionResult.meta?.degraded ? '是' : '否'}</div></div>
                    </div>
                    {selectionSource && (
                      <div className="mt-4 rounded-xl border border-blue-100 bg-blue-50/60 p-3">
                        <div className="text-xs font-bold text-blue-700">策略版本快照</div>
                        <div className="mt-1 text-sm text-blue-900">{selectionSource.name} · v{selectionSource.version_no}</div>
                        <div className="mt-1 text-xs font-mono text-blue-700 break-all">{selectionSource.formula} · {selectionSource.timeframe}</div>
                        {selectionSource.id && <Link href={'/strategies/selection/' + selectionSource.id} className="inline-block mt-2 text-xs font-bold text-blue-700 hover:underline">查看来源策略 v{selectionSource.version_no} →</Link>}
                      </div>
                    )}
                  </section>

                  <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <h2 className="font-bold text-slate-800">选股结果</h2>
                        <div className="text-xs text-slate-400 mt-1">展示前 300 个代码，可从股票研究继续查看。</div>
                      </div>
                      <span className="font-mono text-xs text-slate-500">共 {selectionCodes.length}</span>
                    </div>
                    {selectionCodes.length === 0 ? (
                      <div className="py-10 text-center text-slate-400">没有返回股票代码</div>
                    ) : (
                      <div className="mt-4 space-y-4">
                        <div className="flex flex-wrap items-center gap-2">
                          <select value={selectedWatchlist} onChange={(e) => setSelectedWatchlist(e.target.value ? Number(e.target.value) : '')} className="px-3 py-2 rounded-lg border border-slate-200 bg-white text-xs">
                            <option value="">选择自选股列表</option>
                            {watchlists.map((w) => <option key={w.id} value={w.id}>{w.name} · {w.item_count}</option>)}
                          </select>
                          <button type="button" onClick={() => void addSelectionToWatchlist()} disabled={savingToWatchlist || !selectedWatchlist || selectionCodes.length === 0} className="px-3 py-2 text-xs font-bold text-white bg-blue-600 rounded-lg disabled:opacity-50">
                            {savingToWatchlist ? '加入中…' : '将本次选股结果加入自选股'}
                          </button>
                          {savedCount != null && <span className="text-xs text-emerald-600">已新增 {savedCount} 只</span>}
                        </div>
                        <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
                          {selectionCodes.slice(0, 300).map((code: string) => (
                            <Link key={code} href={'/stocks/' + encodeURIComponent(String(code))} className="px-3 py-2 rounded-lg border border-slate-100 bg-slate-50 text-xs font-mono text-slate-700 hover:border-blue-200 hover:text-blue-600 text-center">{String(code)}</Link>
                          ))}
                        </div>
                      </div>
                    )}
                  </section>
                </>
              ) : (
                <>
                  <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                    <h2 className="font-bold text-slate-800">回测结果摘要</h2>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
                      {task.result_summary?.final_equity != null && <div><div className="text-xs text-slate-400">最终权益</div><div className="font-mono font-bold mt-1">{Number(task.result_summary.final_equity).toLocaleString()}</div></div>}
                      {task.result_summary?.total_return != null && <div><div className="text-xs text-slate-400">总收益</div><div className="font-mono font-bold mt-1">{(task.result_summary.total_return * 100).toFixed(2)}%</div></div>}
                      {task.result_summary?.max_drawdown != null && <div><div className="text-xs text-slate-400">最大回撤</div><div className="font-mono font-bold mt-1">{(task.result_summary.max_drawdown * 100).toFixed(2)}%</div></div>}
                      {task.result_summary?.n_trades != null && <div><div className="text-xs text-slate-400">成交笔数</div><div className="font-mono font-bold mt-1">{task.result_summary.n_trades}</div></div>}
                    </div>
                  </section>

                  <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                    <h2 className="font-bold text-slate-800">研究配置快照</h2>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4 text-sm">
                      <div className="col-span-2 md:col-span-4"><div className="text-xs text-slate-400">Entry</div><div className="mt-1 font-mono break-all">{backtestEntry.condition || '—'}</div></div>
                      <div><div className="text-xs text-slate-400">信号周期</div><div className="mt-1">{backtestEntry.timeframe || 'D'}</div></div>
                      <div><div className="text-xs text-slate-400">股票池</div><div className="mt-1">{backtestUniverse.type === 'index' ? '指数 ' + (backtestUniverse.index_id || '—') : '全 A'}</div></div>
                      <div><div className="text-xs text-slate-400">回测区间</div><div className="mt-1">{payload.start_date || '—'} → {payload.end_signal_date || '—'}</div></div>
                      <div><div className="text-xs text-slate-400">初始资金</div><div className="mt-1 font-mono">{payload.initial_cash != null ? Number(payload.initial_cash).toLocaleString() : '—'}</div></div>
                    </div>
                    {task.strategy_template_id && (
                      <div className="mt-4 rounded-xl border border-amber-100 bg-amber-50/60 p-3">

                        <div className="text-xs font-bold text-amber-700">回测策略版本</div>
                        <div className="mt-1 text-sm text-amber-900">{task.strategy_template_name || ('#' + task.strategy_template_id)} · v{task.strategy_template_version || 1}</div>
                        <Link href="/strategies" className="inline-block mt-2 text-xs font-bold text-amber-700 hover:underline">查看策略库 →</Link>
                      </div>
                    )}
                    {backtestSource && (
                      <div className="mt-3 rounded-xl border border-blue-100 bg-blue-50/60 p-3">
                        <div className="text-xs font-bold text-blue-700">基础选股策略快照</div>
                        <div className="mt-1 text-sm text-blue-900">{backtestSource.name} · v{backtestSource.version_no}</div>
                        <div className="mt-1 text-xs font-mono text-blue-700 break-all">{backtestSource.formula} · {backtestSource.timeframe}</div>
                        {backtestSource.id && <Link href={'/strategies/selection/' + backtestSource.id} className="inline-block mt-2 text-xs font-bold text-blue-700 hover:underline">查看基础选股策略 v{backtestSource.version_no} →</Link>}
                      </div>
                    )}
                    <div className="mt-4 flex flex-wrap gap-2">
                      <button type="button" onClick={() => void extractSelectionStrategy()} className="px-3 py-2 text-xs font-bold text-white bg-blue-600 rounded-lg hover:bg-blue-700">提取为选股策略</button>
                    </div>
                  </section>

                  <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                    <h2 className="font-bold text-slate-800">Artifact 数据</h2>
                    <div className="text-xs text-slate-400 mt-1">大型结果文件仍由 Node1 结果存储提供，本页使用现有权限控制读取。</div>
                    <div className="flex flex-wrap gap-2 mt-4">
                      {(['equity_curve', 'trades', 'positions_daily'] as const).map(name => (
                        <button key={name} type="button" onClick={() => void downloadArtifact(task.id, name, { artifact: true })} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">
                          {name === 'equity_curve' ? '下载权益曲线' : name === 'trades' ? '下载成交明细' : '下载持仓明细'}
                        </button>
                      ))}
                    </div>
                    {task.result_uri && <div className="mt-4 text-xs font-mono text-slate-400 break-all">引用：{task.result_uri}</div>}
                  </section>
                </>
              )}

              <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
                <h2 className="font-bold text-slate-800">任务输入完整快照</h2>
                <pre className="mt-3 p-4 rounded-xl bg-slate-950 text-slate-100 text-xs overflow-auto max-h-[520px]">{JSON.stringify(payload, null, 2)}</pre>
              </section>
            </>
          )}
        </div>
      </main>
    </AppShell>
  );
}

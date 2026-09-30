'use client';

import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import { useCallback, useEffect, useState } from 'react';
import AppShell from '@/components/app/AppShell';

interface User { id: string; email: string; role: string }

interface BacktestTemplate {
  id: number;
  name: string;
  description?: string | null;
  config: any;
  created_at: string;
  updated_at: string;
  version_no?: number;
}

interface BacktestStrategyVersion {
  id: number;
  version_no: number;
  name: string;
  description?: string | null;
  config: any;
  created_at: string;
}

function triggerLabel(value: string) {
  return ({ condition: '条件成立', cross_above: '上穿', cross_below: '下穿' } as Record<string, string>)[value] || value || '—';
}

function timeframeLabel(value: string) {
  return ({ D: '日', W: '周', M: '月' } as Record<string, string>)[value] || value || '—';
}

export default function BacktestStrategyDetailPage() {
  const params = useParams<{ strategyId: string }>();
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [template, setTemplate] = useState<BacktestTemplate | null>(null);
  const [versions, setVersions] = useState<BacktestStrategyVersion[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (json.user) setUser(json.user);
        else router.replace('/login');
      } catch {
        if (mounted) router.replace('/login');
      } finally {
        if (mounted) setAuthLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [router]);

  const refresh = useCallback(async () => {
    const id = Number(params?.strategyId);
    if (!Number.isInteger(id) || id <= 0) {
      setTemplate(null);
      setLoading(false);
      return;
    }
    setLoading(true);
    try {
      const res = await fetch('/api/backtest-strategy-templates', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载回测策略失败');
      const found = (json.templates || []).find((item: BacktestTemplate) => Number(item.id) === id) || null;
      setTemplate(found);
    } catch (error) {
      console.error('Failed to load backtest strategy', error);
      setTemplate(null);
    } finally {
      setLoading(false);
    }
  }, [params?.strategyId]);

  useEffect(() => {
    if (user) void refresh();
  }, [user, refresh]);

  useEffect(() => {
    if (!user || !template) {
      setVersions([]);
      return;
    }
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/backtest-strategy-templates/' + template.id + '/versions', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (!res.ok) throw new Error(json.error || '加载策略版本失败');
        setVersions(json.versions || []);
      } catch (error) {
        console.error('Failed to load backtest strategy versions', error);
        if (mounted) setVersions([]);
      }
    })();
    return () => { mounted = false; };
  }, [user, template?.id]);



  const openWorkspace = () => {
    if (!template) return;
    sessionStorage.setItem('bq-pending-backtest-template', JSON.stringify({ id: template.id }));
    router.push('/select?tab=backtest');
  };

  const extractVersion = async (versionNo?: number, versionName?: string) => {
    if (!template) return;
    const defaultName = (versionName || template.name) + ' · Entry选股';
    const name = window.prompt('新选股策略名称', defaultName);
    if (!name?.trim()) return;
    try {
      const res = await fetch('/api/strategies/extract-from-backtest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          backtest_strategy_id: template.id,
          ...(versionNo ? { version_no: versionNo } : {}),
          name: name.trim(),
        }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '提取失败');
      router.push('/strategies/selection/' + json.strategy.id);
    } catch (error) {
      alert(error instanceof Error ? error.message : '提取失败');
    }
  };

  const remove = async () => {
    if (!template) return;
    if (!confirm('确定删除回测策略“' + template.name + '”？')) return;
    try {
      const res = await fetch('/api/backtest-strategy-templates?id=' + template.id, { method: 'DELETE' });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '删除失败');
      router.replace('/strategies');
    } catch (error) {
      alert(error instanceof Error ? error.message : '删除失败');
    }
  };

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const config = template?.config || {};
  const strategy = config.strategy || {};
  const entry = strategy.entry || {};
  const exit = strategy.exit || {};
  const universe = strategy.universe || {};
  const sizing = strategy.sizing || {};
  const rebalance = strategy.rebalance || {};
  const fee = config.fee_policy || {};
  const benchmark = config.benchmark || {};

  return (
    <AppShell user={user} onLogout={async () => {
      try { await fetch('/api/auth/logout', { method: 'POST' }); } finally { router.replace('/login'); }
    }}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-5xl mx-auto space-y-5">
          <div className="flex items-center gap-2 text-sm text-slate-500">
            <Link href="/strategies" className="hover:text-blue-600">策略库</Link>
            <span>→</span>
            <span>回测策略</span>
          </div>

          {loading ? (
            <section className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-slate-400">加载中...</section>
          ) : !template ? (
            <section className="bg-white rounded-2xl border border-slate-200 p-12 text-center">
              <div className="font-semibold text-slate-600">回测策略不存在或无权访问</div>
              <Link href="/strategies" className="inline-block mt-4 text-sm text-blue-600 font-semibold">返回策略库</Link>
            </section>
          ) : (
            <>
              <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <h1 className="text-2xl font-black text-slate-800">{template.name}</h1>
                      <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700">回测策略</span>
                      {template.version_no && <span className="text-[10px] px-2 py-1 rounded-full bg-blue-50 text-blue-600">v{template.version_no}</span>}
                    </div>
                    {template.description && <p className="text-sm text-slate-500 mt-2">{template.description}</p>}
                    <div className="text-xs text-slate-400 mt-3">创建：{new Date(template.created_at).toLocaleString()} · 更新：{new Date(template.updated_at).toLocaleString()}</div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <button type="button" onClick={openWorkspace} className="px-4 py-2.5 text-sm font-bold text-white bg-blue-600 rounded-xl hover:bg-blue-700">在回测工作台使用</button>
                    <button type="button" onClick={() => void extractVersion(template.version_no)} className="px-4 py-2.5 text-sm font-bold text-blue-600 border border-blue-200 rounded-xl hover:bg-blue-50">提取为选股策略</button>
                    <button type="button" onClick={() => void remove()} className="px-4 py-2.5 text-sm font-bold text-red-500 border border-red-200 rounded-xl hover:bg-red-50">删除</button>
                  </div>
                </div>
              </section>

              {config.source_selection_strategy && (
                <section className="bg-white rounded-2xl border border-blue-200 shadow-sm p-5">
                  <div className="text-xs font-bold text-blue-700">基础选股策略</div>
                  <div className="mt-1 text-sm text-slate-700">
                    <Link href={'/strategies/selection/' + config.source_selection_strategy.id} className="font-semibold text-blue-600 hover:underline">
                      {config.source_selection_strategy.name || ('选股策略 #' + config.source_selection_strategy.id)}
                    </Link>
                    {' · v' + (config.source_selection_strategy.version_no || 1)}
                    {' · ' + (config.source_selection_strategy.timeframe || 'D')}
                  </div>
                  <div className="mt-1 text-xs font-mono text-slate-500 break-all">{config.source_selection_strategy.formula}</div>
                  <div className="mt-1 text-[11px] text-slate-400">该快照表示“创建/更新回测策略时所依据的选股策略版本”；后续 Entry 修改不会回写源策略。</div>
                </section>
              )}

              <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <div className="bg-white rounded-2xl border border-slate-200 p-5 space-y-4">
                  <h2 className="font-bold text-slate-800">Entry / Exit</h2>
                  <div>
                    <div className="text-xs text-slate-400">Entry 条件</div>
                    <div className="mt-1 font-mono text-sm break-all">{entry.condition || '—'}</div>
                    <div className="text-xs text-slate-500 mt-1">{triggerLabel(entry.trigger)} · {timeframeLabel(entry.timeframe)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-400">Exit 条件</div>
                    <div className="mt-1 font-mono text-sm break-all">{exit.condition || '未设置'}</div>
                    {exit.condition && <div className="text-xs text-slate-500 mt-1">{triggerLabel(exit.trigger)} · {timeframeLabel(exit.timeframe)}</div>}
                  </div>
                  <div className="text-xs text-slate-500">模式：{strategy.mode === 'event_driven' ? '事件驱动' : '目标组合'}</div>
                </div>

                <div className="bg-white rounded-2xl border border-slate-200 p-5 space-y-4">
                  <h2 className="font-bold text-slate-800">组合与股票池</h2>
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div><span className="text-xs text-slate-400">股票池</span><div className="mt-1">{universe.type === 'index' ? '指数 ' + (universe.index_id || '—') : '全 A'}</div></div>
                    <div><span className="text-xs text-slate-400">仓位方式</span><div className="mt-1">{sizing.method === 'equal_weight' ? '全选等权' : 'Top-N 等权'}</div></div>
                    <div><span className="text-xs text-slate-400">最大持仓</span><div className="mt-1">{sizing.max_positions || '—'}</div></div>
                    <div><span className="text-xs text-slate-400">调仓频率</span><div className="mt-1">{rebalance.frequency === 'weekly' ? '每周' : '每日'}</div></div>
                    <div><span className="text-xs text-slate-400">最少上市天数</span><div className="mt-1">{config.min_listing_days ?? 0}</div></div>
                    <div><span className="text-xs text-slate-400">排除 ST</span><div className="mt-1">{config.exclude_st ? '是' : '否'}</div></div>
                  </div>
                </div>

                <div className="bg-white rounded-2xl border border-slate-200 p-5 space-y-3">
                  <h2 className="font-bold text-slate-800">费率</h2>
                  <div className="text-sm">方案：{fee.mode === 'fixed' ? '固定研究费率' : '历史真实费率'}</div>
                  {fee.mode === 'fixed' && (
                    <div className="grid grid-cols-2 gap-3 text-sm text-slate-600">
                      <div>佣金率：{fee.commission_rate ?? '—'}</div>
                      <div>最低佣金：{fee.commission_min ?? '—'}</div>
                      <div>印花税率：{fee.stamp_tax_rate ?? '—'}</div>
                      <div>过户费率：{fee.transfer_fee_rate ?? '—'}</div>
                    </div>
                  )}
                </div>

                <div className="bg-white rounded-2xl border border-slate-200 p-5 space-y-3">
                  <h2 className="font-bold text-slate-800">版本历史</h2>
                  {versions.length === 0 ? (
                    <div className="text-sm text-slate-400">暂无版本历史</div>
                  ) : (
                    <div className="space-y-2 max-h-72 overflow-y-auto">
                      {versions.map((version) => (
                        <div key={version.id} className="rounded-xl border border-slate-100 bg-slate-50 px-3 py-2">
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-bold text-blue-600">v{version.version_no}</span>
                            <span className="text-xs font-semibold text-slate-700 truncate">{version.name}</span>
                          </div>
                          <div className="text-[11px] text-slate-400 mt-1">
                            {new Date(version.created_at).toLocaleString()}
                            {version.config?.strategy?.entry?.condition ? ' · ' + version.config.strategy.entry.condition : ''}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="bg-white rounded-2xl border border-slate-200 p-5 space-y-3">
                  <h2 className="font-bold text-slate-800">基准与原始配置</h2>
                  <div className="text-sm text-slate-600">
                    {benchmark.enabled === false ? '未启用基准指数' : '基准指数：' + (benchmark.index_id || '000300')}
                  </div>
                  <details className="mt-2">
                    <summary className="cursor-pointer text-xs text-slate-500">查看完整配置 JSON</summary>
                    <pre className="mt-2 max-h-80 overflow-auto rounded-xl bg-slate-950 text-slate-100 p-3 text-[11px] leading-5">{JSON.stringify(config, null, 2)}</pre>
                  </details>
                </div>
              </section>
            </>
          )}
        </div>
      </main>
    </AppShell>
  );
}

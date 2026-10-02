'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import { downloadFromResponse } from '@/lib/download';
import { BUILT_IN_TEMPLATES, type BacktestStrategyTemplate } from '@/components/BacktestStrategyTemplates';

interface User { id: string; email: string; role: string }
interface Strategy {
  id: number; name: string; formula: string; timeframe: string;
  created_at: string; updated_at: string; version_no?: number;
  source_backtest_strategy_id?: number | null;
  source_backtest_strategy_version?: number | null;
  source_backtest_strategy_name?: string | null;
  source_backtest_strategy_trigger?: string | null;
  source_backtest_artifact_id?: number | null;
  source_backtest_artifact_title?: string | null;
}

export default function StrategiesPage() {
  const router = useRouter();
  const fileRef = useRef<HTMLInputElement>(null);
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');
  const [formula, setFormula] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const [backtestTemplates, setBacktestTemplates] = useState<BacktestStrategyTemplate[]>([]);
  const [backtestLoading, setBacktestLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (json.user) { setUser(json.user); setAuthLoading(false); }
        else router.replace('/login');
      } catch { if (mounted) router.replace('/login'); }
    })();
    return () => { mounted = false; };
  }, [router]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/strategies', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载策略失败');
      setStrategies(json.strategies || []);
    } catch (error) { console.error('Failed to load strategies', error); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { if (user) void refresh(); }, [user, refresh]);

  useEffect(() => {
    if (!user) return;
    let mounted = true;
    (async () => {
      setBacktestLoading(true);
      try {
        const res = await fetch('/api/backtest-strategy-templates', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (!res.ok) throw new Error(json.error || '加载回测策略失败');
        setBacktestTemplates(json.templates || []);
      } catch (error) {
        console.error('Failed to load backtest strategies', error);
        if (mounted) setBacktestTemplates([]);
      } finally {
        if (mounted) setBacktestLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [user]);

  const openBacktestWorkspace = (template?: BacktestStrategyTemplate | { config: any }) => {
    if (template) {
      const payload = 'id' in template ? { id: template.id } : { config: template.config };
      sessionStorage.setItem('bq-pending-backtest-template', JSON.stringify(payload));
    }
    router.push('/select?tab=backtest');
  };

  const removeBacktest = async (template: BacktestStrategyTemplate) => {
    if (!confirm('确定删除回测策略“' + template.name + '”？')) return;
    try {
      const res = await fetch('/api/backtest-strategy-templates?id=' + template.id, { method: 'DELETE' });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '删除失败');
      setBacktestTemplates((prev) => prev.filter((item) => item.id !== template.id));
    } catch (error) {
      alert(error instanceof Error ? error.message : '删除失败');
    }
  };


  const create = async () => {
    const n = name.trim();
    const f = formula.trim();
    if (!n || !f) return;
    try {
      const res = await fetch('/api/strategies', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: n, formula: f, timeframe }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '创建失败');
      setName(''); setFormula(''); setTimeframe('D'); setShowCreate(false);
      await refresh();
    } catch (error) { alert(error instanceof Error ? error.message : '创建失败'); }
  };

  const createBacktestFromStrategy = (strategy: Strategy) => {
    sessionStorage.setItem('bq-pending-backtest-from-selection', JSON.stringify({
      id: strategy.id,
      version_no: strategy.version_no || 1,
      name: strategy.name,
      formula: strategy.formula,
      timeframe: strategy.timeframe,
    }));
    router.push('/select?tab=backtest');
  };

  const useStrategy = (strategy: Strategy) => {
    sessionStorage.setItem('bq-pending-selection-strategy', JSON.stringify({
      id: strategy.id, version_no: strategy.version_no || 1, name: strategy.name, formula: strategy.formula, timeframe: strategy.timeframe,
    }));
    router.push('/select');
  };

  const importFile = async (file: File) => {
    try {
      const text = await file.text();
      const payload = JSON.parse(text);
      const res = await fetch('/api/strategies/import', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '导入失败');
      await refresh();
      const importedSelection = Number(json.imported || 0);
      const importedBacktests = Number(json.imported_backtests || 0);
      const skippedCount = (Array.isArray(json.skipped) ? json.skipped.length : 0) + (Array.isArray(json.skipped_backtests) ? json.skipped_backtests.length : 0);
      const message = `已导入选股策略 ${importedSelection} 个、回测策略 ${importedBacktests} 个${skippedCount ? `，跳过 ${skippedCount} 个重复或无效策略` : ''}`;
      alert(message);
    } catch (error) { alert(error instanceof Error ? error.message : '导入文件格式无效'); }
    finally { if (fileRef.current) fileRef.current.value = ''; }
  };

  const remove = async (strategy: Strategy) => {
    if (!confirm(`确定删除策略“${strategy.name}”？历史版本也会一并删除。`)) return;
    try {
      const res = await fetch(`/api/strategies/${strategy.id}`, { method: 'DELETE' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '删除失败');
      setStrategies((prev) => prev.filter((item) => item.id !== strategy.id));
    } catch (error) { alert(error instanceof Error ? error.message : '删除失败'); }
  };

  const handleLogout = useCallback(async () => {
    try { await fetch('/api/auth/logout', { method: 'POST' }); } finally { router.replace('/login'); }
  }, [router]);

  if (authLoading) return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-6xl mx-auto space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-black">策略库</h1>
              <p className="text-sm text-slate-500 mt-1">统一管理选股与回测策略；导入/导出会保留可迁移的版本历史。</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <input ref={fileRef} type="file" accept="application/json,.json" className="hidden" onChange={(e) => { const file = e.target.files?.[0]; if (file) void importFile(file); }} />
              <button type="button" onClick={() => fileRef.current?.click()} className="px-3 py-2.5 rounded-xl bg-white border border-slate-200 text-slate-600 text-sm font-bold hover:bg-slate-50">导入</button>
              <button type="button" onClick={() => void downloadFromResponse('/api/strategies/export')} className="px-3 py-2.5 rounded-xl bg-white border border-slate-200 text-slate-600 text-sm font-bold hover:bg-slate-50">导出</button>
              <button type="button" onClick={() => setShowCreate(true)} className="px-4 py-2.5 rounded-xl bg-blue-600 text-white text-sm font-bold hover:bg-blue-700 shadow-sm">+ 新建策略</button>
            </div>
          </div>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <div>
                <div className="font-bold text-slate-800">我的选股策略</div>
                <div className="text-xs text-slate-400 mt-1">公式、周期和版本历史都属于策略对象</div>
              </div>
              <span className="text-xs font-mono text-slate-400">{strategies.length}</span>
            </div>
            {loading ? <div className="p-12 text-center text-slate-400">加载中...</div> : strategies.length === 0 ? (
              <div className="p-12 text-center">
                <div className="text-slate-500 font-semibold">暂无保存的选股策略</div>
                <button type="button" onClick={() => setShowCreate(true)} className="mt-3 text-sm text-blue-600 font-semibold">创建第一个策略</button>
              </div>
            ) : (
              <div className="divide-y divide-slate-100">
                {strategies.map((strategy) => (
                  <div key={strategy.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                    <div className="flex-1 min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <Link href={`/strategies/selection/${strategy.id}`} className="font-bold text-slate-800 hover:text-blue-600">{strategy.name}</Link>
                        <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-500">选股策略</span>
                        {strategy.version_no && <span className="text-[10px] px-2 py-1 rounded-full bg-blue-50 text-blue-600">v{strategy.version_no}</span>}
                      </div>
                      <div className="text-xs font-mono text-slate-500 mt-2 break-all">{strategy.formula}</div>
                      <div className="text-xs text-slate-400 mt-2">周期：{strategy.timeframe} · 更新：{new Date(strategy.updated_at).toLocaleString()}</div>
                      {strategy.source_backtest_strategy_id && <div className="text-xs text-amber-600 mt-1">来源：回测策略 {strategy.source_backtest_strategy_name || ('#' + strategy.source_backtest_strategy_id)} · v{strategy.source_backtest_strategy_version || 1}</div>}
                      {strategy.source_backtest_artifact_id && <div className="text-xs text-blue-600 mt-1">来源成果：<Link href={'/artifacts/backtests/' + strategy.source_backtest_artifact_id} className="hover:underline">{strategy.source_backtest_artifact_title || ('回测成果 #' + strategy.source_backtest_artifact_id)}</Link></div>}
                    </div>
                    <div className="flex flex-wrap gap-2 shrink-0">
                      <button type="button" onClick={() => useStrategy(strategy)} className="px-3 py-2 text-xs font-bold text-white bg-blue-600 rounded-lg hover:bg-blue-700">运行选股</button>
                      <button type="button" onClick={() => createBacktestFromStrategy(strategy)} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">创建回测策略</button>
                      <Link href={`/strategies/selection/${strategy.id}`} className="px-3 py-2 text-xs font-bold text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50">查看/编辑</Link>
                      <button type="button" onClick={() => void remove(strategy)} className="px-3 py-2 text-xs font-bold text-red-500 border border-red-200 rounded-lg hover:bg-red-50">删除</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="font-bold text-slate-800">我的回测策略</div>
                <div className="text-xs text-slate-400 mt-1">策略定义继续持久化在 Node1 SQLite；这里提供统一管理入口</div>
              </div>
              <button type="button" onClick={() => openBacktestWorkspace()} className="px-3 py-2 text-xs font-bold text-white bg-blue-600 rounded-lg hover:bg-blue-700">+ 新建回测策略</button>
            </div>
            {backtestLoading ? <div className="p-10 text-center text-slate-400">加载中...</div> : backtestTemplates.length === 0 ? (
              <div className="p-10 text-center text-slate-500">
                <div className="font-semibold">暂无保存的回测策略</div>
                <button type="button" onClick={() => openBacktestWorkspace()} className="mt-3 text-sm text-blue-600 font-semibold">打开回测工作台创建</button>
              </div>
            ) : (
              <div className="divide-y divide-slate-100">
                {backtestTemplates.map((template) => {
                  const s = template.config?.strategy || {};
                  const entry = s.entry || {};
                  const universe = s.universe || {};
                  return (
                    <div key={template.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link href={'/strategies/backtest/' + template.id} className="font-bold text-slate-800 hover:text-blue-600">{template.name}</Link>
                          <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700">回测策略</span>
                          {template.version_no && <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700">v{template.version_no}</span>}
                        </div>
                        <div className="text-xs font-mono text-slate-500 mt-2 break-all">{entry.condition || '未设置 Entry 条件'}</div>
                        <div className="text-xs text-slate-400 mt-2">
                          {universe.type === 'index' ? '指数 ' + (universe.index_id || '') : '全 A'} · {entry.timeframe || 'D'} · {s.mode === 'event_driven' ? '事件驱动' : '目标组合'} · 更新：{new Date(template.updated_at).toLocaleString()}
                        </div>
                        {template.config?.source_selection_strategy && (
                          <div className="text-xs text-blue-600 mt-1">
                            基于选股策略：{template.config.source_selection_strategy.name} · v{template.config.source_selection_strategy.version_no}
                          </div>
                        )}
                        {template.description && <div className="text-xs text-slate-500 mt-1 truncate">{template.description}</div>}
                      </div>
                      <div className="flex flex-wrap gap-2 shrink-0">
                        <button type="button" onClick={() => openBacktestWorkspace(template)} className="px-3 py-2 text-xs font-bold text-white bg-blue-600 rounded-lg hover:bg-blue-700">使用</button>
                        <Link href={'/strategies/backtest/' + template.id} className="px-3 py-2 text-xs font-bold text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50">查看/编辑</Link>
                        <button type="button" onClick={() => void removeBacktest(template)} className="px-3 py-2 text-xs font-bold text-red-500 border border-red-200 rounded-lg hover:bg-red-50">删除</button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </section>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <div>
                <div className="font-bold text-slate-800">内置策略</div>
                <div className="text-xs text-slate-400 mt-1">只读预置，不写入 Node1；使用后可在回测工作台另存为自己的回测策略</div>
              </div>
              <span className="text-xs font-mono text-slate-400">{BUILT_IN_TEMPLATES.length}</span>
            </div>
            <div className="divide-y divide-slate-100">
              {BUILT_IN_TEMPLATES.map((template) => (
                <div key={template.id} className="p-5 flex flex-col lg:flex-row lg:items-center gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <div className="font-bold text-slate-800">{template.name}</div>
                      <span className="text-[10px] px-2 py-1 rounded-full bg-slate-100 text-slate-500">内置</span>
                    </div>
                    <div className="text-xs text-slate-500 mt-1">{template.description}</div>
                    <div className="text-xs font-mono text-slate-400 mt-2 break-all">{template.config?.strategy?.entry?.condition || ''}</div>
                  </div>
                  <button type="button" onClick={() => openBacktestWorkspace({ config: template.config })} className="px-3 py-2 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 shrink-0">使用</button>
                </div>
              ))}
            </div>
          </section>

          {showCreate && <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={() => setShowCreate(false)}>
            <div className="bg-white rounded-2xl w-full max-w-2xl p-6 shadow-xl" onClick={(e) => e.stopPropagation()}>
              <h2 className="font-bold text-slate-800">新建选股策略</h2>
              <p className="text-xs text-slate-400 mt-1">策略名称可自定义，公式保存后作为 v1。</p>
              <input value={name} onChange={(e) => setName(e.target.value)} maxLength={80} autoFocus placeholder="策略名称，例如：20日均线突破" className="mt-4 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm" />
              <textarea value={formula} onChange={(e) => setFormula(e.target.value)} rows={4} placeholder="例如：CLOSE > MA(CLOSE, 20)" className="mt-3 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm font-mono resize-y" />
              <div className="mt-3 flex items-center gap-2">
                <span className="text-xs font-bold text-slate-500">周期</span>
                {['D', 'W', 'M'].map((tf) => <button key={tf} type="button" onClick={() => setTimeframe(tf)} className={`px-3 py-1.5 text-xs rounded-lg border ${timeframe === tf ? 'bg-blue-600 text-white border-blue-600' : 'border-slate-200 text-slate-600'}`}>{tf}</button>)}
              </div>
              <div className="mt-5 flex justify-end gap-2">
                <button type="button" onClick={() => setShowCreate(false)} className="px-4 py-2 text-sm rounded-xl border border-slate-200 text-slate-600">取消</button>
                <button type="button" onClick={() => void create()} disabled={!name.trim() || !formula.trim()} className="px-4 py-2 text-sm rounded-xl bg-blue-600 text-white font-bold disabled:opacity-50">创建</button>
              </div>
            </div>
          </div>}
        </div>
      </main>
    </AppShell>
  );
}

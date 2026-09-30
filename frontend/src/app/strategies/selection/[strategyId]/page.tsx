'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';

interface User { id: string; email: string; role: string }
interface Strategy {
  id: number; name: string; formula: string; timeframe: string;
  created_at: string; updated_at: string; version_no?: number;
  source_backtest_strategy_id?: number | null;
  source_backtest_strategy_version?: number | null;
  source_backtest_strategy_name?: string | null;
  source_backtest_strategy_trigger?: string | null;
}
interface Version { id: number; strategy_id: number; version_no: number; name: string; formula: string; timeframe: string; created_at: string }

export default function SelectionStrategyPage() {
  const params = useParams<{ strategyId: string }>();
  const router = useRouter();
  const strategyId = Array.isArray(params?.strategyId) ? params.strategyId[0] : params?.strategyId;
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [strategy, setStrategy] = useState<Strategy | null>(null);
  const [versions, setVersions] = useState<Version[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [name, setName] = useState('');
  const [formula, setFormula] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const [error, setError] = useState('');

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
    if (!strategyId) return;
    setLoading(true); setError('');
    try {
      const [strategyRes, versionsRes] = await Promise.all([
        fetch(`/api/strategies/${encodeURIComponent(strategyId)}`, { cache: 'no-store' }),
        fetch(`/api/strategies/${encodeURIComponent(strategyId)}/versions`, { cache: 'no-store' }),
      ]);
      const strategyJson = await strategyRes.json();
      if (!strategyRes.ok) throw new Error(strategyJson.error || '加载策略失败');
      const versionsJson = await versionsRes.json();
      setStrategy(strategyJson.strategy);
      setName(strategyJson.strategy.name);
      setFormula(strategyJson.strategy.formula);
      setTimeframe(strategyJson.strategy.timeframe);
      setVersions(versionsRes.ok ? (versionsJson.versions || []) : []);
    } catch (e) { setError(e instanceof Error ? e.message : '加载失败'); }
    finally { setLoading(false); }
  }, [strategyId]);

  useEffect(() => { if (user && strategyId) void refresh(); }, [user, strategyId, refresh]);

  const save = async () => {
    if (!strategyId || !name.trim() || !formula.trim()) return;
    setSaving(true); setError('');
    try {
      const res = await fetch(`/api/strategies/${encodeURIComponent(strategyId)}`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name.trim(), formula: formula.trim(), timeframe }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '保存失败');
      setStrategy(json.strategy);
      const versionRes = await fetch(`/api/strategies/${encodeURIComponent(strategyId)}/versions`, { cache: 'no-store' });
      const versionJson = await versionRes.json();
      if (versionRes.ok) setVersions(versionJson.versions || []);
    } catch (e) { setError(e instanceof Error ? e.message : '保存失败'); }
    finally { setSaving(false); }
  };

  const createBacktestFromVersion = (version: Pick<Version, 'name' | 'formula' | 'timeframe' | 'version_no'>) => {
    sessionStorage.setItem('bq-pending-backtest-from-selection', JSON.stringify({
      id: Number(strategyId),
      version_no: version.version_no,
      name: version.name,
      formula: version.formula,
      timeframe: version.timeframe,
    }));
    router.push('/select?tab=backtest');
  };

  const useVersion = (version: Pick<Version, 'name' | 'formula' | 'timeframe' | 'version_no'>) => {
    sessionStorage.setItem('bq-pending-selection-strategy', JSON.stringify({
      id: strategyId, name: version.name, formula: version.formula, timeframe: version.timeframe, version_no: version.version_no,
    }));
    router.push('/select');
  };

  const deleteStrategy = async () => {
    if (!strategyId || !strategy) return;
    if (!confirm(`确定删除策略“${strategy.name}”？全部版本都会删除。`)) return;
    const res = await fetch(`/api/strategies/${encodeURIComponent(strategyId)}`, { method: 'DELETE' });
    const json = await res.json();
    if (!res.ok) { alert(json.error || '删除失败'); return; }
    router.push('/strategies');
  };

  const handleLogout = useCallback(async () => {
    try { await fetch('/api/auth/logout', { method: 'POST' }); } finally { router.replace('/login'); }
  }, [router]);

  if (authLoading) return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-6xl mx-auto space-y-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <Link href="/strategies" className="text-sm font-semibold text-slate-500 hover:text-blue-600">← 策略库</Link>
              <h1 className="text-2xl font-black mt-2">{strategy?.name || '选股策略'}</h1>
              <p className="text-sm text-slate-500 mt-1">选股策略 · 当前版本 {strategy?.version_no || versions[0]?.version_no || 1}{strategy?.source_backtest_strategy_id ? ' · 提取自回测策略' : ''}</p>
            </div>
            <div className="flex gap-2">
              {strategy && <button type="button" onClick={() => useVersion({ name: strategy.name, formula: strategy.formula, timeframe: strategy.timeframe, version_no: strategy.version_no || versions[0]?.version_no || 1 })} className="px-4 py-2.5 rounded-xl bg-blue-600 text-white text-sm font-bold hover:bg-blue-700">运行选股</button>}
              {strategy && <button type="button" onClick={() => createBacktestFromVersion({ name: strategy.name, formula: strategy.formula, timeframe: strategy.timeframe, version_no: strategy.version_no || versions[0]?.version_no || 1 })} className="px-4 py-2.5 rounded-xl bg-white border border-blue-200 text-blue-600 text-sm font-bold hover:bg-blue-50">用于创建回测策略</button>}
              {strategy && <button type="button" onClick={() => void deleteStrategy()} className="px-3 py-2.5 rounded-xl bg-white border border-red-200 text-red-500 text-sm font-bold hover:bg-red-50">删除</button>}
            </div>
          </div>

          {loading ? <div className="bg-white rounded-2xl border p-12 text-center text-slate-400">加载中...</div> : error && !strategy ? <div className="bg-white rounded-2xl border border-red-200 p-8 text-red-600">{error}</div> : strategy ? <>
            {strategy?.source_backtest_strategy_id && (
              <section className="bg-white rounded-2xl border border-amber-200 shadow-sm p-5">
                <div className="text-xs font-bold text-amber-700">来源回测策略</div>
                <div className="mt-1 text-sm text-slate-700">
                  <Link href={'/strategies/backtest/' + strategy.source_backtest_strategy_id} className="font-semibold text-blue-600 hover:underline">
                    {strategy.source_backtest_strategy_name || ('回测策略 #' + strategy.source_backtest_strategy_id)}
                  </Link>
                  {' · v' + (strategy.source_backtest_strategy_version || 1)}
                </div>
                {strategy.source_backtest_strategy_trigger && <div className="mt-1 text-xs text-slate-400">Entry 触发：{strategy.source_backtest_strategy_trigger}</div>}
              </section>
            )}

            <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
              <div className="flex items-center justify-between gap-3">
                <div className="font-bold text-slate-800">当前策略</div>
                {strategy.version_no && <span className="text-xs font-bold px-2 py-1 rounded-full bg-blue-50 text-blue-600">v{strategy.version_no}</span>}
              </div>
              {error && <div className="mt-3 text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>}
              <label className="block text-xs font-bold text-slate-500 mt-5">策略名称</label>
              <input value={name} onChange={(e) => setName(e.target.value)} maxLength={80} className="mt-1 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm" />
              <label className="block text-xs font-bold text-slate-500 mt-4">策略公式</label>
              <textarea value={formula} onChange={(e) => setFormula(e.target.value)} rows={5} className="mt-1 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm font-mono resize-y" />
              <div className="mt-4 flex items-center gap-2">
                <span className="text-xs font-bold text-slate-500">周期</span>
                {['D', 'W', 'M'].map((tf) => <button key={tf} type="button" onClick={() => setTimeframe(tf)} className={`px-3 py-1.5 text-xs rounded-lg border ${timeframe === tf ? 'bg-blue-600 text-white border-blue-600' : 'border-slate-200 text-slate-600'}`}>{tf}</button>)}
              </div>
              <div className="mt-5 flex justify-end">
                <button type="button" onClick={() => void save()} disabled={saving || !name.trim() || !formula.trim()} className="px-4 py-2.5 rounded-xl bg-slate-900 text-white text-sm font-bold disabled:opacity-50">{saving ? '保存中...' : '保存为新版本'}</button>
              </div>
            </section>

            <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-100">
                <div className="font-bold text-slate-800">版本历史</div>
                <div className="text-xs text-slate-400 mt-1">历史版本只读；运行历史版本不会修改当前策略。</div>
              </div>
              {versions.length === 0 ? <div className="p-8 text-center text-sm text-slate-400">暂无版本记录（生产库执行版本迁移后会显示）</div> :
                <div className="divide-y divide-slate-100">
                  {versions.map((version) => <div key={version.id} className="p-5">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-bold text-slate-700">v{version.version_no}</span>
                      <span className="text-xs text-slate-400">{new Date(version.created_at).toLocaleString()}</span>
                      {version.version_no === (strategy.version_no || versions[0].version_no) && <span className="text-[10px] px-2 py-1 rounded-full bg-green-50 text-green-600 font-bold">当前</span>}
                    </div>
                    <div className="font-semibold text-slate-800 mt-2">{version.name}</div>
                    <div className="mt-1 text-sm font-mono text-slate-500 break-all">{version.formula}</div>
                    <div className="mt-2 flex items-center justify-between gap-2">
                      <span className="text-xs text-slate-400">周期：{version.timeframe}</span>
                      <div className="flex gap-2">
                        <button type="button" onClick={() => useVersion(version)} className="px-3 py-1.5 text-xs font-bold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50">用此版本运行</button>
                        <button type="button" onClick={() => createBacktestFromVersion(version)} className="px-3 py-1.5 text-xs font-bold text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50">以此版本创建回测</button>
                      </div>
                    </div>
                  </div>)}
                </div>}
            </section>
          </> : null}
        </div>
      </main>
    </AppShell>
  );
}

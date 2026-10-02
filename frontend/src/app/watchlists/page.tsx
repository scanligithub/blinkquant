'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import { downloadFromResponse } from '@/lib/download';

interface User { id: string; email: string; role: string }
interface WatchlistSummary {
  id: string; name: string; is_default: boolean; item_count: number;
  created_at: string; updated_at: string;
}

export default function WatchlistsPage() {
  const router = useRouter();
  const fileRef = useRef<HTMLInputElement>(null);
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [lists, setLists] = useState<WatchlistSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');

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
      const res = await fetch('/api/watchlists', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载失败');
      setLists(json.watchlists || []);
    } catch (error) { console.error('Failed to load watchlists', error); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { if (user) void refresh(); }, [user, refresh]);

  const importFile = async (file: File) => {
    try {
      const text = await file.text();
      const payload = JSON.parse(text);
      const res = await fetch('/api/watchlists/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '导入失败');
      await refresh();
      const skipped = Array.isArray(json.skipped) ? json.skipped.length : 0;
      alert(`已导入 ${Number(json.imported || 0)} 个自选股列表${skipped ? `，跳过 ${skipped} 个重复或无效列表` : ''}`);
    } catch (error) {
      alert(error instanceof Error ? error.message : '导入文件格式无效');
    } finally {
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  const create = async () => {
    const trimmed = name.trim();
    if (!trimmed) return;
    try {
      const res = await fetch('/api/watchlists', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: trimmed }),
      });
      const json = await res.json();
      if (!res.ok) { alert(json.error || '创建失败'); return; }
      setName(''); setShowCreate(false);
      if (json.watchlist?.id) router.push(`/watchlists/${json.watchlist.id}`);
      else await refresh();
    } catch { alert('创建失败'); }
  };

  const handleLogout = useCallback(async () => {
    try { await fetch('/api/auth/logout', { method: 'POST' }); }
    finally { router.replace('/login'); }
  }, [router]);

  if (authLoading) return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-6xl mx-auto space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-black">自选股</h1>
              <p className="text-sm text-slate-500 mt-1">创建多个自选股列表；导入/导出可迁移全部列表及股票。</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <input ref={fileRef} type="file" accept="application/json,.json" className="hidden" onChange={(e) => { const file = e.target.files?.[0]; if (file) void importFile(file); }} />
              <button type="button" onClick={() => fileRef.current?.click()} className="px-3 py-2.5 rounded-xl bg-white border border-slate-200 text-slate-600 text-sm font-bold hover:bg-slate-50">导入</button>
              <button type="button" onClick={() => void downloadFromResponse('/api/watchlists/export')} className="px-3 py-2.5 rounded-xl bg-white border border-slate-200 text-slate-600 text-sm font-bold hover:bg-slate-50">导出</button>
              <button type="button" onClick={() => setShowCreate(true)} className="px-4 py-2.5 rounded-xl bg-blue-600 text-white text-sm font-bold hover:bg-blue-700 shadow-sm">+ 新建列表</button>
            </div>
          </div>

          {loading ? (
            <div className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-slate-400">加载中...</div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {lists.map((list) => (
                <Link key={list.id} href={`/watchlists/${list.id}`} className="bg-white rounded-2xl border border-slate-200 p-5 shadow-sm hover:shadow-md hover:border-blue-200 transition-all">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="font-bold text-slate-800 truncate">{list.name}</div>
                      <div className="text-xs text-slate-400 mt-1">{list.item_count} 只股票</div>
                    </div>
                    {list.is_default && <span className="shrink-0 text-[10px] font-bold px-2 py-1 rounded-full bg-blue-50 text-blue-600">默认</span>}
                  </div>
                  <div className="mt-6 text-sm text-blue-600 font-semibold">打开列表 →</div>
                </Link>
              ))}
            </div>
          )}

          {!loading && lists.length === 0 && <div className="text-center text-slate-400 py-4">暂无自选股列表</div>}

          {showCreate && <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={() => setShowCreate(false)}>
            <div className="bg-white rounded-2xl w-full max-w-md p-6 shadow-xl" onClick={(e) => e.stopPropagation()}>
              <h2 className="font-bold text-slate-800">新建自选股列表</h2>
              <p className="text-xs text-slate-400 mt-1">列表名称由你自定义，最多 40 个字符。</p>
              <input autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter') void create(); }} maxLength={40} placeholder="例如：核心持仓、观察池、突破候选" className="mt-4 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500" />
              <div className="mt-5 flex justify-end gap-2">
                <button type="button" onClick={() => setShowCreate(false)} className="px-4 py-2 text-sm rounded-xl border border-slate-200 text-slate-600">取消</button>
                <button type="button" onClick={() => void create()} disabled={!name.trim()} className="px-4 py-2 text-sm rounded-xl bg-blue-600 text-white font-bold disabled:opacity-50">创建</button>
              </div>
            </div>
          </div>}
        </div>
      </main>
    </AppShell>
  );
}

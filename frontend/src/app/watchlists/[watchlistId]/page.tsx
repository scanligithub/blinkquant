'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import AppShell from '@/components/app/AppShell';
import StockSearch from '@/components/StockSearch';
import Watchlist from '@/components/Watchlist';

interface User { id: string; email: string; role: string }
interface StockItem { code: string; name: string }
interface WatchlistDetail {
  id: string; name: string; is_default: boolean; item_count: number;
  codes: string[]; created_at: string; updated_at: string;
}

export default function WatchlistDetailPage() {
  const params = useParams<{ watchlistId: string }>();
  const router = useRouter();
  const watchlistId = Array.isArray(params?.watchlistId) ? params.watchlistId[0] : params?.watchlistId;
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [watchlist, setWatchlist] = useState<WatchlistDetail | null>(null);
  const [stockList, setStockList] = useState<StockItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [renameOpen, setRenameOpen] = useState(false);
  const [rename, setRename] = useState('');
  const [error, setError] = useState('');
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set());
  const [bulkLoading, setBulkLoading] = useState(false);

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
    if (!watchlistId) return;
    setLoading(true); setError('');
    try {
      const [listRes, stocksRes] = await Promise.all([
        fetch(`/api/watchlists/${encodeURIComponent(watchlistId)}`, { cache: 'no-store' }),
        fetch('/api/stock-list', { cache: 'no-store' }),
      ]);
      const listJson = await listRes.json();
      if (!listRes.ok) throw new Error(listJson.error || '加载列表失败');
      setWatchlist(listJson.watchlist);
      setRename(listJson.watchlist.name);
      setSelectedCodes(new Set());
      if (stocksRes.ok) setStockList(await stocksRes.json());
    } catch (error) { setError(error instanceof Error ? error.message : '加载失败'); }
    finally { setLoading(false); }
  }, [watchlistId]);

  useEffect(() => { if (user && watchlistId) void refresh(); }, [user, watchlistId, refresh]);

  const addCode = useCallback(async (code: string) => {
    if (!watchlistId || !watchlist || watchlist.codes.includes(code)) return;
    try {
      const res = await fetch('/api/watchlist', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code, listId: watchlistId }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '添加失败');
      setWatchlist((prev) => prev ? { ...prev, codes: [...prev.codes, code], item_count: prev.item_count + 1 } : prev);
    } catch (error) { alert(error instanceof Error ? error.message : '添加失败'); }
  }, [watchlist, watchlistId]);

  const removeCode = useCallback(async (code: string) => {
    if (!watchlistId) return;
    try {
      const res = await fetch('/api/watchlist', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ listId: watchlistId, code }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '删除失败');
      setSelectedCodes((prev) => { const next = new Set(prev); next.delete(code); return next; });
      setWatchlist((prev) => prev ? { ...prev, codes: prev.codes.filter((item) => item !== code), item_count: Math.max(0, prev.item_count - 1) } : prev);
    } catch (error) { alert(error instanceof Error ? error.message : '删除失败'); }
  }, [watchlistId]);

  const toggleSelection = useCallback((code: string) => {
    setSelectedCodes((prev) => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code); else next.add(code);
      return next;
    });
  }, []);

  const toggleAll = useCallback(() => {
    setSelectedCodes((prev) => prev.size === (watchlist?.codes.length || 0) ? new Set() : new Set(watchlist?.codes || []));
  }, [watchlist?.codes]);

  const bulkRemove = useCallback(async () => {
    if (!watchlistId || selectedCodes.size === 0 || bulkLoading) return;
    if (!confirm(`确定从“${watchlist?.name || ''}”移除已选 ${selectedCodes.size} 只股票？`)) return;
    setBulkLoading(true);
    try {
      const res = await fetch('/api/watchlist', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ listId: watchlistId, codes: Array.from(selectedCodes) }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '批量删除失败');
      const removed = Number(json.removed_count || selectedCodes.size);
      const selected = new Set(selectedCodes);
      setWatchlist((prev) => prev ? {
        ...prev,
        codes: prev.codes.filter((code) => !selected.has(code)),
        item_count: Math.max(0, prev.item_count - removed),
      } : prev);
      setSelectedCodes(new Set());
    } catch (error) { alert(error instanceof Error ? error.message : '批量删除失败'); }
    finally { setBulkLoading(false); }
  }, [bulkLoading, selectedCodes, watchlist, watchlistId]);

  const renameList = useCallback(async () => {
    if (!watchlistId || !rename.trim()) return;
    try {
      const res = await fetch(`/api/watchlists/${encodeURIComponent(watchlistId)}`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: rename.trim() }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '修改失败');
      setWatchlist((prev) => prev ? { ...prev, ...json.watchlist } : prev);
      setRenameOpen(false);
    } catch (error) { alert(error instanceof Error ? error.message : '修改失败'); }
  }, [rename, watchlistId]);

  const deleteList = useCallback(async () => {
    if (!watchlistId || !watchlist || watchlist.is_default) return;
    if (!confirm(`确定删除“${watchlist.name}”？列表中的股票也会从该列表移除。`)) return;
    try {
      const res = await fetch(`/api/watchlists/${encodeURIComponent(watchlistId)}`, { method: 'DELETE' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '删除失败');
      router.push('/watchlists');
    } catch (error) { alert(error instanceof Error ? error.message : '删除失败'); }
  }, [router, watchlist, watchlistId]);

  const handleLogout = useCallback(async () => {
    try { await fetch('/api/auth/logout', { method: 'POST' }); }
    finally { router.replace('/login'); }
  }, [router]);

  if (authLoading) return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-6xl mx-auto space-y-5">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <Link href="/watchlists" className="text-sm font-semibold text-slate-500 hover:text-blue-600">← 自选股</Link>
              {watchlist && <div className="flex items-center gap-2 mt-2">
                <h1 className="text-2xl font-black truncate max-w-[min(70vw,600px)]">{watchlist.name}</h1>
                {watchlist.is_default && <span className="text-[10px] font-bold px-2 py-1 rounded-full bg-blue-50 text-blue-600">默认</span>}
              </div>}
              <p className="text-sm text-slate-500 mt-1">{watchlist?.item_count || 0} 只股票</p>
            </div>
            {watchlist && <div className="flex items-center gap-2">
              <StockSearch stockList={stockList} onSelect={(code) => void addCode(code)} />
              <button type="button" onClick={() => setRenameOpen(true)} className="px-3 py-2 text-xs font-bold rounded-xl border border-slate-200 bg-white text-slate-600 hover:bg-slate-50">重命名</button>
              {!watchlist.is_default && <button type="button" onClick={() => void deleteList()} className="px-3 py-2 text-xs font-bold rounded-xl border border-red-200 bg-red-50 text-red-600 hover:bg-red-100">删除</button>}
            </div>}
          </div>

          {loading ? <div className="bg-white rounded-2xl border border-slate-200 p-12 text-center text-slate-400">加载中...</div> :
           error ? <div className="bg-white rounded-2xl border border-red-200 p-8 text-center text-red-600">{error}</div> :
           watchlist ? <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
             <div className="px-4 py-3 border-b border-slate-100 flex justify-between items-center">
               <div className="text-sm font-bold text-slate-700">列表股票</div>
               <div className="flex items-center gap-2">
                 {selectedCodes.size > 0 && (
                   <button type="button" onClick={() => void bulkRemove()} disabled={bulkLoading} className="px-2.5 py-1.5 text-xs font-bold rounded-lg bg-red-50 text-red-600 border border-red-200 disabled:opacity-50">
                     {bulkLoading ? '移除中…' : `批量移除（${selectedCodes.size}）`}
                   </button>
                 )}
                 <div className="text-xs text-slate-400">点击股票查看研究 · × 可移除</div>
               </div>
             </div>
             <div className="max-h-[65vh] overflow-y-auto custom-scrollbar">
               <Watchlist
                 codes={watchlist.codes}
                 onSelect={(code) => router.push(`/stocks/${encodeURIComponent(code)}`)}
                 onRemove={(code) => void removeCode(code)}
                 stockList={stockList}
                 selectable
                 selectedCodes={Array.from(selectedCodes)}
                 onToggleSelection={toggleSelection}
                 onToggleAll={toggleAll}
               />
             </div>
           </section> : null}

          {renameOpen && watchlist && <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={() => setRenameOpen(false)}>
            <div className="bg-white rounded-2xl w-full max-w-md p-6 shadow-xl" onClick={(e) => e.stopPropagation()}>
              <h2 className="font-bold text-slate-800">重命名自选股列表</h2>
              <input autoFocus value={rename} onChange={(e) => setRename(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter') void renameList(); }} maxLength={40} className="mt-4 w-full border border-slate-200 rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500" />
              <div className="mt-5 flex justify-end gap-2">
                <button type="button" onClick={() => setRenameOpen(false)} className="px-4 py-2 text-sm rounded-xl border border-slate-200 text-slate-600">取消</button>
                <button type="button" onClick={() => void renameList()} disabled={!rename.trim()} className="px-4 py-2 text-sm rounded-xl bg-blue-600 text-white font-bold disabled:opacity-50">保存</button>
              </div>
            </div>
          </div>}
        </div>
      </main>
    </AppShell>
  );
}

'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';

interface WatchlistSummary {
  id: string;
  name: string;
  is_default: boolean;
  item_count: number;
  contains?: boolean;
}

interface WatchlistPickerProps {
  code: string;
  compact?: boolean;
}

export default function WatchlistPicker({ code, compact = false }: WatchlistPickerProps) {
  const router = useRouter();
  const rootRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState('');
  const [lists, setLists] = useState<WatchlistSummary[]>([]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/watchlists?code=${encodeURIComponent(code)}`, { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载自选股列表失败');
      setLists(json.watchlists || []);
    } catch (error) {
      console.error('Failed to load watchlists', error);
    } finally {
      setLoading(false);
    }
  }, [code]);

  useEffect(() => {
    if (open) void refresh();
  }, [open, refresh]);

  useEffect(() => {
    if (!open) return;
    const onMouseDown = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [open]);

  const toggle = async (list: WatchlistSummary) => {
    try {
      const res = list.contains
        ? await fetch(`/api/watchlist?listId=${encodeURIComponent(list.id)}&code=${encodeURIComponent(code)}`, { method: 'DELETE' })
        : await fetch('/api/watchlist', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code, listId: list.id }),
          });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '操作失败');
      setLists((prev) => prev.map((item) => item.id === list.id ? {
        ...item,
        contains: !list.contains,
        item_count: Math.max(0, item.item_count + (list.contains ? -1 : 1)),
      } : item));
    } catch (error) {
      alert(error instanceof Error ? error.message : '操作失败');
    }
  };

  const createList = async () => {
    const name = newName.trim();
    if (!name) return;
    try {
      const res = await fetch('/api/watchlists', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '创建列表失败');
      setNewName('');
      setCreating(false);
      await refresh();
    } catch (error) {
      alert(error instanceof Error ? error.message : '创建列表失败');
    }
  };

  const selectedCount = lists.filter((list) => list.contains).length;
  const label = selectedCount > 0 ? `★ 已自选 ${selectedCount}` : '☆ 加自选';

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className={`${compact ? 'px-2.5 py-1.5' : 'px-3 py-1.5'} text-xs font-bold text-amber-600 border border-amber-200 bg-amber-50 rounded-lg hover:bg-amber-100`}
      >
        {label}
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 z-40 w-72 bg-white rounded-xl shadow-lg border border-slate-200 p-2">
          <div className="px-2 py-1.5 flex items-center justify-between gap-2">
            <span className="text-xs font-bold text-slate-500">加入自选股列表</span>
            <span className="text-[10px] font-mono text-slate-300 truncate">{code}</span>
          </div>
          {loading ? (
            <div className="px-2 py-5 text-xs text-slate-400 text-center">加载列表...</div>
          ) : (
            <div className="max-h-56 overflow-y-auto">
              {lists.map((list) => (
                <button
                  key={list.id}
                  type="button"
                  onClick={() => void toggle(list)}
                  className="w-full flex items-center justify-between gap-2 px-2 py-2 rounded-lg hover:bg-slate-50 text-left"
                >
                  <span className="text-sm text-slate-700 truncate">{list.name}</span>
                  <span className={`text-xs font-bold ${list.contains ? 'text-blue-600' : 'text-slate-300'}`}>{list.contains ? '✓' : '+'}</span>
                </button>
              ))}
            </div>
          )}
          {creating ? (
            <div className="mt-1 pt-2 border-t border-slate-100">
              <input
                autoFocus
                value={newName}
                onChange={(event) => setNewName(event.target.value)}
                onKeyDown={(event) => { if (event.key === 'Enter') void createList(); }}
                maxLength={40}
                placeholder="列表名称"
                className="w-full border border-slate-200 rounded-lg px-2.5 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
              />
              <div className="mt-2 flex justify-end gap-2">
                <button type="button" onClick={() => { setCreating(false); setNewName(''); }} className="px-2.5 py-1.5 text-xs text-slate-500 rounded-lg border border-slate-200">取消</button>
                <button type="button" onClick={() => void createList()} disabled={!newName.trim()} className="px-2.5 py-1.5 text-xs text-white bg-blue-600 rounded-lg font-bold disabled:opacity-50">创建</button>
              </div>
            </div>
          ) : (
            <div className="mt-1 pt-1 border-t border-slate-100 flex gap-1">
              <button type="button" onClick={() => setCreating(true)} className="flex-1 px-2 py-2 text-xs font-bold text-blue-600 rounded-lg hover:bg-blue-50 text-left">+ 新建列表</button>
              <button type="button" onClick={() => { setOpen(false); router.push('/watchlists'); }} className="px-2 py-2 text-xs font-bold text-slate-500 rounded-lg hover:bg-slate-50">管理</button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

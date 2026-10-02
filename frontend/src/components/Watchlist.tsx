'use client';

import { useEffect, useState } from 'react';

interface WatchlistProps {
  codes: string[];
  selectedCode?: string | null;
  onSelect: (code: string) => void;
  onRemove: (code: string) => void;
  stockList: Array<{ code: string; name: string }>;
}

export default function Watchlist({ codes, selectedCode, onSelect, onRemove, stockList }: WatchlistProps) {
  const nameOf = (code: string) => stockList.find((s) => s.code === code)?.name || code;
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [removing, setRemoving] = useState(false);
  const [message, setMessage] = useState('');

  useEffect(() => {
    setSelected((prev) => new Set(Array.from(prev).filter((code) => codes.includes(code))));
  }, [codes]);

  const allSelected = codes.length > 0 && selected.size === codes.length;
  const toggleAll = () => setSelected(allSelected ? new Set() : new Set(codes));
  const toggle = (code: string) => setSelected((prev) => {
    const next = new Set(prev);
    if (next.has(code)) next.delete(code); else next.add(code);
    return next;
  });

  const removeSelected = async () => {
    if (!selected.size || removing) return;
    setRemoving(true);
    setMessage('');
    try {
      const response = await fetch('/api/watchlist', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes: Array.from(selected) }),
      });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error || '批量移除失败');
      Array.from(selected).forEach((code) => onRemove(code));
      setMessage(`已移除 ${json.removed_count ?? selected.size} 只`);
      setSelected(new Set());
    } catch (error) {
      setMessage(error instanceof Error ? error.message : '批量移除失败');
    } finally {
      setRemoving(false);
    }
  };

  return (
    <div className="p-2">
      {codes.length === 0 ? (
        <div className="text-sm text-slate-400 text-center py-8">暂无自选股</div>
      ) : (
        <>
          <div className="mb-2 flex flex-wrap items-center gap-2 px-2 py-1">
            <button type="button" onClick={toggleAll} className="px-2.5 py-1.5 text-xs font-bold rounded-lg border border-slate-200 bg-white hover:bg-slate-100">{allSelected ? '取消全选' : '全选'}</button>
            <span className="text-xs text-slate-500">已选 {selected.size} / {codes.length}</span>
            <button type="button" onClick={() => void removeSelected()} disabled={removing || selected.size === 0} className="px-2.5 py-1.5 text-xs font-bold rounded-lg border border-red-200 bg-red-50 text-red-600 disabled:opacity-50">{removing ? '移除中…' : '批量移除'}</button>
            {message && <span className="text-[11px] text-slate-500">{message}</span>}
          </div>
          {codes.map((code) => (
            <div
              key={code}
              className={`w-full text-left px-4 py-3 rounded-lg flex justify-between items-center group ${selectedCode === code ? 'bg-blue-50 text-blue-700 font-bold border border-blue-100' : 'hover:bg-slate-50 text-slate-600'}`}
            >
              <input type="checkbox" checked={selected.has(code)} onChange={() => toggle(code)} aria-label={`选择 ${code}`} className="mr-2 shrink-0" />
              <button className="flex-1 text-left truncate" onClick={() => onSelect(code)}>
                <span className="truncate block">{nameOf(code)}</span>
                <span className="text-xs font-mono text-slate-400">{code}</span>
              </button>
              <button
                onClick={() => onRemove(code)}
                className="ml-2 text-slate-300 hover:text-red-500 text-sm px-1"
                aria-label="删除自选"
              >
                ×
              </button>
            </div>
          ))}
        </>
      )}
    </div>
  );
}

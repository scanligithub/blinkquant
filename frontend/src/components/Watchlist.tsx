'use client';

interface WatchlistProps {
  codes: string[];
  selectedCode?: string | null;
  onSelect: (code: string) => void;
  onRemove: (code: string) => void;
  stockList: Array<{ code: string; name: string }>;
}

export default function Watchlist({ codes, selectedCode, onSelect, onRemove, stockList }: WatchlistProps) {
  const nameOf = (code: string) => stockList.find((s) => s.code === code)?.name || code;

  return (
    <div className="p-2">
      {codes.length === 0 ? (
        <div className="text-sm text-slate-400 text-center py-8">暂无自选股</div>
      ) : (
        codes.map((code) => (
          <div
            key={code}
            className={`w-full text-left px-4 py-3 rounded-lg flex justify-between items-center group ${selectedCode === code ? 'bg-blue-50 text-blue-700 font-bold border border-blue-100' : 'hover:bg-slate-50 text-slate-600'}`}
          >
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
        ))
      )}
    </div>
  );
}

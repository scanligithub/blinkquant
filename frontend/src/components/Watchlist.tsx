'use client';

interface WatchlistProps {
  codes: string[];
  selectedCode?: string | null;
  onSelect: (code: string) => void;
  onRemove: (code: string) => void;
  stockList: Array<{ code: string; name: string }>;
  selectable?: boolean;
  selectedCodes?: string[];
  onToggleSelection?: (code: string) => void;
  onToggleAll?: () => void;
}

export default function Watchlist({
  codes,
  selectedCode,
  onSelect,
  onRemove,
  stockList,
  selectable = false,
  selectedCodes = [],
  onToggleSelection,
  onToggleAll,
}: WatchlistProps) {
  const nameOf = (code: string) => stockList.find((s) => s.code === code)?.name || code;
  const selected = new Set(selectedCodes);
  const allSelected = codes.length > 0 && selected.size === codes.length;

  return (
    <div className="p-2">
      {selectable && codes.length > 0 && (
        <div className="mb-2 px-2 py-2 flex items-center gap-2 border-b border-slate-100">
          <input
            type="checkbox"
            aria-label="全选自选股"
            checked={allSelected}
            onChange={onToggleAll}
          />
          <span className="text-xs text-slate-500">已选 {selected.size} / {codes.length}</span>
        </div>
      )}
      {codes.length === 0 ? (
        <div className="text-sm text-slate-400 text-center py-8">暂无自选股</div>
      ) : (
        codes.map((code) => (
          <div
            key={code}
            className={`w-full text-left px-4 py-3 rounded-lg flex justify-between items-center group ${selectedCode === code ? 'bg-blue-50 text-blue-700 font-bold border border-blue-100' : 'hover:bg-slate-50 text-slate-600'}`}
          >
            {selectable && (
              <input
                type="checkbox"
                aria-label={`选择 ${code}`}
                checked={selected.has(code)}
                onChange={() => onToggleSelection?.(code)}
                className="mr-2"
              />
            )}
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

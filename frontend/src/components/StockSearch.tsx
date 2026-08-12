'use client';
import { useEffect, useState } from 'react';
import { searchStocks, type StockItem } from '../utils/stockSearch';

interface StockSearchProps {
  stockList: StockItem[];
  onSelect: (code: string) => void;
}

export default function StockSearch({ stockList, onSelect }: StockSearchProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<StockItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [overlayOpen, setOverlayOpen] = useState(false);

  const inputClass =
    'flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full text-sm md:text-base';

  useEffect(() => {
    if (query.length < 1 || stockList.length === 0) {
      setResults([]);
      setLoading(false);
      return;
    }
    setResults([]);
    setLoading(true);
    const handler = setTimeout(() => {
      setResults(searchStocks(stockList, query));
      setLoading(false);
    }, 300);
    return () => clearTimeout(handler);
  }, [query, stockList]);

  const select = (code: string) => {
    onSelect(code);
    setQuery('');
    setResults([]);
    setOverlayOpen(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && query.trim() !== '') {
      if (results.length > 0) {
        select(results[0].code);
      } else {
        const isNumeric = /^[0-9]+$/.test(query.trim());
        if (isNumeric) {
          const qNumeric = query.trim();
          const found = stockList.find((s) => s.code.replace(/^(sh|sz|bj)\./, '') === qNumeric);
          if (found) select(found.code);
        } else {
          const qL = query.toLowerCase();
          const found = stockList.find(
            (s) => s.code.toLowerCase().startsWith(qL) || s.name.toLowerCase().startsWith(qL)
          );
          if (found) select(found.code);
        }
      }
    }
  };

  const renderResults = () =>
    results.map((stock) => (
      <button
        key={stock.code}
        onClick={() => select(stock.code)}
        className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center"
      >
        <span className="font-medium text-slate-900">{stock.name}</span>
        <span className="text-sm font-mono text-slate-500">{stock.code}</span>
      </button>
    ));

  return (
    <>
      {/* 桌面端：工具栏内联搜索框（md+ 显示） */}
      <div className="relative hidden md:block w-64 shrink-0">
        <input
          className={inputClass}
          placeholder="搜索股票：名称/代码/拼音"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
        />
        {loading && (
          <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
            <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
          </div>
        )}
        {query.length > 1 && results.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 z-30 bg-white border rounded-xl shadow-lg max-h-60 overflow-y-auto custom-scrollbar">
            {renderResults()}
          </div>
        )}
      </div>

      {/* 移动端：搜索图标按钮（md- 显示） */}
      <button
        onClick={() => setOverlayOpen(true)}
        className="md:hidden px-3 py-2 text-slate-600 border border-slate-200 bg-white rounded-xl hover:bg-slate-100 transition-colors"
        aria-label="搜索股票"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
      </button>

      {/* 移动端：覆盖层搜索 */}
      {overlayOpen && (
        <div
          className="fixed inset-0 z-50 bg-black/40 flex items-start justify-center p-4 md:hidden"
          onClick={() => setOverlayOpen(false)}
        >
          <div className="bg-white rounded-2xl w-full max-w-md shadow-xl p-4" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center gap-2">
              <div className="relative flex-1">
                <input
                  autoFocus
                  className={inputClass}
                  placeholder="搜索股票：名称/代码/拼音"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={handleKeyDown}
                />
                {loading && (
                  <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
                    <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                  </div>
                )}
              </div>
              <button
                onClick={() => setOverlayOpen(false)}
                className="shrink-0 px-3 py-2 text-sm font-bold text-slate-600 border border-slate-200 bg-white rounded-xl hover:bg-slate-100 transition-colors"
              >
                关闭
              </button>
            </div>
            {query.length > 1 && results.length > 0 && (
              <div className="mt-2 max-h-60 overflow-y-auto custom-scrollbar">{renderResults()}</div>
            )}
          </div>
        </div>
      )}
    </>
  );
}

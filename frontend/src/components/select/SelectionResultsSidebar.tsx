'use client';

import dynamic from 'next/dynamic';
import type { BacktestParams } from '../BacktestPanel';
import BacktestPanel from '../BacktestPanel';
import BacktestResults from '../BacktestResults';
import { TaskList } from '../TaskList';
import WatchlistPicker from '../WatchlistPicker';

const Watchlist = dynamic(() => import('../Watchlist'), { ssr: false });

type SidebarTab = 'results' | 'watchlist' | 'backtest';


interface WatchlistSummary {
  id: string;
  name: string;
  item_count: number;
}

function BulkWatchlistBar({ codes, onChanged }: { codes: string[]; onChanged?: () => void | Promise<void> }) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [lists, setLists] = useState<WatchlistSummary[]>([]);
  const [listId, setListId] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');

  useEffect(() => {
    setSelected((prev) => new Set([...prev].filter((code) => codes.includes(code))));
  }, [codes]);

  useEffect(() => {
    if (!codes.length) return;
    fetch('/api/watchlists', { cache: 'no-store' }).then(async (res) => {
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载列表失败');
      setLists(json.watchlists || []);
      if (!listId && json.watchlists?.[0]) setListId(String(json.watchlists[0].id));
    }).catch((error) => setMessage(error instanceof Error ? error.message : '加载列表失败'));
  }, [codes.length]);

  const allSelected = codes.length > 0 && selected.size === codes.length;
  const toggleAll = () => setSelected(allSelected ? new Set() : new Set(codes));
  const toggle = (code: string) => setSelected((prev) => {
    const next = new Set(prev);
    if (next.has(code)) next.delete(code); else next.add(code);
    return next;
  });
  const addSelected = async () => {
    if (!listId || selected.size === 0) return;
    setLoading(true); setMessage('');
    try {
      const res = await fetch('/api/watchlist', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ listId, codes: [...selected] }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加入自选股失败');
      setMessage(`已加入 ${json.added_count ?? selected.size} 只`);
      setSelected(new Set());
      await onChanged?.();
    } catch (error) { setMessage(error instanceof Error ? error.message : '加入自选股失败'); }
    finally { setLoading(false); }
  };
  return <div className="mb-2 rounded-xl border border-slate-200 bg-slate-50 p-2">
    <div className="flex flex-wrap items-center gap-2">
      <button type="button" onClick={toggleAll} className="px-2.5 py-1.5 text-xs font-bold rounded-lg border border-slate-200 bg-white hover:bg-slate-100">{allSelected ? '取消全选' : '全选'}</button>
      <span className="text-xs text-slate-500">已选 {selected.size} / {codes.length}</span>
      <select value={listId} onChange={(e) => setListId(e.target.value)} className="min-w-32 flex-1 max-w-52 px-2 py-1.5 text-xs border border-slate-200 rounded-lg bg-white">
        {lists.map((list) => <option key={list.id} value={list.id}>{list.name} ({list.item_count})</option>)}
      </select>
      <button type="button" onClick={() => void addSelected()} disabled={loading || !listId || selected.size === 0} className="px-2.5 py-1.5 text-xs font-bold text-white bg-blue-600 rounded-lg disabled:opacity-50">{loading ? '加入中…' : '批量加入'}</button>
    </div>
    {message && <div className="mt-1 text-[11px] text-slate-500">{message}</div>}
    <div className="mt-2 flex flex-wrap gap-1">{codes.slice(0, 200).map((code) => <label key={code} className="inline-flex items-center gap-1 px-1.5 py-1 rounded bg-white border border-slate-100 text-[10px]"><input type="checkbox" checked={selected.has(code)} onChange={() => toggle(code)} />{code}</label>)}</div>
    {codes.length > 200 && <div className="mt-1 text-[10px] text-slate-400">仅显示前 200 只股票的勾选项；“全选”仍作用于全部结果。</div>}
  </div>;
}
\ninterface SelectionResultsSidebarProps {
  sidebarTab: SidebarTab;
  onTabChange: (tab: SidebarTab) => void;
  selectMeta: { date?: string | null; degraded?: boolean } | null;
  results: string[];
  watchlistCodes: string[];
  selectedCode: string | null | undefined;
  stockList: Array<{ code: string; name: string }>;
  onViewStock: (code: string) => void;
  onRemoveWatchlist: (code: string) => void;\n  onWatchlistChanged?: () => void | Promise<void>;
  formula: string;
  backtestLoading: boolean;
  backtestResult: any;
  onBacktest: (params: BacktestParams) => void | Promise<void>;
  onTemplateSelected: (id: number | null) => void;
  isAdmin: boolean;
}

export default function SelectionResultsSidebar(props: SelectionResultsSidebarProps) {
  const {
    sidebarTab, onTabChange, selectMeta, results, watchlistCodes, selectedCode,
    stockList, onViewStock, onRemoveWatchlist, onWatchlistChanged, formula, backtestLoading,
    backtestResult, onBacktest, onTemplateSelected, isAdmin,
  } = props;

  return (
      <aside className="lg:col-span-1 order-1 lg:order-1">
            <div className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm">
              <div className="p-4 border-b flex justify-between items-center bg-slate-50/50">
                <div className="flex gap-1">
                  <button
                    onClick={() => onTabChange('results')}
                    className={`px-3 py-1 text-xs font-bold rounded-lg ${sidebarTab === 'results' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-200/50'}`}
                  >
                    结果
                  </button>
                  <button
                    onClick={() => onTabChange('watchlist')}
                    className={`px-3 py-1 text-xs font-bold rounded-lg ${sidebarTab === 'watchlist' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-200/50'}`}
                  >
                    自选
                  </button>
                  <button
                    onClick={() => onTabChange('backtest')}
                    className={`px-3 py-1 text-xs font-bold rounded-lg ${sidebarTab === 'backtest' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-200/50'}`}
                  >
                    回测
                  </button>
                </div>
                {sidebarTab === 'results' && (
                  <>
                    {selectMeta?.date && (
                      <span className="bg-slate-100 text-slate-600 text-xs px-2 py-0.5 rounded-full font-mono">
                        {selectMeta.date}
                      </span>
                    )}
                    <span className="bg-blue-100 text-blue-700 text-xs px-2 py-0.5 rounded-full font-mono">{results.length}</span>
                  </>
                )}
              </div>
              {sidebarTab === 'results' && selectMeta?.degraded && (
                <div className="mx-2 mt-2 text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
                  部分计算节点响应失败，本次结果可能不完整，建议重试。
                </div>
              )}
              {sidebarTab === 'watchlist' ? (
                <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                  <Watchlist
                    codes={watchlistCodes}
                    selectedCode={selectedCode}
                    onSelect={onViewStock}
                    onRemove={onRemoveWatchlist}
                    stockList={stockList}
                  />
                </div>
              ) : sidebarTab === 'backtest' ? (
                <div className="flex-1 overflow-y-auto p-2 custom-scrollbar space-y-4">
                  <BacktestPanel
                    initialFormula={formula}
                    onRun={onBacktest}
                    loading={backtestLoading}
                    onTemplateSelected={onTemplateSelected}
                  />
                  {backtestResult && (
                    <BacktestResults
                      result={backtestResult.legacy}
                      taskId={backtestResult.taskId}
                      summary={backtestResult.summary}
                    />
                  )}
                  <TaskList isAdmin={isAdmin} />
                </div>
              ) : (
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.map(code => {
                  const name = stockList.find(s => s.code === code)?.name || code;
                  return (
                    <div key={code} className={`rounded-lg mb-1 ${selectedCode === code ? 'bg-blue-50 border border-blue-100' : ''}`}>
                    <div className="flex items-center gap-2 px-2">
                      <button onClick={() => onViewStock(code)} className={`flex-1 min-w-0 text-left px-2 py-3 rounded-lg flex justify-between group ${selectedCode === code ? 'text-blue-700 font-bold' : 'hover:bg-slate-50 text-slate-600'}`}>
                        <span className="truncate">{name}</span>
                        <span className="text-xs font-mono text-slate-400 ml-2">{code}</span>
                      </button>
                      <WatchlistPicker code={code} compact />
                    </div>
                    <div className="px-4 pb-2">
                      <button
                        onClick={() => onTabChange('backtest')}
                        className="text-[10px] text-blue-500 hover:text-blue-700 font-medium"
                      >
                        回测此策略 →
                      </button>
                    </div>
                  </div>
                  );
                })}
              </div>
              )}
            </div>
      </aside>
  );
}

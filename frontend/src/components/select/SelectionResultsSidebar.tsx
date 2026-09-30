'use client';

import dynamic from 'next/dynamic';
import type { BacktestParams } from '../BacktestPanel';
import BacktestPanel from '../BacktestPanel';
import BacktestResults from '../BacktestResults';
import { TaskList } from '../TaskList';

const Watchlist = dynamic(() => import('../Watchlist'), { ssr: false });

type SidebarTab = 'results' | 'watchlist' | 'backtest';

interface SelectionResultsSidebarProps {
  sidebarTab: SidebarTab;
  onTabChange: (tab: SidebarTab) => void;
  selectMeta: { date?: string | null; degraded?: boolean } | null;
  results: string[];
  watchlistCodes: string[];
  selectedCode: string | null | undefined;
  stockList: Array<{ code: string; name: string }>;
  onViewStock: (code: string) => void;
  onRemoveWatchlist: (code: string) => void;
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
    stockList, onViewStock, onRemoveWatchlist, formula, backtestLoading,
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
                      <button onClick={() => onViewStock(code)} className={`w-full text-left px-4 py-3 rounded-lg flex justify-between group ${selectedCode === code ? 'text-blue-700 font-bold' : 'hover:bg-slate-50 text-slate-600'}`}>
                        <span className="truncate">{name}</span>
                        <span className="text-xs font-mono text-slate-400 ml-2">{code}</span>
                      </button>
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

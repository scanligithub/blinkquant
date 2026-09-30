'use client';

import type { MutableRefObject, RefObject, Dispatch, SetStateAction } from 'react';
import dynamic from 'next/dynamic';
import StockSearch from '../StockSearch';
import BacktestResults from '../BacktestResults';
import { applyAdjust } from '@/utils/applyAdjust';
import { formatMoney, formatVolume } from '@/utils/format';

const KLineChart = dynamic(() => import('../KLineChart'), {
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">加载图表引擎...</div>
});

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

const SECTOR_GROUP_ORDER = ['行业板块', '概念板块', '地域板块'];
const SECTOR_GROUP_LABELS: Record<string, string> = {
  '行业板块': '行业',
  '概念板块': '概念',
  '地域板块': '地域',
};
const SECTOR_MAX_SHOWN = 3;

export interface SelectedStock {
  kind: 'stock' | 'sector';
  code: string;
  name?: string;
  data: any;
}

interface StockResearchPanelProps {
  sidebarTab: 'results' | 'watchlist' | 'backtest';
  selectedStock: SelectedStock | null;
  stockList: Array<{code: string; name: string}>;
  sectors: { code: string; name: string; type: string }[];
  expandedSectors: Record<string, boolean>;
  chartLoading: boolean;
  dailyDataCache: any[];
  adjustedDaily: any[];
  sectorDataCache: any[];
  chartTimeframe: string;
  onChangeChartTimeframe: (value: string) => void;
  subChartType: string;
  onChangeSubChartType: (value: string) => void;
  mainChartType: string;
  onChangeMainChartType: (value: string) => void;
  isFullScreen: boolean;
  chartWrapperRef: RefObject<HTMLDivElement>;
  adjustMode: 'none'|'qfq'|'hfq';
  adjustMenuOpen: boolean;
  adjustMenuRef: RefObject<HTMLDivElement>;
  onChangeAdjustMode: (value: 'none'|'qfq'|'hfq') => void;
  onChangeAdjustMenuOpen: (value: boolean) => void;
  onChangeSectorsExpanded: Dispatch<SetStateAction<Record<string, boolean>>>;
  onViewStock: (code: string) => void;
  onViewSector: (code: string, name: string) => void;
  onToggleWatchlist: (code: string) => void;
  watchlistCodes: string[];
  lastStockRef: MutableRefObject<{ code: string; name: string } | null>;
  onReturnToStock: () => void;
  onToggleFullscreen: () => Promise<void>;
  backtestResult: any;
  backtestLoading: boolean;
  backtestEmptyText?: string;
  onSetSelectedStock: (stock: SelectedStock) => void;
  onSetSelectedStockData: (data: any[]) => void;
}

export default function StockResearchPanel({
  sidebarTab, selectedStock, stockList, sectors, expandedSectors, chartLoading,
  dailyDataCache, adjustedDaily, sectorDataCache, chartTimeframe, onChangeChartTimeframe,
  subChartType, onChangeSubChartType, mainChartType, onChangeMainChartType, isFullScreen,
  chartWrapperRef, adjustMode, adjustMenuOpen, adjustMenuRef, onChangeAdjustMode,
  onChangeAdjustMenuOpen, onChangeSectorsExpanded, onViewStock, onViewSector,
  onToggleWatchlist, watchlistCodes, lastStockRef, onReturnToStock, onToggleFullscreen,
  backtestResult, backtestLoading, backtestEmptyText, onSetSelectedStock, onSetSelectedStockData
}: StockResearchPanelProps) {
  return (
      <section className="lg:col-span-3 order-2 lg:order-2">
            {sidebarTab === 'backtest' ? (
              <div className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full p-4 overflow-y-auto">
                {backtestResult ? (
                  <BacktestResults
                    result={backtestResult.legacy}
                    taskId={backtestResult.taskId}
                    summary={backtestResult.summary}
                  />
                ) : (
                  <div className="h-full flex items-center justify-center text-slate-400">
                    {backtestLoading ? '回测计算中...' : '设置参数后点击"运行回测"'}
                  </div>
                )}
              </div>
            ) : (
            <div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
              <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
                <StockSearch stockList={stockList} onSelect={onViewStock} />
                {selectedStock && (
                  <>
                    <div className="flex flex-col items-start min-w-0">
                      <div className="flex items-baseline">
                        <span className="text-xl font-bold">{selectedStock.code}</span>
                        <span className="ml-2 text-base font-medium text-slate-500 truncate">{selectedStock.name}</span>
                      </div>
  {selectedStock.kind === 'stock' && (
    <>
      <div className="w-full mt-1 flex flex-col gap-0.5">
        {SECTOR_GROUP_ORDER.map((type) => {
          const group = sectors.filter((s) => s.type === type);
          if (group.length === 0) return null;
          const expanded = !!expandedSectors[type];
          const shown = expanded ? group : group.slice(0, SECTOR_MAX_SHOWN);
          const hidden = group.length - shown.length;
          return (
            <div key={type} className="flex flex-wrap items-center gap-1">
              <span className="text-[9px] md:text-[10px] font-bold text-slate-400 leading-none shrink-0">
                {SECTOR_GROUP_LABELS[type] || type}
              </span>
              {shown.map((s) => (
                <button
                  key={s.code}
                  onClick={() => { lastStockRef.current = { code: selectedStock.code, name: selectedStock.name || selectedStock.code }; onViewSector(s.code, s.name); }}
                  className={`text-[10px] md:text-xs px-1.5 py-0.5 rounded border font-medium transition-colors ${
                    s.type === '行业板块' ? 'bg-blue-50 text-blue-700 border-blue-200 hover:bg-blue-100'
                    : s.type === '概念板块' ? 'bg-purple-50 text-purple-700 border-purple-200 hover:bg-purple-100'
                    : 'bg-green-50 text-green-700 border-green-200 hover:bg-green-100'
                  }`}
                >
                  {s.name}
                </button>
              ))}
              {hidden > 0 && (
                <button
                  onClick={() => onChangeSectorsExpanded((prev) => ({ ...prev, [type]: !prev[type] }))}
                  className="text-[10px] md:text-xs px-1.5 py-0.5 rounded border border-dashed border-slate-300 text-slate-500 hover:bg-slate-100 font-medium transition-colors"
                >
                  {expanded ? '收起' : `+${hidden}`}
                </button>
              )}
            </div>
          );
        })}
        {Object.values(expandedSectors).some(Boolean) && (
          <button
            onClick={() => onChangeSectorsExpanded({})}
            className="self-start text-[10px] md:text-xs px-1.5 py-0.5 rounded border border-dashed border-slate-300 text-slate-500 hover:bg-slate-100 font-medium transition-colors"
          >
            收起
          </button>
        )}
      </div>
      {(() => {
        const latest = selectedStock.data[selectedStock.data.length - 1];
        if (!latest) return null;
        const items = [
          { label: 'PE(TTM)', value: latest.peTTM != null ? Number(latest.peTTM).toFixed(2) : '--' },
          { label: '总市值', value: formatMoney(latest.total_mv) },
          { label: '流通市值', value: formatMoney(latest.float_mv) },
          { label: '成交额', value: formatMoney(latest.amount) },
          { label: '换手率', value: latest.turn != null ? `${Number(latest.turn).toFixed(2)}%` : '--' },
          { label: '成交量', value: formatVolume(latest.volume) },
        ];
        return (
          <div className="w-full mt-2 flex flex-wrap items-center gap-x-3 gap-y-0.5">
            {items.map(it => (
              <span key={it.label} className="text-[9px] md:text-xs text-slate-500 whitespace-nowrap">
                {it.label}: <span className="font-mono font-medium text-slate-900">{it.value}</span>
              </span>
            ))}
          </div>
        );
      })()}
    </>
  )}
                    </div>
  
                    <div className="flex flex-wrap items-center gap-2">
                      {selectedStock?.kind === 'sector' && (
                        <button
                          onClick={() => {
                            const last = lastStockRef.current;
                            if (!last) return;
                            onReturnToStock();
                          }}
                          className="px-3 py-1 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-md mr-2 hover:bg-slate-100 transition-colors"
                        >
                          ← 返回 {lastStockRef.current?.name || '个股'}
                        </button>
                      )}
                      {selectedStock?.kind === 'stock' && (
                        <button
                          onClick={() => toggleWatchlist(selectedStock.code)}
                          className="px-3 py-1 text-xs font-bold text-amber-600 border border-amber-200 bg-amber-50 rounded-md mr-2 hover:bg-amber-100 transition-colors"
                        >
                          {watchlistCodes.includes(selectedStock.code) ? '★ 已自选' : '☆ 加自选'}
                        </button>
                      )}
                    <div className="flex items-center bg-slate-50 rounded-lg p-1 border border-slate-200">
    {/* 复权按钮 */}
    {selectedStock?.kind === 'stock' && (
    <div className="relative" ref={adjustMenuRef}>
      <button
        onClick={() => onChangeAdjustMenuOpen(!adjustMenuOpen)}
        className="px-3 py-1 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-md mr-2 hover:bg-slate-100 transition-colors"
      >
        {ADJUST_LABELS[adjustMode]} ▼
      </button>
      {adjustMenuOpen && (
        <div className="absolute left-0 top-full mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 min-w-[120px]">
          {ADJUST_OPTIONS.map(opt => (
            <button
              key={opt.value}
              onClick={() => {
                onChangeAdjustMode(opt.value);
                localStorage.setItem('klineAdjustMode', opt.value);
                onChangeAdjustMenuOpen(false);
                if (selectedStock?.kind === 'stock' && dailyDataCache.length > 0) {
                  const adjusted = applyAdjust(dailyDataCache, opt.value);
                  onSetSelectedStockData(resampleData(adjusted, chartTimeframe));
                }
              }}
              className={`w-full text-left px-3 py-1 text-xs ${adjustMode === opt.value ? 'bg-blue-50 text-blue-600' : 'text-slate-600 hover:bg-slate-100'}`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
    )}
                        <button
                          onClick={async () => {
                            if (!document.fullscreenElement) {
                              // 进入全屏
                              try {
                                await chartWrapperRef.current?.requestFullscreen();
                                
                                // 移动端处理
                                if (isMobile()) {
                                  if (isIOS()) {
                                    // iOS：显示横屏提示
                                    setShowRotateHint(true);
                                  } else {
                                    // Android：强制横屏
                                    try {
                                      await (screen.orientation as any).lock('landscape');
                                    } catch (e) {
                                      console.log('Orientation lock not supported:', e);
                                    }
                                  }
                                }
                              } catch (e) {
                                console.log('Fullscreen request failed:', e);
                              }
                            } else {
                              // 退出全屏
                              try {
                                await onToggleFullscreen();
                                setShowRotateHint(false);
                                // 释放屏幕方向锁定
                                if (screen.orientation && screen.orientation.unlock) {
                                  screen.orientation.unlock();
                                }
                              } catch (e) {
                                console.log('Exit fullscreen failed:', e);
                              }
                            }
                          }}
                          className="px-3 py-1 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-md mr-2 hover:bg-slate-100 transition-colors"
                        >
                          {isFullScreen ? '退出全屏' : '全屏'}
                        </button>
                        {TIMEFRAMES.map((tf) => (
                          <button key={tf.value} onClick={() => {
                              onChangeChartTimeframe(tf.value);
                              if (selectedStock?.kind === 'sector') {
                                if (sectorDataCache.length > 0) onChangeChartTimeframe(tf.value);
                              } else if (adjustedDaily && adjustedDaily.length > 0) {
                                onChangeChartTimeframe(tf.value);
                              }
                            }}
                            className={`px-3 py-1 text-xs font-bold rounded-md ${chartTimeframe === tf.value ? 'bg-blue-600 text-white shadow' : 'text-slate-500 hover:bg-slate-200/50'}`}
                          >
                            {tf.label}
                          </button>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </div>
              
              <div className="flex-1 w-full h-full relative p-1">
                {chartLoading && <div className="absolute inset-0 z-20 bg-white/60 backdrop-blur-sm flex items-center justify-center"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div></div>}
                {selectedStock ? (
                  <KLineChart
                    code={selectedStock.code}
                    data={selectedStock.data}
                    subChartType={subChartType}
                    onSubChartTypeChange={setSubChartType}
                    mainChartType={mainChartType}
                    onMainChartTypeChange={setMainChartType}
                  />
                ) : (
                  <div className="h-full flex items-center justify-center text-slate-400 bg-slate-50">选择股票查看图表</div>
                )}
              </div>
            </div>
            )}
      </section>
  );
}

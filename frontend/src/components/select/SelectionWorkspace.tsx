'use client';
import { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { useRouter } from 'next/navigation';
import AISelectModal from '../AISelectModal';
import type { BacktestParams } from '../BacktestPanel';
import { useCluster } from '@/hooks/useCluster';
import AppShell from '../app/AppShell';
import SelectionControls from './SelectionControls';
import SelectionResultsSidebar from './SelectionResultsSidebar';
import StockResearchPanel from './StockResearchPanel';
import useStockResearch from '@/hooks/useStockResearch';

const StrategyList = dynamic(() => import('../StrategyList'), { ssr: false });


// 板块分组显示配置：行业常驻，概念/地域超过阈值折叠
function nodeIdFromHealthCard(node: any, idx: number): "node1" | "node2" | "node3" {
  const n = node?.node;
  if (n === 0 || n === "0") return "node1";
  if (n === 1 || n === "1") return "node2";
  if (n === 2 || n === "2") return "node3";
  return (["node1", "node2", "node3"] as const)[idx] ?? "node1";
}

function queueForSlot(
  queues: ReturnType<typeof import("@/hooks/useCluster").useCluster>["queues"],
  slot: "node1" | "node2" | "node3"
) {
  if (!queues) return null;
  if (slot === "node1") return queues.selection;
  if (slot === "node2") return queues.backtest;
  return queues.running;
}

function queueItemLine(it: {
  id: number;
  status: string;
  task_type_zh?: string;
  assigned_node?: string | null;
  progress_pct?: number | null;
  date_range?: string | null;
  formula_preview?: string | null;
}): string {
  if (it.status === "running") {
    const pct = it.progress_pct != null ? ` ${Math.round(it.progress_pct)}%` : "";
    const where = it.assigned_node ? `@${it.assigned_node}` : "";
    return `#${it.id} ${it.task_type_zh || ""}${where}${pct}`;
  }
  const extra = it.date_range || it.formula_preview || "";
  return `#${it.id} ${extra}`.trim();
}

export default function SelectionWorkspace() {
  const router = useRouter();
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [showStrategies, setShowStrategies] = useState(false);
  const [saveStrategyOpen, setSaveStrategyOpen] = useState(false);
  const [showAISelect, setShowAISelect] = useState(false);
  const [sidebarTab, setSidebarTab] = useState<'results' | 'watchlist' | 'backtest'>('results');
  const [backtestResult, setBacktestResult] = useState<any>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestTemplateId, setBacktestTemplateId] = useState<number | null>(null);
  const backtestLoadingRef = useRef(false);
  const [strategyName, setStrategyName] = useState('');
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [selectDate, setSelectDate] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const {
    chartTimeframe,
    subChartType,
    mainChartType,
    setSubChartType,
    setMainChartType,
    changeChartTimeframe,
    isFullScreen,
    chartWrapperRef,
    adjustMode,
    adjustMenuOpen,
    adjustMenuRef,
    changeAdjustMode,
    setAdjustMenuOpen,
    selectedStock,
    clearSelectedStock,
    chartLoading,
    sectors,
    expandedSectors,
    setExpandedSectors,
    lastStockRef,
    stockList,
    viewStock,
    viewSector,
    toggleFullscreen,
    returnToStock,
  } = useStockResearch();

  const [clusterStatus, setClusterStatus] = useState<any>(null);
  const [watchlistCodes, setWatchlistCodes] = useState<string[]>([]);

  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/status');
      const json = await res.json();
      setClusterStatus(json);
    } catch (e) { console.error("Monitor failed", e); }
  };

  useEffect(() => {
    fetchStatus();
    const timer = setInterval(fetchStatus, 5000); 
    return () => clearInterval(timer);
  }, []);

  const refreshWatchlist = useCallback(async () => {
    try {
      const res = await fetch('/api/watchlist', { cache: 'no-store' });
      if (res.ok) {
        const json = await res.json();
        setWatchlistCodes(json.codes || []);
      }
    } catch (e) {
      console.error('Failed to load watchlist', e);
    }
  }, []);

  useEffect(() => {
    if (user) refreshWatchlist();
  }, [user, refreshWatchlist]);

  // 刷新恢复：检查localStorage中的未完成回测任务
  useEffect(() => {
    const savedJobId = localStorage.getItem('backtestJobId');
    const savedNode = localStorage.getItem('backtestNode');
    // 清理超过 1 小时的旧记录，防止卡死
    const savedTime = localStorage.getItem('backtestTime');
    if (savedTime && Date.now() - parseInt(savedTime) > 3600000) {
      localStorage.removeItem('backtestJobId');
      localStorage.removeItem('backtestNode');
      localStorage.removeItem('backtestTime');
    }
    if (savedJobId && savedNode) {
      const pollSavedJob = async () => {
        setBacktestLoading(true);
        let retries = 0;
        const maxRetries = 20; // 最多重试 20 次（约 1 分钟）
        try {
          while (retries < maxRetries) {
            await new Promise((r) => setTimeout(r, 3000));
            retries++;
            try {
              const pollRes = await fetch(`${savedNode}/api/v1/backtest/async/${savedJobId}`, {
                signal: AbortSignal.timeout(60000),
              });
              if (!pollRes.ok) {
                const text = await pollRes.text();
                throw new Error(`HTTP ${pollRes.status}: ${text.slice(0, 200)}`);
              }
              const result = await pollRes.json();
              if (result.status === 'done') {
                setBacktestResult(result.data);
                localStorage.removeItem('backtestJobId');
                localStorage.removeItem('backtestNode');
                localStorage.removeItem('backtestTime');
                return;
              }
              if (result.status === 'failed' || result.status === 'cancelled' || result.status === 'expired') {
                localStorage.removeItem('backtestJobId');
                localStorage.removeItem('backtestNode');
                localStorage.removeItem('backtestTime');
                return;
              }
              // queued/running -> continue
            } catch (e) {
              console.error('Poll error (retry', retries, '):', e);
              // 继续重试
            }
          }
          alert('恢复回测超时，请重新运行');
        } catch (e) {
          console.error('Failed to poll saved job:', e);
        } finally {
          localStorage.removeItem('backtestJobId');
          localStorage.removeItem('backtestNode');
          localStorage.removeItem('backtestTime');
          setBacktestLoading(false);
        }
      };
      pollSavedJob();
    }
  }, []);

  // Clear any stale backtest state on mount (e.g., from previous sessions)
  useEffect(() => {
    localStorage.removeItem('backtestJobId');
    localStorage.removeItem('backtestNode');
    localStorage.removeItem('backtestTime');
  }, []);

  // Listen for openBacktestResult from TaskList
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail;
      if (detail?.taskId) {
        setSidebarTab('backtest');
        setBacktestResult({ taskId: detail.taskId, summary: detail.summary });
      }
    };
    window.addEventListener('openBacktestResult', handler);
    return () => window.removeEventListener('openBacktestResult', handler);
  }, []);

  // Safety: force reset backtestLoading if stuck > 2 minutes
  useEffect(() => {
    let timer: NodeJS.Timeout;
    if (backtestLoading) {
      timer = setTimeout(() => {
        console.warn('backtestLoading stuck > 2min, force reset');
        setBacktestLoading(false);
        localStorage.removeItem('backtestJobId');
        localStorage.removeItem('backtestNode');
        localStorage.removeItem('backtestTime');
      }, 120000);
    }
    return () => { if (timer) clearTimeout(timer); };
  }, [backtestLoading]);

  const toggleWatchlist = useCallback(async (code: string) => {
    const exists = watchlistCodes.includes(code);
    try {
      if (exists) {
        await fetch(`/api/watchlist?code=${encodeURIComponent(code)}`, { method: 'DELETE' });
        setWatchlistCodes((prev) => prev.filter((c) => c !== code));
      } else {
        await fetch('/api/watchlist', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code }),
        });
        setWatchlistCodes((prev) => [...prev, code]);
      }
    } catch (e) {
      console.error('Failed to toggle watchlist', e);
    }
  }, [watchlistCodes]);

  const handleLogout = useCallback(async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
    } catch (e) {
      console.error('Logout failed', e);
    }
    // Clear any stale backtest state on logout
    localStorage.removeItem('backtestJobId');
    localStorage.removeItem('backtestNode');
    localStorage.removeItem('backtestTime');
    router.replace('/login');
  }, [router]);

  const handleSaveStrategy = useCallback(async () => {
    if (!strategyName.trim()) return;
    try {
      const res = await fetch('/api/strategies', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: strategyName.trim(), formula, timeframe }),
      });
      const json = await res.json();
      if (!res.ok) {
        alert(json.error || '保存失败');
        return;
      }
      setStrategyName('');
      setSaveStrategyOpen(false);
    } catch (e) {
      alert('保存失败');
    }
  }, [strategyName, formula, timeframe]);

  // HF 节点配置 (兼容旧逻辑，逐步迁移到新 API)
const HF_NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

const SUBMIT_TIMEOUT = 240000; // 240s for cold start
const POLL_TIMEOUT = 60000;

  const handleBacktest = async (params: BacktestParams) => {
    setBacktestLoading(true);
    setBacktestResult(null);
    try {
      // 使用新的任务队列 API
      const taskId = await submitTask('backtest', params, {
        strategyTemplateId: backtestTemplateId,
      });
      
      // 轮询任务状态
      const pollTaskStatus = async (taskId: number) => {
        while (true) {
          await new Promise((r) => setTimeout(r, 3000));
          try {
            const res = await fetch(`/api/v1/tasks/${taskId}`, { cache: 'no-store' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const task = await res.json();
            
            if (task.status === 'done') {
              if (task.result) {
                setBacktestResult({ legacy: task.result });
              } else if (task.result_uri) {
                setBacktestResult({ taskId: task.id, summary: task.result_summary });
              } else {
                setBacktestResult({ taskId: task.id, summary: task.result_summary });
              }
              return;
            }
            if (task.status === 'failed') {
              alert(`回测失败: ${task.error || '未知错误'}`);
              return;
            }
            if (task.status === 'cancelled') {
              alert('回测任务已取消');
              return;
            }
            if (task.status === 'preempted') {
              alert('回测被选股任务抢占，已自动重新排队');
              return;
            }
            // queued/running -> continue
          } catch (e: any) {
            console.error('Poll error:', e);
            // 继续重试
          }
        };
};
      await pollTaskStatus(taskId);
    } catch (e: any) {
      alert(`回测失败: ${e.message}`);
    } finally {
      setBacktestLoading(false);
    }
  };

  const handleApplyStrategy = useCallback((strategyFormula: string, strategyTimeframe: string) => {
    setFormula(strategyFormula);
    setTimeframe(strategyTimeframe);
    setShowStrategies(false);
  }, []);

  const handleSelect = async (overrides?: { formula?: string; timeframe?: string; date?: string }) => {
    setLoading(true); setResults([]); clearSelectedStock(); setSelectMeta(null);
    const f = overrides?.formula ?? formula;
    const t = overrides?.timeframe ?? timeframe;
    const d = overrides?.date;
    try {
      // 使用新的任务队列 API 进行选股
      const taskId = await submitTask('selection', { formula: f, timeframe: t, ...(d ? { date: d } : {}) });
      
      // 轮询选股任务状态
      const pollSelection = async (taskId: number) => {
        while (true) {
          await new Promise((r) => setTimeout(r, 2000));
          try {
            const res = await fetch(`/api/v1/tasks/${taskId}`, { cache: 'no-store' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const task = await res.json();
            
            if (task.status === 'done') {
              // 选股结果直接在 task.result 中
              // 支持两种格式：
              // 1. 调度器格式：{ nodes: {...}, codes: [...] }
              // 2. 直连节点格式：{ success: true, data: [...], date, meta }
              let codes: string[] | null = null;
              let signalDate: string | null = null;
              let degraded = false;

              if (Array.isArray(task.result?.codes)) {
                // 调度器格式
                codes = task.result.codes;
                signalDate = task.result.nodes?.node1?.signal_date ?? null;
              } else if (task.result?.success && Array.isArray(task.result?.data)) {
                // 直连节点格式
                codes = task.result.data;
                signalDate = task.result.date ?? null;
                degraded = !!task.result.meta?.degraded;
              }

              if (codes) {
                setResults(codes);
                setSelectMeta({ date: signalDate, degraded });
              } else {
                alert(`Selection failed: ${task.result?.error || '未知错误'}`);
              }
              return;
            }
            if (task.status === 'failed') {
              alert(`Selection failed: ${task.error || '未知错误'}`);
              return;
            }
            if (task.status === 'cancelled') {
              alert('选股任务已取消');
              return;
            }
            // queued/running -> continue
          } catch (e: any) {
            console.error('Selection poll error:', e);
            // 继续重试
          }
        }
      };
      
      await pollSelection(taskId);
    } catch (err) { 
      alert('Gateway connection failed'); 
    }
    setLoading(false);
  };

  if (authLoading) {
    return (
      <main className="min-h-screen flex items-center justify-center bg-slate-50 text-slate-900 font-sans">
        <div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
      </main>
    );
  }

  return (
    <AppShell
      user={user}
      onLogout={handleLogout}
      onOpenWatchlist={() => setSidebarTab('watchlist')}
      onOpenStrategies={() => setShowStrategies(true)}
    >
      <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-7xl mx-auto space-y-6">
        
        <section className="flex flex-col gap-4">
          <div className="flex flex-col gap-3">
            <div className="flex flex-wrap justify-center gap-3">
              {clusterStatus?.nodes?.map((node: any, idx: number) => {
                const slot = nodeIdFromHealthCard(node, idx);
                const role = (clusterNodes || []).find((n) => n.node_id === slot);
                const q = queueForSlot(queues, slot);

                return (
                  <div
                    key={idx}
                    className={`text-xs md:text-sm font-mono px-3 py-2 rounded-lg border shadow-sm min-w-[200px] max-w-[280px] ${
                      node.online ? 'bg-white border-slate-200' : 'bg-red-50 border-red-200'
                    }`}
                  >
                    <div className="flex items-center gap-2 flex-wrap">
                      <div className={`w-2.5 h-2.5 md:w-3 md:h-3 rounded-full ${node.online ? 'bg-green-500' : 'bg-red-500'}`}></div>
                      <span className="font-bold text-slate-700 text-sm md:text-base">Node {node.node ?? idx}</span>
                      <span className={`text-xs uppercase font-bold px-2 py-0.5 rounded-full ${
                        node.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                      }`}>
                        {node.status || 'OFFLINE'}
                      </span>
                    </div>

                    <div className="mt-1.5 text-sm font-semibold text-slate-800">
                      {role?.display_label || (node.online ? '—' : '离线')}
                    </div>

                    {node.online ? (
                      <div className="mt-2 space-y-1">
                        <div className="flex justify-between gap-3">
                          <span className="text-slate-500 text-xs">进程内存</span>
                          <span className="font-mono font-medium text-slate-900">{node.process_memory_gb} GB</span>
                        </div>
                        <div className="flex justify-between gap-3">
                          <span className="text-slate-500 text-xs">系统空闲</span>
                          <span className="font-mono font-bold text-blue-600">{node.system_memory_free_gb ?? node.system_memory_available_gb} GB</span>
                        </div>
                        <div className="flex justify-between gap-3">
                          <span className="text-slate-500 text-xs">磁盘空闲</span>
                          <span className="font-mono text-slate-900">{node.disk_free_gb} GB</span>
                        </div>
                        <div className="flex justify-between gap-3">
                          <span className="text-slate-500 text-xs">数据行</span>
                          <span className="font-mono text-slate-500">{node.rows_daily?.toLocaleString()}</span>
                        </div>
                      </div>
                    ) : null}

                    {q && (
                      <div className="mt-2 pt-2 border-t border-slate-100">
                        <div className="font-medium text-slate-600 text-xs">
                          {q.title}{' '}
                          <span className={q.count > 0 ? 'text-slate-900' : 'text-slate-400'}>
                            {q.count}
                          </span>
                        </div>
                        {q.count > 0 && (
                          <div className="mt-1 space-y-0.5 max-h-24 overflow-y-auto">
                            {q.items.map((it) => (
                              <div key={it.id} className="pl-1 text-slate-600 truncate text-xs">
                                {queueItemLine(it)}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </section>

        <SelectionControls
          formula={formula}
          selectDate={selectDate}
          loading={loading}
          onFormulaChange={setFormula}
          onDateChange={setSelectDate}
          onRun={() => handleSelect({ date: selectDate || undefined })}
          onAISelect={() => setShowAISelect(true)}
          onSaveStrategy={() => setSaveStrategyOpen(true)}
        />

        {/* Results Area */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 md:gap-6">
          <SelectionResultsSidebar
            sidebarTab={sidebarTab}
            onTabChange={setSidebarTab}
            selectMeta={selectMeta}
            results={results}
            watchlistCodes={watchlistCodes}
            selectedCode={selectedStock?.code}
            stockList={stockList}
            onViewStock={viewStock}
            onRemoveWatchlist={toggleWatchlist}
            formula={formula}
            backtestLoading={backtestLoading}
            backtestResult={backtestResult}
            onBacktest={handleBacktest}
            onTemplateSelected={setBacktestTemplateId}
            isAdmin={user?.role === 'admin'}
          />
          <StockResearchPanel
            sidebarTab={sidebarTab}
            selectedStock={selectedStock}
            stockList={stockList}
            sectors={sectors}
            expandedSectors={expandedSectors}
            chartLoading={chartLoading}
            chartTimeframe={chartTimeframe}
            onChangeChartTimeframe={changeChartTimeframe}
            subChartType={subChartType}
            onChangeSubChartType={setSubChartType}
            mainChartType={mainChartType}
            onChangeMainChartType={setMainChartType}
            isFullScreen={isFullScreen}
            chartWrapperRef={chartWrapperRef}
            adjustMode={adjustMode}
            adjustMenuOpen={adjustMenuOpen}
            adjustMenuRef={adjustMenuRef}
            onChangeAdjustMode={changeAdjustMode}
            onChangeAdjustMenuOpen={setAdjustMenuOpen}
            onChangeSectorsExpanded={setExpandedSectors}
            onViewStock={viewStock}
            onViewSector={viewSector}
            onToggleWatchlist={toggleWatchlist}
            watchlistCodes={watchlistCodes}
            lastStockRef={lastStockRef}
            onReturnToStock={returnToStock}
            onToggleFullscreen={toggleFullscreen}
            backtestResult={backtestResult}
            backtestLoading={backtestLoading}
          />
        </div>
      </div>

      {showStrategies && (
        <StrategyList
          onApply={handleApplyStrategy}
          onClose={() => setShowStrategies(false)}
        />
      )}

      {saveStrategyOpen && (
        <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={() => setSaveStrategyOpen(false)}>
          <div
            className="bg-white rounded-2xl w-full max-w-md shadow-xl p-6"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="font-bold text-slate-700 mb-4">保存为策略</h2>
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">策略名称</label>
            <input
              value={strategyName}
              onChange={(e) => setStrategyName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSaveStrategy()}
              className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
              placeholder="例如：突破20日均线"
              autoFocus
            />
            <div className="mt-4 text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 font-mono break-all">{formula}</div>
            <div className="mt-6 flex justify-end gap-2">
              <button onClick={() => setSaveStrategyOpen(false)} className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50">取消</button>
              <button onClick={handleSaveStrategy} disabled={!strategyName.trim()} className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50">保存</button>
            </div>
          </div>
        </div>
      )}

      {showAISelect && (
        <AISelectModal
          onClose={() => setShowAISelect(false)}
          onRun={(formula, timeframe, date) => {
            setShowAISelect(false);
            handleSelect({ formula, timeframe, date });
          }}
        />
      )}
      </main>
    </AppShell>
  );
}

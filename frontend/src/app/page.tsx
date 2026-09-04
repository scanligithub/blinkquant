'use client';
import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import dynamic from 'next/dynamic';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import StockSearch from '../components/StockSearch';
import AISelectModal from '../components/AISelectModal';
import BacktestPanel from '../components/BacktestPanel';
import BacktestResults from '../components/BacktestResults';
import ClusterStatusBar from '../components/ClusterStatusBar';
import TaskList from '../components/TaskList';
import { useCluster } from '@/hooks/useCluster';

const KLineChart = dynamic(() => import('../components/KLineChart'), {
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">加载图表引擎...</div>
});

const Watchlist = dynamic(() => import('../components/Watchlist'), { ssr: false });
const StrategyList = dynamic(() => import('../components/StrategyList'), { ssr: false });

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { downloadFromResponse } from '@/lib/download';
import { applyAdjust, ADJUST_LABELS, ADJUST_OPTIONS } from '../utils/applyAdjust';
import { parseParquetRecords } from '../utils/parquet';
import { formatMoney, formatVolume } from '../utils/format';

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

// 板块分组显示配置：行业常驻，概念/地域超过阈值折叠
const SECTOR_GROUP_ORDER = ['行业板块', '概念板块', '地域板块'];
const SECTOR_GROUP_LABELS: Record<string, string> = {
  '行业板块': '行业',
  '概念板块': '概念',
  '地域板块': '地域',
};
const SECTOR_MAX_SHOWN = 3;

export default function Home() {
  const router = useRouter();
  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [showStrategies, setShowStrategies] = useState(false);
  const [saveStrategyOpen, setSaveStrategyOpen] = useState(false);
  const [showAISelect, setShowAISelect] = useState(false);
  const [sidebarTab, setSidebarTab] = useState<'results' | 'watchlist' | 'backtest'>('results');
  const [backtestResult, setBacktestResult] = useState<any>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const backtestLoadingRef = useRef(false);
  const [strategyName, setStrategyName] = useState('');
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [selectDate, setSelectDate] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const [chartTimeframe, setChartTimeframe] = useState('D');
  const [subChartType, setSubChartType] = useState('MACD');
  const [mainChartType, setMainChartType] = useState('MA'); // 新增：主图指标切换状态

  // 集群状态管理
  const { 
    nodes, 
    queueStats, 
    myTasks, 
    canRunSelection, 
    canRunBacktest, 
    idleNodeCount,
    submitTask,
    cancelTask 
  } = useCluster();
  
  const [isFullScreen, setIsFullScreen] = useState(false);
  const [showRotateHint, setShowRotateHint] = useState(false);
  const chartWrapperRef = useRef<HTMLDivElement>(null);
const [adjustMode, setAdjustMode] = useState<'none'|'qfq'|'hfq'>('none');
const [adjustMenuOpen, setAdjustMenuOpen] = useState(false);
const adjustMenuRef = useRef<HTMLDivElement>(null);

// Load saved adjust mode from localStorage on mount
useEffect(() => {
  const saved = localStorage.getItem('klineAdjustMode');
  if (saved === 'none' || saved === 'qfq' || saved === 'hfq') {
    setAdjustMode(saved as any);
  }
}, []);

// Close adjust dropdown when clicking outside
useEffect(() => {
  const handleClickOutside = (e: MouseEvent) => {
    if (adjustMenuOpen && adjustMenuRef.current && !adjustMenuRef.current.contains(e.target as Node)) {
      setAdjustMenuOpen(false);
    }
  };
  document.addEventListener('mousedown', handleClickOutside);
  return () => document.removeEventListener('mousedown', handleClickOutside);
}, [adjustMenuOpen]);


  
  // 检测是否为iOS设备
  const isIOS = () => {
    return /iPad|iPhone|iPod/.test(navigator.userAgent) ||
           (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  };
  
  // 检测是否为移动端
  const isMobile = () => {
    return window.innerWidth < 768;
  };
  
  useEffect(() => {
    const handler = () => {
      const isFullscreen = !!document.fullscreenElement;
      setIsFullScreen(isFullscreen);
      
      // 退出全屏时隐藏横屏提示
      if (!isFullscreen) {
        setShowRotateHint(false);
        // 释放屏幕方向锁定
        if (screen.orientation && screen.orientation.unlock) {
          screen.orientation.unlock();
        }
      }
    };
    document.addEventListener('fullscreenchange', handler);
    return () => document.removeEventListener('fullscreenchange', handler);
  }, []);
  
  // 监听屏幕方向变化
  useEffect(() => {
    const handleOrientationChange = () => {
      // 如果是横屏，隐藏提示
      if (window.innerWidth > window.innerHeight) {
        setShowRotateHint(false);
      }
    };
    
    window.addEventListener('resize', handleOrientationChange);
    window.addEventListener('orientationchange', handleOrientationChange);
    
    return () => {
      window.removeEventListener('resize', handleOrientationChange);
      window.removeEventListener('orientationchange', handleOrientationChange);
    };
  }, []);

  useEffect(() => {
    let mounted = true;
    const checkSession = async () => {
      try {
        const res = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (json.user) {
          setUser(json.user);
          setAuthLoading(false);
        } else {
          // Not logged in: clear any stale backtest state
          localStorage.removeItem('backtestJobId');
          localStorage.removeItem('backtestNode');
          localStorage.removeItem('backtestTime');
          router.replace('/login');
        }
      } catch (e) {
        if (mounted) router.replace('/login');
      }
    };
    checkSession();
    return () => { mounted = false; };
  }, [router]);

  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectMeta, setSelectMeta] = useState<{ date?: string | null; degraded?: boolean } | null>(null);
  const [selectedStock, setSelectedStock] = useState<{kind: 'stock' | 'sector'; code: string; name?: string; data: any} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [dailyDataCache, setDailyDataCache] = useState<any[]>([]);
  const [sectorDataCache, setSectorDataCache] = useState<any[]>([]);
  const [sectors, setSectors] = useState<{ code: string; name: string; type: string }[]>([]);
  const [expandedSectors, setExpandedSectors] = useState<Record<string, boolean>>({});
  const lastStockRef = useRef<{ code: string; name: string } | null>(null);

  const adjustedDaily = useMemo(() => applyAdjust(dailyDataCache, adjustMode), [dailyDataCache, adjustMode]); 

  const [stockList, setStockList] = useState<Array<{code: string; name: string}>>([]);
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

  useEffect(() => {
    const loadStockList = async () => {
      const CACHE_KEY = 'stockListCache_v1';
      const CACHE_EXPIRY_MS = 24 * 60 * 60 * 1000; // 24 小时过期
      const cachedStr = localStorage.getItem(CACHE_KEY);
      if (cachedStr) {
        try {
          const cachedData = JSON.parse(cachedStr);
          // 检查是否在有效期内
          if (Date.now() - cachedData.timestamp < CACHE_EXPIRY_MS) {
            setStockList(cachedData.list);
            return;
          }
        } catch (e) {
          // 解析失败或格式不对，直接跳过走网络请求
          console.warn('Cache parse failed, fetching fresh list');
        }
      }
      try {
        const res = await fetch('/api/stock-list');
        if (!res.ok) throw new Error('Failed to load stock list');
        const data = await res.json();
        setStockList(data);
        // 存入带时间戳的对象
        localStorage.setItem(CACHE_KEY, JSON.stringify({ timestamp: Date.now(), list: data }));
      } catch (err) {
        console.error('Failed to load stock list', err);
      }
    };
    loadStockList();
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

  const handleBacktest = async (params: {
    formula: string;
    start_date: string;
    end_signal_date: string;
    initial_cash: number;
  }) => {
    setBacktestLoading(true);
    setBacktestResult(null);
    try {
      // 使用新的任务队列 API
      const taskId = await submitTask('backtest', params);
      
      // 轮询任务状态
      const pollTaskStatus = async (taskId: number) => {
        while (true) {
          await new Promise((r) => setTimeout(r, 3000));
          try {
            const res = await fetch(`/api/v1/tasks/${taskId}`, { cache: 'no-store' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const task = await res.json();
            
            if (task.status === 'done') {
              setBacktestResult(task.result);
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
              localStorage.removeItem('backtestJobId');
              localStorage.removeItem('backtestNode');
              return;
            }
            // queued/running -> continue
          } catch (e: any) {
            console.error('Poll error:', e);
            pollErrors++;
            if (pollErrors >= 5) {
              alert(`回测轮询连续失败 ${pollErrors} 次: ${e.message}`);
              localStorage.removeItem('backtestJobId');
              localStorage.removeItem('backtestNode');
              localStorage.removeItem('backtestTime');
              return;
            }
            // 轮询出错继续重试
          }
        }
      };
      await pollResult();
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
    setLoading(true); setResults([]); setSelectedStock(null); setSelectMeta(null);
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
              if (task.result?.success) {
                setResults(task.result.data);
                setSelectMeta({ date: task.result.date ?? null, degraded: !!task.result.meta?.degraded });
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

  const resampleData = useCallback((dailyData: any[], targetTimeframe: string) => {
    if (targetTimeframe === 'D') return dailyData;
    const grouped = new Map<string, any[]>();
    
    dailyData.forEach(item => {
      const date = new Date(item.time * 1000);
      let key: string;
      if (targetTimeframe === 'W') {
        const dayOfWeek = date.getDay();
        const weekStart = new Date(date);
        weekStart.setDate(date.getDate() - dayOfWeek);
        key = weekStart.toISOString().split('T')[0];
      } else {
        key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
      }
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push(item);
    });
    
    const resampled: any[] = [];
    grouped.forEach((items) => {
      const sortedItems = items.sort((a, b) => a.time - b.time);
      const first = sortedItems[0];
      const last = sortedItems[sortedItems.length - 1];
resampled.push({
  time: first.time,
  open: first.open,
  high: Math.max(...sortedItems.map(i => i.high)),
  low: Math.min(...sortedItems.map(i => i.low)),
  close: last.close,
  volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
  amount: sortedItems.reduce((sum, i) => sum + (i.amount || 0), 0),
  turn: last.turn,
  peTTM: last.peTTM,
  total_mv: last.total_mv,
  float_mv: last.float_mv,
  main_net: sortedItems.reduce((sum, i) => sum + (i.main_net || 0), 0), // 聚合资金流数据
});
    });
    return resampled.sort((a, b) => a.time - b.time);
  }, []);

  const viewStock = useCallback(async (code: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/kline?code=${code}&timeframe=D`);
      if (!res.ok) throw new Error('Fetch failed');
      const buffer = await res.arrayBuffer();
      if (buffer.byteLength === 0) throw new Error('Empty buffer');
      
      const records = await parquetReadObjects({ file: buffer, compressors });
      if (!records || records.length === 0) throw new Error('Empty records');

      const dailyData = parseParquetRecords(records);

setDailyDataCache(dailyData);
       const adjusted = applyAdjust(dailyData, adjustMode);
       const resampledData = resampleData(adjusted, chartTimeframe);
       const stock = stockList.find(s => s.code === code);
       setSelectedStock({ kind: 'stock', code, name: stock?.name || code, data: resampledData });
       // 懒加载该股票的板块标签（失败静默）
       try {
         const sectorRes = await fetch(`/api/stock-sectors?code=${encodeURIComponent(code)}`);
         if (sectorRes.ok) {
           const sectorJson = await sectorRes.json();
           setSectors(sectorJson.sectors || []);
           setExpandedSectors({});
         } else {
           setSectors([]);
         }
       } catch (e) {
         console.warn('Failed to load stock sectors:', e);
         setSectors([]);
       }
    } catch (err: any) { alert(`Failed: ${err.message}`); } 
    finally { setChartLoading(false); }
  }, [chartTimeframe, stockList, resampleData, adjustMode]);

  const viewSector = useCallback(async (sectorCode: string, sectorName: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/sector-kline?code=${encodeURIComponent(sectorCode)}&timeframe=D`);
      if (!res.ok) throw new Error('Fetch failed');
      const buffer = await res.arrayBuffer();
      if (buffer.byteLength === 0) throw new Error('Empty buffer');

      const records = await parquetReadObjects({ file: buffer, compressors });
      if (!records || records.length === 0) throw new Error('Empty records');

      const sectorDaily = parseParquetRecords(records);
      setSectorDataCache(sectorDaily);
      setSelectedStock({ kind: 'sector', code: sectorCode, name: sectorName, data: resampleData(sectorDaily, chartTimeframe) });
    } catch (err: any) {
      alert(`Failed: ${err.message}`);
    } finally {
      setChartLoading(false);
    }
  }, [chartTimeframe, resampleData]);

  if (authLoading) {
    return (
      <main className="min-h-screen flex items-center justify-center bg-slate-50 text-slate-900 font-sans">
        <div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
      </main>
    );
  }

  return (
    <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-7xl mx-auto space-y-6">
        
        <header className="flex flex-col gap-4">
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-3 md:gap-4">
            <div>
              <h1 className="text-2xl md:text-3xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">BlinkQuant</h1>
              <p className="text-slate-500 text-xs md:text-sm mt-1">分布式计算集群</p>
            </div>
            {user && (
              <div className="relative z-40">
                <button
                  onClick={() => setUserMenuOpen((o) => !o)}
                  className="flex items-center gap-2 bg-white border border-slate-200 rounded-xl px-3 py-2 shadow-sm hover:bg-slate-50 transition-colors"
                >
                  <span className="text-sm font-medium text-slate-700 max-w-[160px] truncate">{user.email}</span>
                  {user.role === 'admin' && (
                    <span className="text-xs font-bold bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full">管理员</span>
                  )}
                  <svg className="w-4 h-4 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 9l-7 7-7-7" /></svg>
                </button>
                {userMenuOpen && (
                  <div className="absolute right-0 mt-2 w-48 bg-white border border-slate-200 rounded-xl shadow-lg overflow-hidden">
                    <button onClick={() => { setUserMenuOpen(false); setSidebarTab('watchlist'); }} className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50">自选股</button>
                    <button onClick={() => { setUserMenuOpen(false); setShowStrategies(true); }} className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50">我的策略</button>
                    <button onClick={() => { setUserMenuOpen(false); downloadFromResponse('/api/me/export'); }} className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50">导出我的数据</button>
                    {user.role === 'admin' && (
                      <Link href="/admin" onClick={() => setUserMenuOpen(false)} className="block w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50">管理后台</Link>
                    )}
                    <button onClick={handleLogout} className="w-full text-left px-4 py-2.5 text-sm text-red-600 border-t border-slate-100 hover:bg-red-50">退出登录</button>
                  </div>
                )}
              </div>
            )}
          </div>
          <div className="flex flex-col gap-3">
            <div className="flex justify-center">
              <div className="text-xs md:text-sm font-mono text-slate-400 bg-white px-3 py-2 rounded-lg border shadow-sm">
                集群: {clusterStatus?.cluster_health || '连接中...'}
              </div>
            </div>
            <div className="flex flex-wrap justify-center gap-3">
              {clusterStatus?.nodes?.map((node: any, idx: number) => (
                <div key={idx} className={`text-xs md:text-sm font-mono px-3 py-2 rounded-lg border shadow-sm ${node.online ? 'bg-white border-slate-200' : 'bg-red-50 border-red-200'}`}>
                  <div className="flex items-center gap-2">
                    <div className={`w-2.5 h-2.5 md:w-3 md:h-3 rounded-full ${node.online ? 'bg-green-500' : 'bg-red-500'}`}></div>
                    <span className="font-bold text-slate-700 text-sm md:text-base">Node {node.node || idx}</span>
                    <span className={`text-xs md:text-sm uppercase font-bold px-2 md:px-2.5 py-0.5 rounded-full ${
                      node.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                    }`}>
                      {node.status || 'OFFLINE'}
                    </span>
                  </div>
                  {node.online ? (
                    <div className="mt-2 space-y-1">
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">进程内存</span>
                        <div className="font-mono font-medium text-slate-900 text-right text-xs md:text-sm">{node.process_memory_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">系统空闲</span>
                        <div className="font-mono font-bold text-blue-600 text-right text-xs md:text-sm">{node.system_memory_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">磁盘空闲</span>
                        <div className="font-mono text-slate-900 text-right text-xs md:text-sm">{node.disk_free_gb} GB</div>
                      </div>
                      <div className="flex justify-between gap-3">
                        <span className="text-slate-500 text-xs md:text-sm">数据行</span>
                        <div className="font-mono text-slate-500 text-right text-xs md:text-sm">{node.rows_daily?.toLocaleString()}</div>
                      </div>
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        </header>

        {/* Formula Inputs */}
        <section className="bg-white p-4 md:p-6 rounded-2xl border border-slate-200 shadow-sm">
          <div className="flex flex-col gap-3">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">策略公式</label>
            <div className="flex flex-col sm:flex-row gap-3">
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                placeholder="例如：CLOSE > MA(CLOSE, 20)"
                value={formula} onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect({ date: selectDate || undefined })}
              />
              <input
                type="date"
                title="选股日期（留空 = 最新交易日）"
                className="bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm text-slate-600 focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                value={selectDate} onChange={(e) => setSelectDate(e.target.value)}
              />
              <button onClick={() => handleSelect({ date: selectDate || undefined })} disabled={loading} className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 rounded-xl font-bold flex items-center justify-center gap-2 min-w-[160px]">
                {loading ? <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div> : '运行选股'}
              </button>
              <button
                onClick={() => setShowAISelect(true)}
                disabled={loading}
                className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-xl font-bold"
              >
                AI 选股
              </button>
              <button
                onClick={() => setSaveStrategyOpen(true)}
                disabled={loading}
                className="bg-white border border-slate-200 hover:bg-slate-50 text-slate-600 px-4 py-2 rounded-xl font-bold"
              >
                保存为策略
              </button>
            </div>
          </div>
        </section>

        {/* Results Area */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 md:gap-6">
          <aside className="lg:col-span-1 order-1 lg:order-1">
            <div className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm">
              <div className="p-4 border-b flex justify-between items-center bg-slate-50/50">
                <div className="flex gap-1">
                  <button
                    onClick={() => setSidebarTab('results')}
                    className={`px-3 py-1 text-xs font-bold rounded-lg ${sidebarTab === 'results' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-200/50'}`}
                  >
                    结果
                  </button>
                  <button
                    onClick={() => setSidebarTab('watchlist')}
                    className={`px-3 py-1 text-xs font-bold rounded-lg ${sidebarTab === 'watchlist' ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-200/50'}`}
                  >
                    自选
                  </button>
                  <button
                    onClick={() => setSidebarTab('backtest')}
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
                    selectedCode={selectedStock?.code}
                    onSelect={(code) => viewStock(code)}
                    onRemove={(code) => toggleWatchlist(code)}
                    stockList={stockList}
                  />
                </div>
              ) : sidebarTab === 'backtest' ? (
                <div className="flex-1 overflow-y-auto p-2 custom-scrollbar space-y-4">
                  <ClusterStatusBar />
                  <BacktestPanel initialFormula={formula} onRun={handleBacktest} loading={backtestLoading} />
                  {backtestResult && <BacktestResults result={backtestResult} />}
                  <TaskList />
                </div>
              ) : (
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.map(code => {
                  const name = stockList.find(s => s.code === code)?.name || code;
                  return (
                    <div key={code} className={`rounded-lg mb-1 ${selectedStock?.code === code ? 'bg-blue-50 border border-blue-100' : ''}`}>
                      <button onClick={() => viewStock(code)} className={`w-full text-left px-4 py-3 rounded-lg flex justify-between group ${selectedStock?.code === code ? 'text-blue-700 font-bold' : 'hover:bg-slate-50 text-slate-600'}`}>
                        <span className="truncate">{name}</span>
                        <span className="text-xs font-mono text-slate-400 ml-2">{code}</span>
                      </button>
                      <div className="px-4 pb-2">
                        <button
                          onClick={() => setSidebarTab('backtest')}
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

          <section className="lg:col-span-3 order-2 lg:order-2">
            {sidebarTab === 'backtest' ? (
              <div className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full p-4 overflow-y-auto">
                <ClusterStatusBar />
                {backtestResult ? (
                  <BacktestResults result={backtestResult} />
                ) : (
                  <div className="h-full flex items-center justify-center text-slate-400">
                    {backtestLoading ? '回测计算中...' : '设置参数后点击"运行回测"'}
                  </div>
                )}
              </div>
            ) : (
            <div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
              <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
                <StockSearch stockList={stockList} onSelect={viewStock} />
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
                  onClick={() => { lastStockRef.current = { code: selectedStock.code, name: selectedStock.name || selectedStock.code }; viewSector(s.code, s.name); }}
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
                  onClick={() => setExpandedSectors((prev) => ({ ...prev, [type]: !prev[type] }))}
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
            onClick={() => setExpandedSectors({})}
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
                            setSelectedStock({ kind: 'stock', code: last.code, name: last.name, data: resampleData(adjustedDaily, chartTimeframe) });
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
        onClick={() => setAdjustMenuOpen(o => !o)}
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
                setAdjustMode(opt.value);
                localStorage.setItem('klineAdjustMode', opt.value);
                setAdjustMenuOpen(false);
                if (selectedStock?.kind === 'stock' && dailyDataCache.length > 0) {
                  const adjusted = applyAdjust(dailyDataCache, opt.value);
                  setSelectedStock(prev => prev ? { ...prev, data: resampleData(adjusted, chartTimeframe) } : prev);
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
                                await document.exitFullscreen();
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
                              setChartTimeframe(tf.value);
                              if (selectedStock?.kind === 'sector') {
                                if (sectorDataCache.length > 0) setSelectedStock({ ...selectedStock, data: resampleData(sectorDataCache, tf.value) });
                              } else if (adjustedDaily && adjustedDaily.length > 0) {
                                setSelectedStock({ ...selectedStock, data: resampleData(adjustedDaily, tf.value) });
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
  );
}

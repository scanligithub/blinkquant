'use client';
import { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';

const KLineChart = dynamic(() => import('../components/KLineChart'), {
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">加载图表引擎...</div>
});

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { getPinyinInitials } from '../utils/pinyin';

const TIMEFRAMES = [
  { label: 'Daily', value: 'D' },
  { label: 'Weekly', value: 'W' },
  { label: 'Monthly', value: 'M' },
];

function formatStockCode(code: string): string {
  const numericCode = code.replace(/[^0-9]/g, ''); // Remove non-numeric characters
  if (numericCode.startsWith('6')) {
    return `sh.${numericCode}`;
  } else if (numericCode.startsWith('0') || numericCode.startsWith('3')) {
    return `sz.${numericCode}`;
  }
  return code; // Return original if no match (e.g., already prefixed or invalid)
}

export default function Home() {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [timeframe, setTimeframe] = useState('D'); // 用于策略公式选股
  const [chartTimeframe, setChartTimeframe] = useState('D'); // 用于K线图周期选择
   const [isFullScreen, setIsFullScreen] = useState(false); // 全屏状态
   const chartWrapperRef = useRef<HTMLDivElement>(null); // 用于全屏 API
   // 监听全屏变化，同步 isFullScreen 状态
   useEffect(() => {
     const handler = () => setIsFullScreen(!!document.fullscreenElement);
     document.addEventListener('fullscreenchange', handler);
     return () => document.removeEventListener('fullscreenchange', handler);
   }, []);
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
    const [selectedStock, setSelectedStock] = useState<{code: string, name?: string, data: any} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [dailyDataCache, setDailyDataCache] = useState<any[]>([]); // 缓存原始日线数据

  const [searchQuery, setSearchQuery] = useState('');

  const [searchResults, setSearchResults] = useState<{code: string; name: string}[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  
  // 本地缓存股票列表
  const [stockList, setStockList] = useState<Array<{code: string; name: string}>>([]);
  
  // 监控数据
  const [clusterStatus, setClusterStatus] = useState<any>(null);

  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/status');
      const json = await res.json();
      setClusterStatus(json);
    } catch (e) { console.error("Monitor failed", e); }
  };

  useEffect(() => {
    fetchStatus();
    const timer = setInterval(fetchStatus, 5000); // 5秒刷新一次
    return () => clearInterval(timer);
  }, []);

  // 加载股票列表并缓存到 localStorage
  useEffect(() => {
    const loadStockList = async () => {
      const cached = localStorage.getItem('stockList');
      if (cached) {
        const parsed = JSON.parse(cached);
        console.log('=== stockList 加载 ===');
        console.log('从缓存加载 stockList:', parsed.length, '条');
        console.log('缓存前5条:', parsed.slice(0, 5));
        console.log('=== stockList 加载结束 ===');
        setStockList(parsed);
        return;
      }
      try {
        const res = await fetch('/api/stock-list');
        if (!res.ok) throw new Error('Failed to load stock list');
        const data = await res.json();
        console.log('=== stockList 加载 ===');
        console.log('从 API 加载 stockList:', data.length, '条');
        console.log('API 前5条:', data.slice(0, 5));
        console.log('=== stockList 加载结束 ===');
        setStockList(data);
        localStorage.setItem('stockList', JSON.stringify(data));
      } catch (err) {
        console.error('Failed to load stock list:', err);
      }
    };
    loadStockList();
  }, []);

  // 前端本地搜索（支持拼音首字母与打分排序）
  useEffect(() => {
    // 输入至少1个字符开始搜索
    if (searchQuery.length < 1 || stockList.length === 0) {
      setSearchResults([]);
      setSearchLoading(false);
      return;
    }
    setSearchLoading(true);
    const handler = setTimeout(() => {
      const qLower = searchQuery.toLowerCase();
      const qPinyin = getPinyinInitials(searchQuery);

      // 1. 遍历计算得分
      const scoredResults = stockList.map(stock => {
        const { code, name } = stock;
        // 过滤掉空名和指数
        if (!name || !name.trim() || code.includes('.000')) {
            return { ...stock, score: 0 };
        }

        const nameLower = name.toLowerCase();
        const codeLower = code.toLowerCase();
        const namePinyin = getPinyinInitials(name);

        let score = 0;

        // 【规则 A】完全精确匹配 (最高优先级)
        if (codeLower === qLower || nameLower === qLower) score += 1000;

        // 【规则 B】前缀匹配 (高优先级：如输入 'zg' 匹配 '中国平安 zgpa')
        if (codeLower.startsWith(qLower)) score += 100;
        if (namePinyin.startsWith(qPinyin)) score += 80;
        if (nameLower.startsWith(qLower)) score += 80;

        // 【规则 C】包含匹配 (低优先级：如输入 'zg' 匹配 '上证国企 szgq')
        if (codeLower.includes(qLower)) score += 10;
        if (namePinyin.includes(qPinyin)) score += 5;
        if (nameLower.includes(qLower)) score += 5;

        return { ...stock, score };
      });

      // 2. 过滤得分为0的，按分数降序排序，取前10
      const results = scoredResults
        .filter(item => item.score > 0)
        .sort((a, b) => b.score - a.score)
        .map(({ code, name }) => ({ code, name })) // 剥离 score 属性，保持原结构
        .slice(0, 10);

      setSearchResults(results);
      setSearchLoading(false);
    }, 300); // 防抖时间缩短到 300ms，让搜索感觉更跟手
    
    return () => clearTimeout(handler);
  }, [searchQuery, stockList]);

  const handleSelect = async () => {
    setLoading(true);
    setResults([]);
    setSelectedStock(null);
    try {
      const res = await fetch('/api/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe })
      });
      const json = await res.json();
      if (json.success) setResults(json.data);
      else alert(`Selection failed: ${json.error}`);
    } catch (err) { alert('Gateway connection failed'); }
    setLoading(false);
  };

  // 重采样函数：将日线数据转换为周线或月线
  const resampleData = useCallback((dailyData: any[], targetTimeframe: string) => {
    if (targetTimeframe === 'D') return dailyData;
    
    const grouped = new Map<string, any[]>();
    
    dailyData.forEach(item => {
      const date = new Date(item.time * 1000);
      let key: string;
      
      if (targetTimeframe === 'W') {
        // 周线：使用该周的第一天作为key
        const dayOfWeek = date.getDay();
        const weekStart = new Date(date);
        weekStart.setDate(date.getDate() - dayOfWeek);
        key = weekStart.toISOString().split('T')[0];
      } else {
        // 月线：使用该月的第一天作为key
        key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-01`;
      }
      
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(item);
    });
    
    // 对每个分组进行聚合
    const resampled: any[] = [];
    grouped.forEach((items) => {
      const sortedItems = items.sort((a, b) => a.time - b.time);
      const first = sortedItems[0];
      const last = sortedItems[sortedItems.length - 1];
      
      resampled.push({
        time: first.time, // 使用该周期的第一个交易日
        open: first.open,
        high: Math.max(...sortedItems.map(i => i.high)),
        low: Math.min(...sortedItems.map(i => i.low)),
        close: last.close,
        volume: sortedItems.reduce((sum, i) => sum + i.volume, 0),
      });
    });
    
    return resampled.sort((a, b) => a.time - b.time);
  }, []);

  const viewStock = useCallback(async (code: string) => {
    setChartLoading(true);
    try {
      // 只请求日线数据
      const res = await fetch(`/api/kline?code=${code}&timeframe=D`);

      if (!res.ok) {
        let errorMessage = 'Fetch failed';
        const contentType = res.headers.get('Content-Type');
        if (contentType && contentType.includes('application/json')) {
          try {
            const errorJson = await res.json();
            errorMessage = errorJson.error || errorJson.detail || errorMessage;
          } catch (jsonError) {
            errorMessage = `Fetch failed: ${res.status} ${res.statusText}`;
          }
        } else {
          const errorText = await res.text();
          errorMessage = `Fetch failed: ${res.status} ${res.statusText}: ${errorText || errorMessage}`;
        }
        throw new Error(errorMessage);
      }

      const buffer = await res.arrayBuffer();

      if (buffer.byteLength === 0) {
        throw new Error('Received empty data buffer for kline');
      }

      const records = await parquetReadObjects({ file: buffer, compressors });

      if (!records || records.length === 0) {
        throw new Error('Received empty or invalid Parquet data');
      }

      // 格式化日线数据
      const dailyData = records.map((record) => {
        let timeValue;

        if (record.date === null || record.date === undefined) {
            throw new Error('Date record is null or undefined in K-line data');
        } else if (record.date instanceof Date) {
          timeValue = Math.floor(record.date.getTime() / 1000);
        } else {
          throw new Error('Invalid date format received from K-line data');
        }

        return {
          time: timeValue,
          open: record.open,
          high: record.high,
          low: record.low,
          close: record.close,
          volume: record.volume,
        };
      });

      // 保存原始日线数据到缓存
      setDailyDataCache(dailyData);

      // 根据chartTimeframe进行重采样
      const resampledData = resampleData(dailyData, chartTimeframe);

      // 从 stockList 中查找股票名称
      const stock = stockList.find(s => s.code === code);
      const stockName = stock?.name || code;
      
      setSelectedStock({ code, name: stockName, data: resampledData });
    } catch (err: any) {
      console.error('Failed to load kline:', err);
      alert(`Failed to load kline: ${err.message || err}`);
    } finally {
      setChartLoading(false);
    }
  }, [chartTimeframe, stockList, resampleData]);

  return (
    <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Header Title */}
        <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <h1 className="text-3xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
              BlinkQuant
            </h1>
            <p className="text-slate-500 text-sm mt-1">分布式计算集群</p>
          </div>
          
          <div className="text-xs font-mono text-slate-400 bg-white px-3 py-1 rounded border border-slate-200 shadow-sm">
            状态: {clusterStatus?.cluster_health || '连接中...'}
          </div>
        </header>

        {/* Monitor Dashboard (直接平铺) */}
        {clusterStatus && (
          <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {clusterStatus.nodes.map((node: any, idx: number) => (
              <div key={idx} className={`p-4 rounded-xl border shadow-sm transition-all ${
                  node.online ? 'bg-white border-slate-200' : 'bg-red-50 border-red-200'
                }`}>
                <div className="flex justify-between items-center mb-3">
                  <div className="flex items-center gap-2">
                    <div className={`w-2.5 h-2.5 rounded-full ${node.online ? 'bg-green-500' : 'bg-red-500'}`}></div>
                    <span className="font-bold text-slate-700">Node {node.node || idx}</span>
                  </div>
                  <span className={`text-[10px] uppercase font-bold px-2 py-0.5 rounded-full ${
                    node.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                  }`}>
                    {node.status || 'OFFLINE'}
                  </span>
                </div>
                
                {node.online ? (
                  <div className="grid grid-cols-2 gap-y-2 text-xs">
                    <div className="text-slate-500">RAM (App):</div>
                    <div className="font-mono font-medium text-slate-900 text-right">{node.process_memory_gb} GB</div>
                    
                    <div className="text-slate-500">RAM (Free):</div>
                    <div className="font-mono font-bold text-blue-600 text-right">{node.system_memory_free_gb} GB</div>
                    
                    <div className="text-slate-500">Disk Free:</div>
                    <div className="font-mono text-slate-900 text-right">{node.disk_free_gb} GB</div>
                    
                    <div className="text-slate-500">Rows:</div>
                    <div className="font-mono text-slate-500 text-right">{node.rows_daily?.toLocaleString()}</div>
                  </div>
                ) : (
                  <div className="text-xs text-red-500 font-medium py-4 text-center">Connection Failed</div>
                )}
              </div>
            ))}
          </section>
        )}

        {/* Input Controls */}
        <section className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
          {/* Search Input */}
          <div className="flex flex-col gap-4 mb-6">
            <label className="text-sm font-bold text-slate-500 uppercase tracking-wider">搜索股票</label>
            <div className="relative z-20">
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full"
                placeholder="例如：000952, 平安, PA"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={async (e) => {
                  console.log('onKeyDown triggered. Key:', e.key, 'SearchQuery:', searchQuery);
                  if (e.key === 'Enter' && searchQuery.trim() !== '') {
                    console.log('Enter key pressed with search query:', searchQuery);
                    if (searchResults.length > 0) {
                      // If there are search results, use the first one
                      console.log('Viewing stock from search results:', searchResults[0].code);
                      viewStock(searchResults[0].code);
                      setSearchQuery('');
                      setSearchResults([]);
                    } else {
                      // No search results, determine if searchQuery is a code or a name
                      const isNumericCode = /^[0-9]+$/.test(searchQuery.trim());
                      if (isNumericCode) {
                        // It's a numeric code, format and view directly
                        console.log('Viewing stock as numeric code:', searchQuery);
                        viewStock(formatStockCode(searchQuery));
                        setSearchQuery('');
                        setSearchResults([]);
                      } else {
                        // It's likely a name, perform immediate local search
                        const qLower = searchQuery.toLowerCase();
                        const qPinyin = getPinyinInitials(searchQuery);
                        
                        // 优先寻找"前缀匹配"的最优解
                        let found = stockList.find(({code, name}) => {
                          return (
                            code.toLowerCase().startsWith(qLower) ||
                            name.toLowerCase().startsWith(qLower) ||
                            (qPinyin && getPinyinInitials(name).startsWith(qPinyin))
                          );
                        });

                        // 如果没有前缀匹配，降级寻找"包含匹配"
                        if (!found) {
                            found = stockList.find(({code, name}) => {
                                return (
                                  code.toLowerCase().includes(qLower) ||
                                  name.toLowerCase().includes(qLower) ||
                                  (qPinyin && getPinyinInitials(name).includes(qPinyin))
                                );
                            });
                        }
                        
                        if (found) {
                          viewStock(found.code);
                          setSearchQuery('');
                          setSearchResults([]);
                        } else {
                          console.warn('Stock not found by name search for:', searchQuery);
                        }
                      }
                    }
                  }
                }}
              />
              {searchLoading && (
                <div className="absolute inset-y-0 right-0 pr-3 flex items-center pointer-events-none">
                  <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                </div>
              )}

              {searchQuery.length > 1 && searchResults.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-2 z-30 bg-white border border-slate-200 rounded-xl shadow-lg max-h-60 overflow-y-auto custom-scrollbar">
                {searchResults.map((stock) => (
                  <button
                    key={stock.code}
                    onClick={() => {
                      console.log('Search result item clicked. Code:', stock.code);
                      viewStock(stock.code);
                      setSearchQuery(''); // Clear search query after selection
                      setSearchResults([]); // Clear search results
                    }}
                    className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center"
                  >
                    <span className="font-medium text-slate-900">{stock.name}</span>
                    <span className="text-sm font-mono text-slate-500">{stock.code}</span>
                  </button>
                ))}
                </div>
              )}
            </div>
          </div>

          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-end">
               <label className="text-sm font-bold text-slate-500 uppercase tracking-wider">策略公式</label>
            </div>
            <div className="flex gap-4">
              <input 
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400"
                placeholder="例如：CLOSE > MA(CLOSE, 20)"
                value={formula}
                onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect()}
              />
              <button 
                onClick={handleSelect}
                disabled={loading}
                className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-3 rounded-xl font-bold transition-all shadow-lg shadow-blue-500/20 disabled:opacity-50 flex items-center gap-2 min-w-[160px] justify-center"
              >
                {loading ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                    <span>搜索中...</span>
                  </>
                ) : '运行选股'}
              </button>
            </div>
          </div>
        </section>

        {/* Results Area */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <aside className="lg:col-span-1">
            <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden flex flex-col h-[600px] shadow-sm">
              <div className="p-4 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                <h2 className="font-bold text-slate-700">结果</h2>
                <span className="bg-blue-100 text-blue-700 text-xs px-2 py-0.5 rounded-full font-mono font-bold">{results.length}</span>
              </div>
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.length > 0 ? (
                  <div className="grid grid-cols-1 gap-1">
                    {results.map(code => {
                      const stock = stockList.find(s => s.code === code);
                      const displayName = stock?.name || code;
                      return (
                        <button
                          key={code}
                          onClick={() => {
                            console.log('Strategy result item clicked. Code:', code);
                            viewStock(code);
                          }}
                          className={`w-full text-left px-4 py-3 rounded-lg transition-all text-sm flex justify-between items-center group
                            ${selectedStock?.code === code
                              ? 'bg-blue-50 text-blue-700 border border-blue-100 font-bold'
                              : 'hover:bg-slate-50 text-slate-600 border border-transparent'}`}
                        >
                          <span className="font-medium">{displayName}</span>
                          <span className="text-xs font-mono text-slate-400">{code}</span>
                        </button>
                      );
                    })}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400 text-sm italic p-4 text-center">
                    {loading ? '聚合集群数据中...' : '暂无结果。'}
                  </div>
                )}
              </div>
            </div>
          </aside>

          <section className="lg:col-span-3">
            {/* 将所有内容统一放入 chartWrapperRef 中，原生全屏会自动放大这个 div */}
            <div
              ref={chartWrapperRef}
              className="bg-white rounded-2xl border border-slate-200 overflow-hidden flex flex-col h-[600px] shadow-sm w-full"
            >
              {/* === 统一的顶部控制栏 (Header) === */}
              {selectedStock && (
                <div className="px-4 py-3 border-b border-slate-100 flex justify-between items-center bg-white z-10 shrink-0">
                  {/* 左侧：股票代码、名称、周期标签 */}
                  <div className="flex items-baseline">
                    <span className="text-xl font-bold text-slate-900 tracking-wider">{selectedStock.code}</span>
                    <span className="ml-2 text-base font-medium text-slate-500">{selectedStock.name}</span>
                    <span className="ml-3 text-xs text-blue-600 font-mono bg-blue-50 px-1.5 py-0.5 rounded border border-blue-100">
                      {chartTimeframe === 'D' ? '1-DAY' : chartTimeframe === 'W' ? '1-WEEK' : '1-MONTH'}
                    </span>
                  </div>

                  {/* 右侧：全屏与周期切换按钮 */}
                  <div className="flex items-center bg-slate-50 rounded-lg p-1 border border-slate-200">
                    <button
                      onClick={() => {
                        // 【核心修复】：请求 chartWrapperRef 全屏，而不是整个 document
                        if (!document.fullscreenElement) {
                          chartWrapperRef.current?.requestFullscreen().catch(err => {
                            console.error(`Error attempting to enable fullscreen: ${err.message}`);
                          });
                        } else {
                          document.exitFullscreen();
                        }
                      }}
                      className="px-3 py-1 text-xs font-bold text-slate-600 bg-white shadow-sm border border-slate-200 rounded-md mr-2 hover:bg-slate-100 transition-colors"
                    >
                      {isFullScreen ? '退出全屏' : '全屏'}
                    </button>
                    {TIMEFRAMES.map((tf) => (
                      <button
                        key={tf.value}
                        onClick={() => {
                          setChartTimeframe(tf.value);
                          if (dailyDataCache.length > 0 && selectedStock) {
                            const resampledData = resampleData(dailyDataCache, tf.value);
                            setSelectedStock({ ...selectedStock, data: resampledData });
                          }
                        }}
                        className={`px-3 py-1 text-xs font-bold rounded-md transition-all ${
                          chartTimeframe === tf.value
                            ? 'bg-blue-600 text-white shadow-sm border border-blue-600'
                            : 'text-slate-500 hover:text-slate-700 hover:bg-slate-200/50'
                        }`}
                      >
                        {tf.label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
              
              {/* === 主图表区域 === */}
              <div className="flex-1 w-full h-full relative bg-white p-1">
                {chartLoading && (
                  <div className="absolute inset-0 z-20 bg-white/60 backdrop-blur-sm flex items-center justify-center">
                     <div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                  </div>
                )}
                {selectedStock ? (
                  <KLineChart code={selectedStock.code} data={selectedStock.data} />
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400 bg-slate-50">
                    <p className="text-lg font-medium">选择股票以查看图表</p>
                  </div>
                )}
              </div>
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
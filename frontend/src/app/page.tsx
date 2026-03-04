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
  const numericCode = code.replace(/[^0-9]/g, '');
  if (numericCode.startsWith('6')) return `sh.${numericCode}`;
  else if (numericCode.startsWith('0') || numericCode.startsWith('3')) return `sz.${numericCode}`;
  return code; 
}

export default function Home() {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [timeframe, setTimeframe] = useState('D'); 
  const [chartTimeframe, setChartTimeframe] = useState('D'); 
  const [subChartType, setSubChartType] = useState('MACD'); // 新增：副图切换状态 (MACD / MF)
  
  const [isFullScreen, setIsFullScreen] = useState(false); 
  const chartWrapperRef = useRef<HTMLDivElement>(null); 
  
  useEffect(() => {
    const handler = () => setIsFullScreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', handler);
    return () => document.removeEventListener('fullscreenchange', handler);
  }, []);

  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState<{code: string, name?: string, data: any} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [dailyDataCache, setDailyDataCache] = useState<any[]>([]); 

  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<{code: string; name: string}[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  
  const [stockList, setStockList] = useState<Array<{code: string; name: string}>>([]);
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
    const timer = setInterval(fetchStatus, 5000); 
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const loadStockList = async () => {
      const cached = localStorage.getItem('stockList');
      if (cached) {
        setStockList(JSON.parse(cached));
        return;
      }
      try {
        const res = await fetch('/api/stock-list');
        if (!res.ok) throw new Error('Failed to load stock list');
        const data = await res.json();
        setStockList(data);
        localStorage.setItem('stockList', JSON.stringify(data));
      } catch (err) {}
    };
    loadStockList();
  }, []);

  useEffect(() => {
    if (searchQuery.length < 1 || stockList.length === 0) {
      setSearchResults([]);
      setSearchLoading(false);
      return;
    }
    setSearchLoading(true);
    const handler = setTimeout(() => {
      const qLower = searchQuery.toLowerCase();
      const qPinyin = getPinyinInitials(searchQuery);

      const scoredResults = stockList.map(stock => {
        const { code, name } = stock;
        if (!name || !name.trim() || code.includes('.000')) return { ...stock, score: 0 };

        const nameLower = name.toLowerCase();
        const codeLower = code.toLowerCase();
        const namePinyin = getPinyinInitials(name);
        let score = 0;

        if (codeLower === qLower || nameLower === qLower) score += 1000;
        if (codeLower.startsWith(qLower)) score += 100;
        if (namePinyin.startsWith(qPinyin)) score += 80;
        if (nameLower.startsWith(qLower)) score += 80;
        if (codeLower.includes(qLower)) score += 10;
        if (namePinyin.includes(qPinyin)) score += 5;
        if (nameLower.includes(qLower)) score += 5;

        return { ...stock, score };
      });

      const results = scoredResults.filter(item => item.score > 0).sort((a, b) => b.score - a.score).map(({ code, name }) => ({ code, name })).slice(0, 10);
      setSearchResults(results);
      setSearchLoading(false);
    }, 300); 
    return () => clearTimeout(handler);
  }, [searchQuery, stockList]);

  const handleSelect = async () => {
    setLoading(true); setResults([]); setSelectedStock(null);
    try {
      const res = await fetch('/api/select', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe })
      });
      const json = await res.json();
      if (json.success) setResults(json.data);
      else alert(`Selection failed: ${json.error}`);
    } catch (err) { alert('Gateway connection failed'); }
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

      const dailyData = records.map((record) => {
        let timeValue;
        if (record.date instanceof Date) timeValue = Math.floor(record.date.getTime() / 1000);
        else throw new Error('Invalid date');

        return {
          time: timeValue,
          open: record.open,
          high: record.high,
          low: record.low,
          close: record.close,
          volume: record.volume,
          main_net: record.main_net || 0, // 提取主力资金流入数据
        };
      });

      setDailyDataCache(dailyData);
      const resampledData = resampleData(dailyData, chartTimeframe);
      const stock = stockList.find(s => s.code === code);
      setSelectedStock({ code, name: stock?.name || code, data: resampledData });
    } catch (err: any) { alert(`Failed: ${err.message}`); } 
    finally { setChartLoading(false); }
  }, [chartTimeframe, stockList, resampleData]);

  return (
    <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-7xl mx-auto space-y-6">
        
        <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-3 md:gap-4">
          <div>
            <h1 className="text-2xl md:text-3xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">BlinkQuant</h1>
            <p className="text-slate-500 text-xs md:text-sm mt-1">分布式计算集群</p>
          </div>
          <div className="text-[10px] md:text-xs font-mono text-slate-400 bg-white px-2 py-1 rounded border shadow-sm">
            状态: {clusterStatus?.status || '连接中...'}
          </div>
        </header>

        {/* Search & Formula Inputs */}
        <section className="bg-white p-4 md:p-6 rounded-2xl border border-slate-200 shadow-sm">
          <div className="flex flex-col gap-3 md:gap-4 mb-4 md:mb-6">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">搜索股票</label>
            <div className="relative z-20">
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 md:px-4 md:py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full text-sm md:text-base"
                placeholder="例如：000952, 平安, PA"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && searchQuery.trim() !== '') {
                    if (searchResults.length > 0) {
                      viewStock(searchResults[0].code);
                      setSearchQuery(''); setSearchResults([]);
                    } else {
                      const isNumeric = /^[0-9]+$/.test(searchQuery.trim());
                      if (isNumeric) { viewStock(formatStockCode(searchQuery)); setSearchQuery(''); } 
                      else {
                        const qL = searchQuery.toLowerCase();
                        let found = stockList.find(s => s.code.toLowerCase().startsWith(qL) || s.name.toLowerCase().startsWith(qL));
                        if(found) { viewStock(found.code); setSearchQuery(''); }
                      }
                    }
                  }
                }}
              />
              {searchLoading && <div className="absolute inset-y-0 right-0 pr-3 flex items-center"><div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div></div>}
              {searchQuery.length > 1 && searchResults.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-2 z-30 bg-white border rounded-xl shadow-lg max-h-60 overflow-y-auto custom-scrollbar">
                {searchResults.map((stock) => (
                  <button key={stock.code} onClick={() => { viewStock(stock.code); setSearchQuery(''); setSearchResults([]); }} className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center">
                    <span className="font-medium text-slate-900">{stock.name}</span>
                    <span className="text-sm font-mono text-slate-500">{stock.code}</span>
                  </button>
                ))}
                </div>
              )}
            </div>
          </div>

          <div className="flex flex-col gap-3">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">策略公式</label>
            <div className="flex flex-col sm:flex-row gap-3">
              <input
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
                placeholder="例如：CLOSE > MA(CLOSE, 20)"
                value={formula} onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect()}
              />
              <button onClick={handleSelect} disabled={loading} className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 rounded-xl font-bold flex items-center justify-center gap-2 min-w-[160px]">
                {loading ? <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div> : '运行选股'}
              </button>
            </div>
          </div>
        </section>

        {/* Results Area */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 md:gap-6">
          <aside className="lg:col-span-1 order-1 lg:order-1">
            <div className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm">
              <div className="p-4 border-b flex justify-between items-center bg-slate-50/50">
                <h2 className="font-bold text-slate-700">结果</h2>
                <span className="bg-blue-100 text-blue-700 text-xs px-2 py-0.5 rounded-full font-mono">{results.length}</span>
              </div>
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.map(code => {
                  const name = stockList.find(s => s.code === code)?.name || code;
                  return (
                    <button key={code} onClick={() => viewStock(code)} className={`w-full text-left px-4 py-3 rounded-lg flex justify-between group ${selectedStock?.code === code ? 'bg-blue-50 text-blue-700 font-bold border border-blue-100' : 'hover:bg-slate-50 text-slate-600'}`}>
                      <span className="truncate">{name}</span>
                      <span className="text-xs font-mono text-slate-400 ml-2">{code}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          </aside>

          <section className="lg:col-span-3 order-2 lg:order-2">
            <div ref={chartWrapperRef} className="bg-white rounded-2xl border flex flex-col h-[600px] shadow-sm w-full">
              {selectedStock && (
                <div className="px-4 py-3 border-b flex flex-wrap justify-between items-center gap-2 bg-white z-10 shrink-0">
                  <div className="flex items-baseline">
                    <span className="text-xl font-bold">{selectedStock.code}</span>
                    <span className="ml-2 text-base font-medium text-slate-500">{selectedStock.name}</span>
                  </div>

                  <div className="flex items-center gap-2">
                    {/* 新增：副图切换 Tabs */}
                    <div className="flex items-center bg-slate-50 rounded-lg p-1 border border-slate-200">
                      <button onClick={() => setSubChartType('MACD')} className={`px-3 py-1 text-xs font-bold rounded-md ${subChartType === 'MACD' ? 'bg-indigo-600 text-white shadow' : 'text-slate-500 hover:text-slate-700'}`}>MACD</button>
                      <button onClick={() => setSubChartType('MF')} className={`px-3 py-1 text-xs font-bold rounded-md ${subChartType === 'MF' ? 'bg-indigo-600 text-white shadow' : 'text-slate-500 hover:text-slate-700'}`}>资金流</button>
                    </div>

                    <div className="flex items-center bg-slate-50 rounded-lg p-1 border border-slate-200">
                      <button 
                        onClick={() => { 
                          // 【修复】：判断当前是否已经是全屏状态，执行不同的 API
                          if (!document.fullscreenElement) {
                            chartWrapperRef.current?.requestFullscreen().catch(()=>{}); 
                          } else {
                            document.exitFullscreen().catch(()=>{});
                          }
                        }} 
                        className="px-3 py-1 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-md mr-2 hover:bg-slate-100 transition-colors"
                      >
                        {isFullScreen ? '退出全屏' : '全屏'}
                      </button>
                      {TIMEFRAMES.map((tf) => (
                        <button key={tf.value} onClick={() => {
                            setChartTimeframe(tf.value);
                            if (dailyDataCache.length > 0) setSelectedStock({ ...selectedStock, data: resampleData(dailyDataCache, tf.value) });
                          }} 
                          className={`px-3 py-1 text-xs font-bold rounded-md ${chartTimeframe === tf.value ? 'bg-blue-600 text-white shadow' : 'text-slate-500 hover:bg-slate-200/50'}`}
                        >
                          {tf.label}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              )}
              
              <div className="flex-1 w-full h-full relative p-1">
                {chartLoading && <div className="absolute inset-0 z-20 bg-white/60 backdrop-blur-sm flex items-center justify-center"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div></div>}
                {selectedStock ? (
                  <KLineChart code={selectedStock.code} data={selectedStock.data} subChartType={subChartType} />
                ) : (
                  <div className="h-full flex items-center justify-center text-slate-400 bg-slate-50">选择股票查看图表</div>
                )}
              </div>
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}

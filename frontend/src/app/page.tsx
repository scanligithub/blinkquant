'use client'; 
import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';

const KLineChart = dynamic(() => import('../components/KLineChart'), { 
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-gray-900 rounded-xl animate-pulse">Loading Chart Engine...</div>
});

const TIMEFRAMES = [
  { label: 'Daily', value: 'D' },
  { label: 'Weekly', value: 'W' },
  { label: 'Monthly', value: 'M' },
];

export default function Home() {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [timeframe, setTimeframe] = useState('D');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState<{code: string, data: any} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  
  // --- 新增：监控状态 ---
  const [clusterStatus, setClusterStatus] = useState<any>(null);
  const [showMonitor, setShowMonitor] = useState(false);

  // 加载集群状态
  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/status');
      const json = await res.json();
      setClusterStatus(json);
    } catch (e) { console.error("Monitor failed", e); }
  };

  useEffect(() => {
    fetchStatus();
    // 每 10 秒自动刷新一次状态
    const timer = setInterval(fetchStatus, 10000);
    return () => clearInterval(timer);
  }, []);

  // --- 逻辑执行：选股 ---
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
      if (json.success) {
        setResults(json.data);
      } else {
        alert(`Selection failed: ${json.error || 'Unknown error'}`);
      }
    } catch (err) {
      alert('Gateway connection failed');
    }
    setLoading(false);
  };

  // --- 逻辑执行：加载 K 线 ---
  const viewStock = async (code: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/kline?code=${code}&timeframe=${timeframe}`);
      if (!res.ok) throw new Error('Fetch failed');
      const json = await res.json();
      if (json.data) {
        setSelectedStock({ code, data: json.data });
      } else {
        alert('Stock data empty');
      }
    } catch (err) {
      console.error(err);
      alert('Failed to load kline data');
    }
    setChartLoading(false);
  };

  return (
    <main className="min-h-screen bg-gray-950 text-slate-200 p-4 md:p-8 font-sans">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Header & Monitor */}
        <header className="flex flex-col gap-4 border-b border-slate-800 pb-6">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <h1 className="text-3xl font-black bg-gradient-to-r from-blue-400 to-indigo-500 bg-clip-text text-transparent">
                BlinkQuant
              </h1>
              <p className="text-slate-500 text-sm mt-1">Distributed Computing Cluster | Gateway Aggregation</p>
            </div>
            
            <button 
              onClick={() => setShowMonitor(!showMonitor)}
              className="px-4 py-2 bg-slate-900 border border-slate-800 rounded-lg text-xs flex items-center gap-2 hover:bg-slate-800 transition-colors"
            >
               <span className={`w-2 h-2 rounded-full ${clusterStatus?.cluster_health?.startsWith('3') ? 'bg-green-500' : 'bg-yellow-500 animate-pulse'}`}></span>
               Cluster Status: {clusterStatus?.cluster_health || 'Checking...'}
               <span className="text-slate-500 ml-1">▼</span>
            </button>
          </div>

          {/* 详细监控面板 (可折叠) */}
          {showMonitor && clusterStatus && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 bg-slate-900/50 p-4 rounded-xl border border-slate-800/50 animate-in fade-in slide-in-from-top-2">
              {clusterStatus.nodes.map((node: any, idx: number) => (
                <div key={idx} className={`p-3 rounded-lg border ${node.online ? 'border-slate-700 bg-slate-800' : 'border-red-900 bg-red-900/20'}`}>
                  <div className="flex justify-between items-center mb-2">
                    <span className="font-bold text-sm text-slate-300">Node {node.node || idx}</span>
                    <span className={`text-[10px] uppercase px-1.5 py-0.5 rounded ${node.status === 'healthy' ? 'bg-green-900 text-green-300' : 'bg-yellow-900 text-yellow-300'}`}>
                      {node.status || 'OFFLINE'}
                    </span>
                  </div>
                  {node.online ? (
                    <div className="space-y-1">
                      <div className="flex justify-between text-xs">
                        <span className="text-slate-500">Mem (App/Sys):</span>
                        <span className="font-mono text-blue-300">{node.process_memory_gb}G / {node.system_memory_free_gb}G free</span>
                      </div>
                      <div className="flex justify-between text-xs">
                        <span className="text-slate-500">Disk Free:</span>
                        <span className="font-mono text-indigo-300">{node.disk_free_gb} GB</span>
                      </div>
                      <div className="flex justify-between text-xs">
                        <span className="text-slate-500">Data Rows:</span>
                        <span className="font-mono text-slate-400">{node.rows_daily?.toLocaleString()}</span>
                      </div>
                    </div>
                  ) : (
                    <div className="text-xs text-red-400">Connection Timeout</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </header>

        {/* 控制面板 */}
        <section className="bg-slate-900 p-6 rounded-2xl border border-slate-800 shadow-2xl">
          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-end">
               <label className="text-sm font-bold text-slate-400 uppercase tracking-wider">Strategy Formula</label>
               <div className="flex bg-slate-950 rounded-lg p-1 border border-slate-800">
                 {TIMEFRAMES.map((tf) => (
                   <button
                     key={tf.value}
                     onClick={() => setTimeframe(tf.value)}
                     className={`px-4 py-1.5 text-xs font-bold rounded-md transition-all ${
                       timeframe === tf.value 
                         ? 'bg-blue-600 text-white shadow-lg' 
                         : 'text-slate-500 hover:text-slate-300'
                     }`}
                   >
                     {tf.label}
                   </button>
                 ))}
               </div>
            </div>
            <div className="flex gap-4">
              <input 
                className="flex-1 bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 font-mono text-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                placeholder="e.g. CLOSE > MA(CLOSE, 20)"
                value={formula}
                onChange={(e) => setFormula(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSelect()}
              />
              <button 
                onClick={handleSelect}
                disabled={loading}
                className="bg-blue-600 hover:bg-blue-500 text-white px-8 py-3 rounded-xl font-bold transition-all shadow-lg shadow-blue-900/20 disabled:opacity-50 flex items-center gap-2 min-w-[160px] justify-center"
              >
                {loading ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                    <span>Processing...</span>
                  </>
                ) : 'Run Selection'}
              </button>
            </div>
          </div>
        </section>

        {/* 主交互区 */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <aside className="lg:col-span-1 flex flex-col gap-4">
            <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden flex flex-col h-[600px]">
              <div className="p-4 border-b border-slate-800 flex justify-between items-center bg-slate-900/50">
                <h2 className="font-bold text-slate-200">Results</h2>
                <span className="bg-blue-500/10 text-blue-400 text-xs px-2 py-0.5 rounded-full font-mono">{results.length}</span>
              </div>
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.length > 0 ? (
                  <div className="grid grid-cols-1 gap-1">
                    {results.map(code => (
                      <button 
                        key={code} 
                        onClick={() => viewStock(code)}
                        className={`w-full text-left px-4 py-3 rounded-lg transition-all text-sm font-mono flex justify-between items-center group
                          ${selectedStock?.code === code ? 'bg-blue-600 text-white shadow-lg' : 'hover:bg-slate-800 text-slate-400'}`}
                      >
                        {code}
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-600 text-sm italic p-4 text-center">
                    {loading ? 'Gateway aggregating...' : 'No results.'}
                  </div>
                )}
              </div>
            </div>
          </aside>

          <section className="lg:col-span-3 space-y-6">
            <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden min-h-[520px] shadow-2xl relative flex flex-col">
              {selectedStock && (
                 <div className="absolute top-4 left-4 z-10 bg-slate-950/80 backdrop-blur px-4 py-2 rounded-lg border border-slate-800">
                    <span className="text-xl font-bold text-white tracking-wider">{selectedStock.code}</span>
                    <span className="ml-3 text-xs text-blue-400 font-mono border border-blue-900 bg-blue-900/20 px-1.5 py-0.5 rounded">
                      {timeframe === 'D' ? '1-DAY' : timeframe === 'W' ? '1-WEEK' : '1-MONTH'}
                    </span>
                 </div>
              )}
              {chartLoading && (
                <div className="absolute inset-0 z-20 bg-slate-950/60 backdrop-blur-sm flex items-center justify-center">
                   <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin"></div>
                </div>
              )}
              <div className="flex-1 w-full h-full p-1">
                {selectedStock ? (
                  <KLineChart code={selectedStock.code} data={selectedStock.data} />
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-700 bg-[radial-gradient(#1e293b_1px,transparent_1px)] [background-size:20px_20px]">
                    <p className="text-lg font-medium">Select a stock to visualize</p>
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

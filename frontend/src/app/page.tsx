'use client';
import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';

// 动态导入 KLineChart 组件，防止 SSR 阶段找不到 window 对象
const KLineChart = dynamic(() => import('../components/KLineChart'), { 
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-gray-900 rounded-xl">Loading Chart Engine...</div>
});

export default function Home() {
  // --- 状态管理 ---
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 250)');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<any>(null);
  const [selectedStock, setSelectedStock] = useState<{code: string, data: any[]} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);

  // --- 逻辑执行：选股 ---
  const handleSelect = async () => {
    setLoading(true);
    setResults([]);
    setSelectedStock(null);
    try {
      const res = await fetch('/api/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe: 'D' })
      });
      const data = await res.json();
      if (data.success) {
        setResults(data.data || []);
        setStatus(data.meta);
      } else {
        alert(`Error: ${data.error || 'Selection failed'}`);
      }
    } catch (err) {
      console.error(err);
      alert('Network error or timeout');
    }
    setLoading(false);
  };

  // --- 逻辑执行：加载个股 K 线 ---
  const loadKline = async (code: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/kline?code=${code}&timeframe=D`);
      const json = await res.json();
      if (json.data) {
        setSelectedStock({ code, data: json.data });
      } else {
        alert('Stock data not found');
      }
    } catch (err) {
      alert('Failed to load kline data');
    }
    setChartLoading(false);
  };

  return (
    <main className="min-h-screen bg-gray-950 text-slate-200 p-4 md:p-8 font-sans">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Header */}
        <header className="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-800 pb-6 gap-4">
          <div>
            <h1 className="text-3xl font-black bg-gradient-to-r from-blue-400 to-indigo-500 bg-clip-text text-transparent">
              BlinkQuant
            </h1>
            <p className="text-slate-500 text-sm mt-1">Distributed Computing Cluster | 3 Nodes Active</p>
          </div>
          <div className="flex items-center gap-3">
             <div className="px-3 py-1 bg-slate-900 border border-slate-800 rounded-full text-xs flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${status?.nodes_responding === 3 ? 'bg-green-500' : 'bg-yellow-500'}`}></span>
                Node Health: {status?.nodes_responding || 0}/3
             </div>
          </div>
        </header>

        {/* 控制面板：选股公式输入 */}
        <section className="bg-slate-900 p-6 rounded-2xl border border-slate-800 shadow-2xl">
          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-end">
               <label className="text-sm font-bold text-slate-400 uppercase tracking-wider">Strategy Formula (Blink-AST)</label>
               <span className="text-xs text-slate-600">Tip: use codes like sh.000001</span>
            </div>
            <div className="flex gap-4">
              <input 
                className="flex-1 bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 font-mono text-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                placeholder="e.g. CLOSE > MA(CLOSE, 250)"
                value={formula}
                onChange={(e) => setFormula(e.target.value)}
              />
              <button 
                onClick={handleSelect}
                disabled={loading}
                className="bg-blue-600 hover:bg-blue-500 text-white px-8 py-3 rounded-xl font-bold transition-all shadow-lg shadow-blue-900/20 disabled:opacity-50 flex items-center gap-2"
              >
                {loading ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                    Scanning...
                  </>
                ) : 'Run Selection'}
              </button>
            </div>
          </div>
        </section>

        {/* 主交互区 */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          
          {/* 左侧：选股结果列表 */}
          <aside className="lg:col-span-1 flex flex-col gap-4">
            <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden flex flex-col h-[600px]">
              <div className="p-4 border-b border-slate-800 flex justify-between items-center bg-slate-900/50">
                <h2 className="font-bold">Hits</h2>
                <span className="bg-blue-500/10 text-blue-400 text-xs px-2 py-0.5 rounded-full font-mono">{results.length}</span>
              </div>
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.length > 0 ? (
                  <div className="grid grid-cols-1 gap-1">
                    {results.map(code => (
                      <button 
                        key={code} 
                        onClick={() => loadKline(code)}
                        className={`w-full text-left px-4 py-3 rounded-lg transition-all text-sm font-mono flex justify-between items-center group
                          ${selectedStock?.code === code ? 'bg-blue-600 text-white shadow-lg' : 'hover:bg-slate-800 text-slate-400'}`}
                      >
                        {code}
                        <span className={`text-[10px] opacity-0 group-hover:opacity-100 transition-opacity ${selectedStock?.code === code ? 'text-white/50' : 'text-slate-600'}`}>VIEW →</span>
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-600 text-sm italic p-4 text-center">
                    {loading ? 'Crunching 16M rows...' : 'No results. Adjust your formula and run selection.'}
                  </div>
                )}
              </div>
            </div>
          </aside>

          {/* 右侧：图表与详情 */}
          <section className="lg:col-span-3 space-y-6">
            <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden min-h-[480px] shadow-2xl relative">
              {chartLoading && (
                <div className="absolute inset-0 z-20 bg-slate-950/50 backdrop-blur-sm flex items-center justify-center">
                   <div className="flex flex-col items-center gap-3">
                      <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin"></div>
                      <span className="text-blue-400 text-sm font-bold tracking-widest animate-pulse">STREAMING DATA</span>
                   </div>
                </div>
              )}
              
              {selectedStock ? (
                <div className="p-1">
                  <KLineChart code={selectedStock.code} data={selectedStock.data} />
                </div>
              ) : (
                <div className="h-[480px] flex flex-col items-center justify-center text-slate-700 bg-[radial-gradient(#1e293b_1px,transparent_1px)] [background-size:20px_20px]">
                  <div className="w-16 h-16 border-2 border-slate-800 rounded-2xl flex items-center justify-center mb-4">
                    <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" /></svg>
                  </div>
                  <p className="text-lg font-medium">Select a stock from the list to visualize</p>
                  <p className="text-xs mt-2 text-slate-800">20-year history will be loaded instantly</p>
                </div>
              )}
            </div>

            {/* 系统遥测 (Telemetry) */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800/50 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Response Time</span>
                <span className="text-xl font-mono text-emerald-400">~850ms</span>
              </div>
              <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800/50 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Data Points</span>
                <span className="text-xl font-mono text-blue-400">16,432,015</span>
              </div>
              <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800/50 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Engine Mode</span>
                <span className="text-xl font-mono text-indigo-400">Polars Lazy</span>
              </div>
            </div>
          </section>

        </div>
      </div>
    </main>
  );
}

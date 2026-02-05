'use client';
import { useState } from 'react';
import dynamic from 'next/dynamic';

// 动态导入 KLineChart 组件
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
  // --- 状态管理 ---
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [timeframe, setTimeframe] = useState('D'); // 新增：周期状态
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
      // 聚合 3 个节点的结果
      // 注意：这里我们通过 Vercel 的 rewrite 规则分发，或者直接请求后端
      // 为了演示聚合逻辑，这里模拟并发请求所有节点 (实际生产通常由 /api/select 聚合接口处理)
      // 但根据之前的 routes.py，我们先请求 /api/nodeX/api/v1/select
      
      const nodePromises = [0, 1, 2].map(id => 
        fetch(`/api/node${id}/api/v1/select`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ formula, timeframe }) // 关键：传入 timeframe
        }).then(r => r.json())
      );

      const allData = await Promise.all(nodePromises);
      
      // 合并去重
      const combined = allData.flatMap(d => d.results || []);
      const uniqueResults = Array.from(new Set(combined)).sort();
      
      setResults(uniqueResults);
      setStatus({ 
        nodes_responding: allData.filter(d => !d.error).length,
        total_hits: uniqueResults.length 
      });

    } catch (err) {
      console.error(err);
      alert('Network error: Ensure nodes are running.');
    }
    setLoading(false);
  };

  // --- 逻辑执行：加载个股 K 线 ---
  const viewStock = async (code: string) => {
    setChartLoading(true);
    try {
      // 关键：传入 code 和 timeframe
      const res = await fetch(`/api/kline?code=${code}&timeframe=${timeframe}`);
      if (!res.ok) throw new Error('Fetch failed');
      
      const json = await res.json();
      if (json.data) {
        setSelectedStock({ code, data: json.data });
      } else {
        alert('Stock data not found');
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
        
        {/* Header */}
        <header className="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-800 pb-6 gap-4">
          <div>
            <h1 className="text-3xl font-black bg-gradient-to-r from-blue-400 to-indigo-500 bg-clip-text text-transparent">
              BlinkQuant
            </h1>
            <p className="text-slate-500 text-sm mt-1">Distributed Computing Cluster | Self-Evolving Engine</p>
          </div>
          <div className="flex items-center gap-3">
             <div className="px-3 py-1 bg-slate-900 border border-slate-800 rounded-full text-xs flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${status?.nodes_responding === 3 ? 'bg-green-500' : 'bg-green-500/50'}`}></span>
                Cluster: {status?.nodes_responding || 3}/3 Nodes
             </div>
          </div>
        </header>

        {/* 控制面板：选股公式输入 */}
        <section className="bg-slate-900 p-6 rounded-2xl border border-slate-800 shadow-2xl">
          <div className="flex flex-col gap-4">
            
            {/* 上方：标签与周期选择 */}
            <div className="flex justify-between items-end">
               <label className="text-sm font-bold text-slate-400 uppercase tracking-wider">Strategy Formula (Blink-AST)</label>
               
               {/* 周期切换器 */}
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

            {/* 下方：输入框与按钮 */}
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
                    <span>Scanning...</span>
                  </>
                ) : 'Run Selection'}
              </button>
            </div>
            <div className="text-xs text-slate-600 flex justify-between">
              <span>Current Timeframe: <span className="text-blue-400 font-mono">{timeframe}</span></span>
              <span>Tip: Indicators will be auto-cached for this timeframe.</span>
            </div>
          </div>
        </section>

        {/* 主交互区 */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          
          {/* 左侧：选股结果列表 */}
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
                        <span className={`text-[10px] opacity-0 group-hover:opacity-100 transition-opacity ${selectedStock?.code === code ? 'text-white/50' : 'text-slate-600'}`}>VIEW →</span>
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-600 text-sm italic p-4 text-center">
                    {loading ? 'Crunching data...' : 'No results found.'}
                  </div>
                )}
              </div>
            </div>
          </aside>

          {/* 右侧：图表与详情 */}
          <section className="lg:col-span-3 space-y-6">
            <div className="bg-slate-900 rounded-2xl border border-slate-800 overflow-hidden min-h-[520px] shadow-2xl relative flex flex-col">
              
              {/* Chart Header */}
              {selectedStock && (
                 <div className="absolute top-4 left-4 z-10 bg-slate-950/80 backdrop-blur px-4 py-2 rounded-lg border border-slate-800">
                    <span className="text-xl font-bold text-white tracking-wider">{selectedStock.code}</span>
                    <span className="ml-3 text-xs text-blue-400 font-mono border border-blue-900 bg-blue-900/20 px-1.5 py-0.5 rounded">
                      {timeframe === 'D' ? '1-DAY' : timeframe === 'W' ? '1-WEEK' : '1-MONTH'}
                    </span>
                 </div>
              )}

              {/* Chart Loading State */}
              {chartLoading && (
                <div className="absolute inset-0 z-20 bg-slate-950/60 backdrop-blur-sm flex items-center justify-center">
                   <div className="flex flex-col items-center gap-3">
                      <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin"></div>
                   </div>
                </div>
              )}
              
              {/* Chart Component */}
              <div className="flex-1 w-full h-full p-1">
                {selectedStock ? (
                  <KLineChart code={selectedStock.code} data={selectedStock.data} />
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-700 bg-[radial-gradient(#1e293b_1px,transparent_1px)] [background-size:20px_20px]">
                    <div className="w-16 h-16 border-2 border-slate-800 rounded-2xl flex items-center justify-center mb-4">
                      <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" /></svg>
                    </div>
                    <p className="text-lg font-medium">Select a stock to visualize</p>
                  </div>
                )}
              </div>
            </div>

            {/* 遥测面板 (Telemetry) */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 opacity-80">
              <div className="bg-slate-900 p-4 rounded-xl border border-slate-800 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Engine Mode</span>
                <span className="text-lg font-mono text-emerald-400">Polars Lazy + Hot-JIT</span>
              </div>
              <div className="bg-slate-900 p-4 rounded-xl border border-slate-800 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Data Scope</span>
                <span className="text-lg font-mono text-blue-400">Day / Week / Month</span>
              </div>
              <div className="bg-slate-900 p-4 rounded-xl border border-slate-800 flex flex-col">
                <span className="text-[10px] text-slate-500 font-bold uppercase">Evolution</span>
                <span className="text-lg font-mono text-indigo-400">Active (Postgres)</span>
              </div>
            </div>
          </section>

        </div>
      </div>
    </main>
  );
}

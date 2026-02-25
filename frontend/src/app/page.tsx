'use client'; 
import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';

const KLineChart = dynamic(() => import('../components/KLineChart'), { 
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">Loading Chart Engine...          </div>


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
    const [selectedStock, setSelectedStock] = useState<{code: string, name: string, data: any} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);

  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<{code: string; name: string}[]>([]);
  const [searchLoading, setSearchLoading] = useState(false); // New state for search loading
  
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

  useEffect(() => {
    if (searchQuery.length > 1) { // Only search if query is at least 2 characters
      setSearchLoading(true);
      const handler = setTimeout(async () => {
        try {
          const res = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}`);
          if (!res.ok) throw new Error('Search failed');
          const json = await res.json();
          setSearchResults(json);
        } catch (err) {
          console.error('Failed to search stocks:', err);
          setSearchResults([]);
        } finally {
          setSearchLoading(false);
        }
      }, 500); // Debounce for 500ms
      return () => clearTimeout(handler);
    } else {
      setSearchResults([]);
      setSearchLoading(false);
    }
  }, [searchQuery]);

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

  const viewStock = async (code: string) => {
    setChartLoading(true);
    try {
      const res = await fetch(`/api/kline?code=${code}&timeframe=${timeframe}`);
            if (!res.ok) {
        const errorJson = await res.json();
        throw new Error(errorJson.error || errorJson.detail || 'Fetch failed');
      }
      const json = await res.json();
            if (json.data) setSelectedStock({ code, name: json.name, data: json.data });
      else alert('Stock data empty');
    } catch (err: any) { alert(`Failed to load kline: ${err.message}`); }
    setChartLoading(false);
  };

  return (
    <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Header Title */}
        <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <h1 className="text-3xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
              BlinkQuant
            </h1>
            <p className="text-slate-500 text-sm mt-1">Distributed Computing Cluster</p>
          </div>
          
          <div className="text-xs font-mono text-slate-400 bg-white px-3 py-1 rounded border border-slate-200 shadow-sm">
            Status: {clusterStatus?.cluster_health || 'Connecting...'}
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
            <label className="text-sm font-bold text-slate-500 uppercase tracking-wider">Search Stock</label>
            <div className="relative">
              <input 
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400 w-full"
                placeholder="e.g. 000952, Ping An, PA"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && searchResults.length > 0) {
                    viewStock(searchResults[0].code);
                    setSearchQuery(''); // Clear search query after selection
                    setSearchResults([]); // Clear search results
                  }
                }}
              />
              {searchLoading && (
                <div className="absolute inset-y-0 right-0 pr-3 flex items-center pointer-events-none">
                  <div className="w-4 h-4 border-2 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                </div>
              )}
            </div>

            {searchQuery.length > 1 && searchResults.length > 0 && (
              <div className="absolute z-10 w-full bg-white border border-slate-200 rounded-xl shadow-lg mt-1 max-h-60 overflow-y-auto custom-scrollbar">
                {searchResults.map((stock) => (
                  <button
                    key={stock.code}
                    onClick={() => {
                      viewStock(stock.code);
                      setSearchQuery(''); // Clear search query after selection
                      setSearchResults([]); // Clear search results
                    }}
                    className="w-full text-left px-4 py-2 hover:bg-slate-50 flex justify-between items-center"
                  >
                    <span className="font-mono text-slate-700">{stock.code}</span>
                    <span className="text-sm text-slate-500">{stock.name}</span>
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="flex flex-col gap-4">
            <div className="flex justify-between items-end">
               <label className="text-sm font-bold text-slate-500 uppercase tracking-wider">Strategy Formula</label>
               <div className="flex bg-slate-100 rounded-lg p-1 border border-slate-200">
                 {TIMEFRAMES.map((tf) => (
                   <button
                     key={tf.value}
                     onClick={() => setTimeframe(tf.value)}
                     className={`px-4 py-1.5 text-xs font-bold rounded-md transition-all ${
                       timeframe === tf.value 
                         ? 'bg-white text-blue-600 shadow-sm border border-slate-200' 
                         : 'text-slate-500 hover:text-slate-700'
                     }`}
                   >
                     {tf.label}
                   </button>
                 ))}
               </div>
            </div>
            <div className="flex gap-4">
              <input 
                className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3 font-mono text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-slate-400"
                placeholder="e.g. CLOSE > MA(CLOSE, 20)"
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
                    <span>Searching...</span>
                  </>
                ) : 'Run Selection'}
              </button>
            </div>
          </div>
        </section>

        {/* Results Area */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <aside className="lg:col-span-1">
            <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden flex flex-col h-[600px] shadow-sm">
              <div className="p-4 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                <h2 className="font-bold text-slate-700">Results</h2>
                <span className="bg-blue-100 text-blue-700 text-xs px-2 py-0.5 rounded-full font-mono font-bold">{results.length}</span>
              </div>
              <div className="flex-1 overflow-y-auto p-2 custom-scrollbar">
                {results.length > 0 ? (
                  <div className="grid grid-cols-1 gap-1">
                    {results.map(code => (
                      <button 
                        key={code} 
                        onClick={() => viewStock(code)}
                        className={`w-full text-left px-4 py-3 rounded-lg transition-all text-sm font-mono flex justify-between items-center group
                          ${selectedStock?.code === code 
                            ? 'bg-blue-50 text-blue-700 border border-blue-100 font-bold' 
                            : 'hover:bg-slate-50 text-slate-600 border border-transparent'}`}
                      >
                        {code}
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400 text-sm italic p-4 text-center">
                    {loading ? 'Aggregating cluster data...' : 'No results found.'}
                  </div>
                )}
              </div>
            </div>
          </aside>

          <section className="lg:col-span-3">
            <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden min-h-[520px] shadow-sm relative flex flex-col">
              {selectedStock && (
                 <div className="absolute top-4 left-4 z-10 bg-white/90 backdrop-blur px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
                    <span className="text-xl font-bold text-slate-900 tracking-wider">{selectedStock.code}</span>
                    <span className="ml-2 text-lg text-slate-500">{selectedStock.name}</span>
                    <span className="ml-3 text-xs text-blue-600 font-mono bg-blue-50 px-1.5 py-0.5 rounded border border-blue-100">
                      {timeframe === 'D' ? '1-DAY' : timeframe === 'W' ? '1-WEEK' : '1-MONTH'}
                    </span>
                 </div>
              )}
              {chartLoading && (
                <div className="absolute inset-0 z-20 bg-white/60 backdrop-blur-sm flex items-center justify-center">
                   <div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin"></div>
                </div>
              )}
              <div className="flex-1 w-full h-full p-1">
                {selectedStock ? (
                  <KLineChart code={selectedStock.code} data={selectedStock.data} />
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400 bg-slate-50">
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

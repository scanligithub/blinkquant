'use client'; 
import { useState, useEffect, useCallback } from 'react';
import dynamic from 'next/dynamic';

const KLineChart = dynamic(() => import('../components/KLineChart'), { 
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">Loading Chart Engine...          </div>
});

import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

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
  const [timeframe, setTimeframe] = useState('D');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
    const [selectedStock, setSelectedStock] = useState<{code: string, name?: string, data: any} | null>(null);
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

  const viewStock = useCallback(async (code: string) => {
    setChartLoading(true);
    try {
      console.log('Entering viewStock for code:', code);
      const res = await fetch(`/api/kline?code=${code}&timeframe=${timeframe}`);
      
      console.log('Fetch response status:', res.status);
      console.log('Fetch response ok:', res.ok);
      console.log('Fetch response Content-Type:', res.headers.get('Content-Type'));

      if (!res.ok) {
        let errorMessage = 'Fetch failed';
        const contentType = res.headers.get('Content-Type');
        if (contentType && contentType.includes('application/json')) {
          try {
            const errorJson = await res.json();
            errorMessage = errorJson.error || errorJson.detail || errorMessage;
            console.error('Error from server (JSON):', errorJson);
          } catch (jsonError) {
            errorMessage = `Fetch failed: ${res.status} ${res.statusText}: Failed to parse JSON error`;
            console.error('Failed to parse JSON error:', jsonError);
          }
        } else {
          const errorText = await res.text();
          errorMessage = `Fetch failed: ${res.status} ${res.statusText}: ${errorText || errorMessage}`;
          console.error('Error from server (text):', errorText);
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

      const formattedData = records.map(record => {
        console.log('Raw date from Parquet:', record.date, typeof record.date); // Debug log
        const dateInMillis = record.date * 24 * 60 * 60 * 1000; // Convert days to milliseconds
        const dateObject = new Date(dateInMillis);
        const formattedDateString = dateObject.toISOString().slice(0, 10); // Format to YYYY-MM-DD

        return {
          time: formattedDateString,
          open: record.open,
          high: record.high,
          low: record.low,
          close: record.close,
          volume: record.volume,
        };
      });

      let stockName = code;
      const foundInSearchResults = searchResults.find(s => s.code === code);
      if (foundInSearchResults) {
        stockName = foundInSearchResults.name;
      } else if (selectedStock && selectedStock.code === code && selectedStock.name) {
        stockName = selectedStock.name;
      }
      
      setSelectedStock({ code, name: stockName, data: formattedData });
    } catch (err: any) {
      console.error('Failed to load kline:', err);
      console.error('Full error object:', err);
      alert(`Failed to load kline: ${err.message || err}`);
    } finally {
      setChartLoading(false);
    }
  }, [timeframe, searchResults, selectedStock]);

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
                        // It's likely a name, perform an immediate search to get the code
                        console.log('Performing immediate name search for:', searchQuery);
                        setSearchLoading(true);
                        try {
                          const res = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}`);
                          if (!res.ok) throw new Error('Immediate search failed');
                          const json = await res.json();
                          if (json.length > 0) {
                            console.log('Immediate name search found:', json[0].code);
                            viewStock(json[0].code);
                            setSearchQuery('');
                            setSearchResults([]);
                          } else {
                            console.warn('Stock not found by name search for:', searchQuery);
                            // alert('Stock not found by name search.'); // Temporarily disabled
                          }
                        } catch (err) {
                          console.error('Failed to search stocks by name:', err);
                          // alert(`Failed to search stocks by name: ${ (err as Error).message || err}`); // Temporarily disabled
                        } finally {
                          setSearchLoading(false);
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
            </div>

            {searchQuery.length > 1 && searchResults.length > 0 && (
              <div className="absolute z-10 w-full bg-white border border-slate-200 rounded-xl shadow-lg mt-1 max-h-60 overflow-y-auto custom-scrollbar">
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
                         ? 'bg-white text-blue-600 shadow-sm border border-blue-100' 
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
                        onClick={() => {
                          console.log('Strategy result item clicked. Code:', code);
                          viewStock(code);
                        }}
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
                 <div className="absolute top-4 left-4 z-10 bg-white/90 backdrop-blur px-4 py-2 rounded-lg border border-slate-200 shadow-sm flex items-baseline">
                    <span className="text-xl font-bold text-slate-900 tracking-wider">{selectedStock.code}</span>
                    <span className="ml-2 text-base font-medium text-slate-500">{selectedStock.name}</span>
                    
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
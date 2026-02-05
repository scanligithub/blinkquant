'use client';
import { useState } from 'react';
import dynamic from 'next/dynamic';

const KLineChart = dynamic(() => import('../components/KLineChart'), { 
  ssr: false,
  loading: () => <div className="h-[400px] flex items-center justify-center bg-gray-900 rounded-xl">Loading Chart Engine...</div>
});

export default function Home() {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 250)');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState<{code: string, data: any[]} | null>(null);
  const [chartLoading, setChartLoading] = useState(false);

  const handleSelect = async () => {
    setLoading(true);
    setResults([]);
    try {
      // 聚合 3 个节点的结果
      const nodePromises = [0, 1, 2].map(id => 
        fetch(`/api/node${id}/api/v1/select`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ formula })
        }).then(r => r.json())
      );

      const allData = await Promise.all(nodePromises);
      const combined = allData.flatMap(d => d.results || []);
      setResults(Array.from(new Set(combined))); // 去重
    } catch (err) {
      alert('Selection failed. Check node status.');
    }
    setLoading(false);
  };

  const viewStock = async (code: string) => {
    setChartLoading(true);
    try {
      // 通过 API 路由获取 K 线
      const res = await fetch(`/api/kline?code=${code}`);
      const json = await res.json();
      if (json.data) {
        setSelectedStock({ code, data: json.data });
      }
    } catch (err) {
      alert('Failed to load kline');
    }
    setChartLoading(false);
  };

  return (
    <div className="min-h-screen bg-black text-white p-8">
      <div className="max-w-6xl mx-auto space-y-8">
        <h1 className="text-4xl font-black text-blue-500">BlinkQuant</h1>
        
        <div className="flex gap-4 bg-gray-900 p-6 rounded-2xl border border-gray-800">
          <input 
            className="flex-1 bg-black border border-gray-700 rounded-lg px-4 py-2 font-mono"
            value={formula}
            onChange={(e) => setFormula(e.target.value)}
          />
          <button 
            onClick={handleSelect}
            className="bg-blue-600 px-6 py-2 rounded-lg font-bold hover:bg-blue-500 transition-colors"
            disabled={loading}
          >
            {loading ? 'Scanning...' : 'Run Selection'}
          </button>
        </div>

        <div className="grid grid-cols-4 gap-8">
          <div className="col-span-1 bg-gray-900 rounded-2xl border border-gray-800 h-[600px] overflow-y-auto p-4">
            <h2 className="text-gray-500 text-sm font-bold mb-4 uppercase">Results ({results.length})</h2>
            <div className="space-y-2">
              {results.map(code => (
                <div 
                  key={code}
                  onClick={() => viewStock(code)}
                  className={`p-2 rounded cursor-pointer font-mono text-sm transition-colors ${selectedStock?.code === code ? 'bg-blue-600' : 'hover:bg-gray-800 text-gray-400'}`}
                >
                  {code}
                </div>
              ))}
            </div>
          </div>

          <div className="col-span-3 space-y-4">
            {selectedStock ? (
              <KLineChart code={selectedStock.code} data={selectedStock.data} />
            ) : (
              <div className="h-[400px] bg-gray-900 rounded-2xl border border-gray-800 flex items-center justify-center text-gray-600">
                Select a stock to view K-Line
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

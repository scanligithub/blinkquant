'use client';
import { useState } from 'react';

export default function Home() {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<any>(null);

  const handleSelect = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ formula, timeframe: 'D' })
      });
      const data = await res.json();
      setResults(data.data || []);
      setStatus(data.meta);
    } catch (err) {
      alert('Selection failed');
    }
    setLoading(false);
  };

  return (
    <main className="min-h-screen bg-gray-950 text-white p-8">
      <div className="max-w-4xl mx-auto space-y-8">
        <header className="border-b border-gray-800 pb-4">
          <h1 className="text-2xl font-bold text-blue-400">BlinkQuant Control Panel</h1>
          <p className="text-gray-400 text-sm">Distributed Engine: 3 Nodes Active</p>
        </header>

        {/* 选股器区域 */}
        <section className="bg-gray-900 p-6 rounded-xl border border-gray-800">
          <label className="block text-sm font-medium mb-2">Quant Formula (AST Mode)</label>
          <div className="flex gap-4">
            <input 
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-4 py-2 font-mono text-green-400 focus:outline-none focus:border-blue-500"
              value={formula}
              onChange={(e) => setFormula(e.target.value)}
            />
            <button 
              onClick={handleSelect}
              disabled={loading}
              className="bg-blue-600 hover:bg-blue-700 px-6 py-2 rounded font-bold transition disabled:opacity-50"
            >
              {loading ? 'Calculating...' : 'Run Selection'}
            </button>
          </div>
        </section>

        {/* 结果展示 */}
        <section className="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div className="bg-gray-900 p-6 rounded-xl border border-gray-800">
            <h2 className="text-lg font-semibold mb-4 border-b border-gray-800 pb-2">Results ({results.length})</h2>
            <div className="h-96 overflow-y-auto grid grid-cols-3 gap-2 text-sm">
              {results.map(code => (
                <div key={code} className="bg-gray-800 px-2 py-1 rounded text-center hover:bg-gray-700 cursor-pointer border border-transparent hover:border-blue-500 transition">
                  {code}
                </div>
              ))}
            </div>
          </div>

          <div className="bg-gray-900 p-6 rounded-xl border border-gray-800">
            <h2 className="text-lg font-semibold mb-4 border-b border-gray-800 pb-2">System Telemetry</h2>
            <div className="space-y-4 text-sm font-mono">
              <div className="flex justify-between">
                <span>Nodes Responding:</span>
                <span className="text-green-500">{status?.nodes_responding || 0} / 3</span>
              </div>
              <div className="flex justify-between">
                <span>Aggregation Mode:</span>
                <span className="text-blue-500">Vercel Edge</span>
              </div>
              {status?.errors && (
                <div className="mt-4 p-2 bg-red-900/30 border border-red-800 rounded text-red-400 text-xs">
                  {status.errors.map((e: string, i: number) => <div key={i}>{e}</div>)}
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}

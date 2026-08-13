'use client';
import { useState } from 'react';

interface AISelectModalProps {
  onClose: () => void;
  onRun: (formula: string, timeframe: string) => void;
}

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

export default function AISelectModal({ onClose, onRun }: AISelectModalProps) {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState<{ formula: string; timeframe: string; explanation: string } | null>(null);
  const [timeframe, setTimeframe] = useState('D');

  const translate = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError('');
    setResult(null);
    try {
      const res = await fetch('/api/select-nl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: query.trim() }),
      });
      const json = await res.json();
      if (!res.ok) {
        if (json.code === 'RATE_LIMITED' && json.retryAfterMs) {
          setError(`调用过于频繁，请 ${Math.ceil(json.retryAfterMs / 1000)} 秒后重试`);
        } else {
          setError(json.error || '翻译失败');
        }
        return;
      }
      setResult(json.data);
      setTimeframe(json.data.timeframe);
    } catch (e) {
      setError('AI 选股服务暂不可用');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl w-full max-w-lg shadow-xl p-6" onClick={(e) => e.stopPropagation()}>
        <h2 className="font-bold text-slate-700 mb-4">AI 选股</h2>

        <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">描述你的选股条件</label>
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          rows={2}
          className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
          placeholder="例如：市盈率低于20且总市值大于100亿的股票"
        />
        <div className="mt-3 flex justify-end gap-2">
          <button
            onClick={translate}
            disabled={loading || !query.trim()}
            className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50 flex items-center gap-2"
          >
            {loading && <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>}
            翻译
          </button>
        </div>

        {error && (
          <div className="mt-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>
        )}

        {result && (
          <div className="mt-4">
            <div className="flex items-center justify-between gap-2">
              <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">公式预览（可编辑）</label>
              <div className="flex items-center gap-1">
                {TIMEFRAMES.map((tf) => (
                  <button
                    key={tf.value}
                    onClick={() => setTimeframe(tf.value)}
                    className={`px-2 py-1 text-xs font-bold rounded-md ${timeframe === tf.value ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100'}`}
                  >
                    {tf.label}
                  </button>
                ))}
              </div>
            </div>
            <textarea
              value={result.formula}
              onChange={(e) => setResult({ ...result, formula: e.target.value })}
              rows={2}
              className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
            />
            {result.explanation && (
              <div className="mt-2 text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                {result.explanation}
              </div>
            )}
            <div className="mt-4 flex justify-end gap-2">
              <button onClick={onClose} className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50">
                取消
              </button>
              <button
                onClick={() => onRun(result.formula, timeframe)}
                disabled={!result.formula.trim()}
                className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50"
              >
                运行选股
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

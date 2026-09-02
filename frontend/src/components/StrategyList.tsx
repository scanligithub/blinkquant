'use client';
import { useState, useEffect, useCallback } from 'react';

export interface Strategy {
  id: number;
  name: string;
  formula: string;
  timeframe: string;
  created_at: string;
  updated_at: string;
}

interface StrategyListProps {
  onApply: (formula: string, timeframe: string) => void;
  onClose: () => void;
}

export default function StrategyList({ onApply, onClose }: StrategyListProps) {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/strategies', { cache: 'no-store' });
      const json = await res.json();
      if (!res.ok) {
        setError(json.error || '加载失败');
        setStrategies([]);
      } else {
        setStrategies(json.strategies || []);
      }
    } catch (e) {
      setError('网络错误');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const remove = async (id: number) => {
    if (!confirm('确定删除该策略？')) return;
    try {
      await fetch(`/api/strategies/${id}`, { method: 'DELETE' });
      setStrategies((prev) => prev.filter((s) => s.id !== id));
    } catch (e) {
      console.error('Failed to delete strategy', e);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={onClose}>
      <div
        className="bg-white rounded-2xl w-full max-w-lg max-h-[80vh] flex flex-col shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-4 border-b flex justify-between items-center">
          <h2 className="font-bold text-slate-700">我的策略</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-600 text-xl px-2">×</button>
        </div>
        <div className="flex-1 overflow-y-auto p-4 space-y-3 custom-scrollbar">
          {loading && <div className="text-center text-slate-400 py-8">加载中...</div>}
          {error && <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-xl px-3 py-2">{error}</div>}
          {!loading && !error && strategies.length === 0 && (
            <div className="text-center text-slate-400 py-8">暂无保存的策略</div>
          )}
          {strategies.map((s) => (
            <div key={s.id} className="border border-slate-200 rounded-xl p-3 flex items-center justify-between gap-3">
              <div className="flex-1 min-w-0">
                <div className="font-semibold text-slate-800">{s.name}</div>
                <div className="text-xs font-mono text-slate-500 truncate mt-0.5">{s.formula}</div>
                <div className="text-xs text-slate-400 mt-0.5">周期: {s.timeframe}</div>
              </div>
              <div className="flex gap-2 shrink-0">
                <button
                  onClick={() => onApply(s.formula, s.timeframe)}
                  className="bg-blue-600 hover:bg-blue-700 text-white text-xs font-bold px-3 py-1.5 rounded-lg"
                >
                  应用
                </button>
                <button
                  onClick={() => remove(s.id)}
                  className="text-xs text-red-500 hover:bg-red-50 border border-red-200 rounded-lg px-2.5 py-1.5"
                >
                  删除
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

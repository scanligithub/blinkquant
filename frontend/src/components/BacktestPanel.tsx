'use client';

import { useState } from 'react';

interface BacktestPanelProps {
  initialFormula?: string;
  onRun: (params: {
    formula: string;
    start_date: string;
    end_signal_date: string;
    initial_cash: number;
  }) => void;
  loading: boolean;
}

export default function BacktestPanel({ initialFormula = '', onRun, loading }: BacktestPanelProps) {
  const [formula, setFormula] = useState(initialFormula);
  const [startDate, setStartDate] = useState('2024-01-02');
  const [endDate, setEndDate] = useState('2024-12-30');
  const [cash, setCash] = useState('10000000');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onRun({
      formula,
      start_date: startDate,
      end_signal_date: endDate,
      initial_cash: parseFloat(cash),
    });
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">策略公式</label>
        <input
          type="text"
          value={formula}
          onChange={(e) => setFormula(e.target.value)}
          placeholder="CLOSE > MA(CLOSE, 20)"
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
          required
        />
      </div>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">起始日期</label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            required
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">结束日期</label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            required
          />
        </div>
      </div>
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">初始资金</label>
        <input
          type="text"
          value={cash}
          onChange={(e) => setCash(e.target.value.replace(/[^0-9]/g, ''))}
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
          required
        />
      </div>
      <button
        type="submit"
        disabled={loading || !formula.trim()}
        className="w-full px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {loading ? '回测中...' : '运行回测'}
      </button>
    </form>
  );
}

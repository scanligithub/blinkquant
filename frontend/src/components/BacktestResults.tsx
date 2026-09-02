'use client';

import { useState } from 'react';
import EquityCurveChart from './EquityCurveChart';
import TradesTable from './TradesTable';
import PositionsTable from './PositionsTable';

interface BacktestResult {
  formula: string;
  start_date: string;
  signal_end_date: string;
  valuation_end_date: string;
  initial_cash: number;
  equity_curve: Array<{ date: string; equity: number; cash: number; positions_value: number }>;
  trades: Array<{
    signal_date: string;
    execution_date: string;
    code: string;
    side: string;
    qty: number;
    price: number;
    fee: number;
  }>;
  positions_daily: Array<{
    date: string;
    code: string;
    qty: number;
    cost: number;
    market_value: number;
  }>;
  metrics: {
    total_return: number;
    cagr: number;
    sharpe: number;
    max_drawdown: number;
    total_days: number;
  };
}

interface BacktestResultsProps {
  result: BacktestResult;
}

export default function BacktestResults({ result }: BacktestResultsProps) {
  const [tab, setTab] = useState<'equity' | 'trades' | 'positions'>('equity');

  const finalEquity = result.equity_curve[result.equity_curve.length - 1]?.equity ?? 0;
  const m = result.metrics;

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-2 text-center">
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">最终权益</div>
          <div className="text-sm font-semibold">{finalEquity.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">收益率</div>
          <div className={`text-sm font-semibold ${(m.total_return ?? 0) >= 0 ? 'text-red-600' : 'text-green-600'}`}>
            {((m.total_return ?? 0) * 100).toFixed(2)}%
          </div>
        </div>
        <div className="bg-gray-50 rounded-lg p-2">
          <div className="text-xs text-gray-500">最大回撤</div>
          <div className="text-sm font-semibold text-green-600">
            {((m.max_drawdown ?? 0) * 100).toFixed(2)}%
          </div>
        </div>
      </div>

      <div className="flex gap-1 text-xs">
        {(['equity', 'trades', 'positions'] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-3 py-1.5 rounded-lg ${
              tab === t ? 'bg-blue-100 text-blue-700 font-medium' : 'text-gray-500 hover:bg-gray-100'
            }`}
          >
            {t === 'equity' ? '权益曲线' : t === 'trades' ? `交易 (${result.trades.length})` : '持仓'}
          </button>
        ))}
      </div>

      {tab === 'equity' && (
        <EquityCurveChart data={result.equity_curve.map((d) => ({ date: d.date, equity: d.equity }))} />
      )}
      {tab === 'trades' && <TradesTable trades={result.trades} />}
      {tab === 'positions' && <PositionsTable positions={result.positions_daily} />}
    </div>
  );
}

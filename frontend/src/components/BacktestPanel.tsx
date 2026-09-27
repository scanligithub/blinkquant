'use client';

import { useState } from 'react';

export interface BacktestParams {
  formula: string;
  start_date: string;
  end_signal_date: string;
  initial_cash: number;
  top_n: number;
  rebalance_freq: 'daily' | 'weekly';
  universe_type: 'all_a' | 'index';
  index_id?: string;
  min_listing_days: number;
  exclude_st: boolean;
  historical_fees: boolean;
  strategy: {
    universe: { type: 'all_a' | 'index'; index_id?: string };
    entry: { condition: string; trigger: 'condition' | 'cross_above' | 'cross_below'; timeframe: string };
    exit?: { condition: string; trigger: 'condition' | 'cross_above' | 'cross_below'; timeframe: string };
    sizing: { method: 'equal_weight' | 'top_n_equal_weight'; max_positions?: number };
    rebalance: { frequency: 'daily' | 'weekly' };
    mode: 'target_portfolio' | 'event_driven';
  };
}

interface BacktestPanelProps {
  initialFormula?: string;
  onRun: (params: BacktestParams) => void;
  loading: boolean;
}

export default function BacktestPanel({ initialFormula = '', onRun, loading }: BacktestPanelProps) {
  const [formula, setFormula] = useState(initialFormula);
  const [exitFormula, setExitFormula] = useState('');
  const [entryTrigger, setEntryTrigger] = useState<'condition' | 'cross_above' | 'cross_below'>('condition');
  const [exitTrigger, setExitTrigger] = useState<'condition' | 'cross_above' | 'cross_below'>('condition');
  const [mode, setMode] = useState<'target_portfolio' | 'event_driven'>('target_portfolio');
  const [universeType, setUniverseType] = useState<'all_a' | 'index'>('all_a');
  const [indexId, setIndexId] = useState('000300');
  const [sizingMethod, setSizingMethod] = useState<'equal_weight' | 'top_n_equal_weight'>('top_n_equal_weight');
  const [topN, setTopN] = useState('20');
  const [rebalanceFreq, setRebalanceFreq] = useState<'daily' | 'weekly'>('daily');
  const [timeframe, setTimeframe] = useState('D');
  const [startDate, setStartDate] = useState('2024-01-02');
  const [endDate, setEndDate] = useState('2024-12-30');
  const [cash, setCash] = useState('10000000');
  const [minListingDays, setMinListingDays] = useState('0');
  const [excludeSt, setExcludeSt] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const maxPositions = Math.max(1, parseInt(topN, 10) || 20);
    const strategy: BacktestParams['strategy'] = {
      universe: universeType === 'index'
        ? { type: 'index', index_id: indexId.trim() || '000300' }
        : { type: 'all_a' },
      entry: { condition: formula.trim(), trigger: entryTrigger, timeframe },
      sizing: sizingMethod === 'equal_weight'
        ? { method: 'equal_weight' }
        : { method: 'top_n_equal_weight', max_positions: maxPositions },
      rebalance: { frequency: rebalanceFreq },
      mode,
    };

    if (exitFormula.trim()) {
      strategy.exit = {
        condition: exitFormula.trim(),
        trigger: exitTrigger,
        timeframe,
      };
    }

    onRun({
      formula: formula.trim(),
      start_date: startDate,
      end_signal_date: endDate,
      initial_cash: parseFloat(cash),
      top_n: maxPositions,
      rebalance_freq: rebalanceFreq,
      universe_type: universeType,
      index_id: universeType === 'index' ? (indexId.trim() || '000300') : undefined,
      min_listing_days: Math.max(0, parseInt(minListingDays, 10) || 0),
      exclude_st: excludeSt,
      historical_fees: true,
      strategy,
    });
  };

  const triggerLabel = (value: string) => ({
    condition: '条件成立',
    cross_above: '上穿',
    cross_below: '下穿',
  } as Record<string, string>)[value] || value;

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Entry 条件</label>
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
          <label className="block text-xs font-medium text-gray-500 mb-1">Entry 触发</label>
          <select value={entryTrigger} onChange={(e) => setEntryTrigger(e.target.value as typeof entryTrigger)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            {(['condition', 'cross_above', 'cross_below'] as const).map(v => <option key={v} value={v}>{triggerLabel(v)}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">信号周期</label>
          <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            <option value="D">日</option>
            <option value="W">周</option>
            <option value="M">月</option>
          </select>
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Exit 条件（可选）</label>
        <input
          type="text"
          value={exitFormula}
          onChange={(e) => setExitFormula(e.target.value)}
          placeholder={mode === 'event_driven' ? 'event_driven 必填，例如 MA(CLOSE,5) < MA(CLOSE,20)' : '留空则按目标组合调仓退出'}
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
          required={mode === 'event_driven'}
        />
      </div>

      {exitFormula.trim() && (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Exit 触发</label>
          <select value={exitTrigger} onChange={(e) => setExitTrigger(e.target.value as typeof exitTrigger)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            {(['condition', 'cross_above', 'cross_below'] as const).map(v => <option key={v} value={v}>{triggerLabel(v)}</option>)}
          </select>
        </div>
      )}

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">股票池</label>
          <select value={universeType} onChange={(e) => setUniverseType(e.target.value as typeof universeType)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            <option value="all_a">全 A</option>
            <option value="index">指数成分股</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">策略模式</label>
          <select value={mode} onChange={(e) => setMode(e.target.value as typeof mode)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            <option value="target_portfolio">目标组合</option>
            <option value="event_driven">事件驱动</option>
          </select>
        </div>
      </div>

      {universeType === 'index' && (
        <input
          value={indexId}
          onChange={(e) => setIndexId(e.target.value)}
          placeholder="指数代码，例如 000300"
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono"
          required
        />
      )}

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">仓位方式</label>
          <select value={sizingMethod} onChange={(e) => setSizingMethod(e.target.value as typeof sizingMethod)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
            <option value="top_n_equal_weight">Top-N 等权</option>
            <option value="equal_weight">全选等权</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">最大持仓数</label>
          <input
            type="number" min="1" value={topN}
            onChange={(e) => setTopN(e.target.value)}
            disabled={sizingMethod === 'equal_weight'}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg disabled:bg-gray-50"
          />
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">调仓频率</label>
        <select value={rebalanceFreq} onChange={(e) => setRebalanceFreq(e.target.value as typeof rebalanceFreq)}
          className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg">
          <option value="daily">每日</option>
          <option value="weekly">每周</option>
        </select>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">起始日期</label>
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg" required />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">结束日期</label>
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg" required />
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">初始资金</label>
        <input
          type="text" value={cash}
          onChange={(e) => setCash(e.target.value.replace(/[^0-9.]/g, ''))}
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono"
          required
        />
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">最少上市天数</label>
          <input type="number" min="0" value={minListingDays}
            onChange={(e) => setMinListingDays(e.target.value)}
            className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg" />
        </div>
        <label className="flex items-center gap-2 text-xs text-gray-600 pt-6">
          <input type="checkbox" checked={excludeSt} onChange={(e) => setExcludeSt(e.target.checked)} />
          排除 ST
        </label>
      </div>

      <button
        type="submit"
        disabled={loading || !formula.trim() || (mode === 'event_driven' && !exitFormula.trim())}
        className="w-full px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {loading ? '回测中...' : '运行回测'}
      </button>
    </form>
  );
}

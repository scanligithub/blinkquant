'use client';

import { useEffect, useState } from 'react';
import BacktestStrategyTemplates, { type BacktestTemplateConfig } from './BacktestStrategyTemplates';

export interface BacktestParams {
  formula: string;
  start_date: string;
  end_signal_date: string;
  initial_cash: number;
  universe_type: 'all_a' | 'index';
  index_id?: string;
  min_listing_days: number;
  exclude_st: boolean;
  historical_fees: boolean;
  benchmark: { enabled: boolean; type: 'index'; index_id: string };
  fee_policy: {
    mode: 'historical' | 'fixed';
    commission_rate?: number;
    commission_min?: number;
    stamp_tax_rate?: number;
    transfer_fee_rate?: number;
  };
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
  onTemplateSelected?: (templateId: number | null) => void;
}

const TRIGGERS = ['condition', 'cross_above', 'cross_below'] as const;
const TIMEFRAMES = ['D', 'W', 'M'] as const;

export default function BacktestPanel({ initialFormula = '', onRun, loading, onTemplateSelected }: BacktestPanelProps) {
  const [formula, setFormula] = useState(initialFormula);
  const [exitFormula, setExitFormula] = useState('');
  const [entryTrigger, setEntryTrigger] = useState<typeof TRIGGERS[number]>('condition');
  const [exitTrigger, setExitTrigger] = useState<typeof TRIGGERS[number]>('condition');
  const [mode, setMode] = useState<'target_portfolio' | 'event_driven'>('target_portfolio');
  const [universeType, setUniverseType] = useState<'all_a' | 'index'>('all_a');
  const [indexId, setIndexId] = useState('000300');
  const [sizingMethod, setSizingMethod] = useState<'equal_weight' | 'top_n_equal_weight'>('top_n_equal_weight');
  const [topN, setTopN] = useState('20');
  const [rebalanceFreq, setRebalanceFreq] = useState<'daily' | 'weekly'>('daily');
  const [entryTimeframe, setEntryTimeframe] = useState<typeof TIMEFRAMES[number]>('D');
  const [exitTimeframe, setExitTimeframe] = useState<typeof TIMEFRAMES[number]>('D');
  const [startDate, setStartDate] = useState('2024-01-02');
  const [endDate, setEndDate] = useState('2024-12-30');
  const [cash, setCash] = useState('10000000');
  const [minListingDays, setMinListingDays] = useState('0');
  const [excludeSt, setExcludeSt] = useState(false);
  const [benchmarkEnabled, setBenchmarkEnabled] = useState(true);
  const [benchmarkIndex, setBenchmarkIndex] = useState('000300');
  const [feeMode, setFeeMode] = useState<'historical' | 'fixed'>('historical');
  const [commissionRate, setCommissionRate] = useState('0.00025');
  const [commissionMin, setCommissionMin] = useState('5');
  const [stampTaxRate, setStampTaxRate] = useState('0.0005');
  const [transferFeeRate, setTransferFeeRate] = useState('0.00001');

  const templateConfig: BacktestTemplateConfig = {
    strategy: {
      universe: universeType === 'index'
        ? { type: 'index', index_id: indexId.trim() || '000300' }
        : { type: 'all_a' },
      entry: { condition: formula.trim(), trigger: entryTrigger, timeframe: entryTimeframe },
      ...(exitFormula.trim() ? {
        exit: { condition: exitFormula.trim(), trigger: exitTrigger, timeframe: exitTimeframe },
      } : {}),
      sizing: sizingMethod === 'equal_weight'
        ? { method: 'equal_weight' }
        : { method: 'top_n_equal_weight', max_positions: Math.max(1, parseInt(topN, 10) || 20) },
      rebalance: { frequency: rebalanceFreq },
      mode,
    },
    fee_policy: {
      mode: feeMode,
      ...(feeMode === 'fixed' ? {
        commission_rate: parseFloat(commissionRate),
        commission_min: parseFloat(commissionMin),
        stamp_tax_rate: parseFloat(stampTaxRate),
        transfer_fee_rate: parseFloat(transferFeeRate),
      } : {}),
    },
    benchmark: { enabled: benchmarkEnabled, type: 'index', index_id: benchmarkIndex.trim() || '000300' },
    min_listing_days: Math.max(0, parseInt(minListingDays, 10) || 0),
    exclude_st: excludeSt,
  };

  const applyTemplate = (config: BacktestTemplateConfig) => {
    const s = config.strategy || {};
    const u = s.universe || {};
    const entry = s.entry || {};
    const exit = s.exit || {};
    const sizing = s.sizing || {};
    const fee = config.fee_policy || {};
    const benchmark = config.benchmark || {};
    setUniverseType(u.type === 'index' ? 'index' : 'all_a');
    setIndexId(String(u.index_id || '000300'));
    setFormula(String(entry.condition || ''));
    setEntryTrigger(entry.trigger || 'condition');
    setEntryTimeframe(entry.timeframe === 'W' || entry.timeframe === 'M' ? entry.timeframe : 'D');
    setExitFormula(String(exit.condition || ''));
    setExitTrigger(exit.trigger || 'condition');
    setExitTimeframe(exit.timeframe === 'W' || exit.timeframe === 'M' ? exit.timeframe : 'D');
    setSizingMethod(sizing.method === 'equal_weight' ? 'equal_weight' : 'top_n_equal_weight');
    setTopN(String(sizing.max_positions || 20));
    setRebalanceFreq(s.rebalance?.frequency === 'weekly' ? 'weekly' : 'daily');
    setMode(s.mode === 'event_driven' ? 'event_driven' : 'target_portfolio');
    setFeeMode(fee.mode === 'fixed' ? 'fixed' : 'historical');
    setCommissionRate(String(fee.commission_rate ?? '0.00025'));
    setCommissionMin(String(fee.commission_min ?? '5'));
    setStampTaxRate(String(fee.stamp_tax_rate ?? '0.0005'));
    setTransferFeeRate(String(fee.transfer_fee_rate ?? '0.00001'));
    setBenchmarkEnabled(benchmark.enabled !== false);
    setBenchmarkIndex(String(benchmark.index_id || '000300'));
    setMinListingDays(String(config.min_listing_days || 0));
    setExcludeSt(!!config.exclude_st);
  };

  const triggerLabel = (value: string) => ({
    condition: '条件成立',
    cross_above: '上穿',
    cross_below: '下穿',
  } as Record<string, string>)[value] || value;

  // 从策略库进入回测工作台时，恢复指定的 Node1 SQLite 回测策略模板。\n  useEffect(() => {\n    let active = true;\n    const raw = sessionStorage.getItem('bq-pending-backtest-template');\n    if (!raw) return () => { active = false; };\n\n    sessionStorage.removeItem('bq-pending-backtest-template');\n    let pending: { id?: number } = {};\n    try {\n      pending = JSON.parse(raw);\n    } catch {\n      return () => { active = false; };\n    }\n    if (!Number.isInteger(pending.id) || Number(pending.id) <= 0) {\n      return () => { active = false; };\n    }\n\n    (async () => {\n      try {\n        const res = await fetch('/api/backtest-strategy-templates', { cache: 'no-store' });\n        const json = await res.json();\n        if (!res.ok || !active) return;\n        const template = (json.templates || []).find((item: any) => Number(item.id) === Number(pending.id));\n        if (template?.config) {\n          applyTemplate(template.config);\n          onTemplateSelected?.(Number(template.id));\n        }\n      } catch {\n        // 回测工作台仍可继续使用当前默认参数。\n      }\n    })();\n\n    return () => { active = false; };\n  // eslint-disable-next-line react-hooks/exhaustive-deps\n  }, []);\n\n  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const entryCondition = formula.trim();
    const exitCondition = exitFormula.trim();

    if (!entryCondition || (mode === 'event_driven' && !exitCondition)) return;

    const maxPositions = Math.max(1, parseInt(topN, 10) || 20);
    const strategy: BacktestParams['strategy'] = {
      universe: universeType === 'index'
        ? { type: 'index', index_id: indexId.trim() || '000300' }
        : { type: 'all_a' },
      entry: { condition: entryCondition, trigger: entryTrigger, timeframe: entryTimeframe },
      sizing: sizingMethod === 'equal_weight'
        ? { method: 'equal_weight' }
        : { method: 'top_n_equal_weight', max_positions: maxPositions },
      rebalance: { frequency: rebalanceFreq },
      mode,
    };

    if (exitCondition) {
      strategy.exit = {
        condition: exitCondition,
        trigger: exitTrigger,
        timeframe: exitTimeframe,
      };
    }

    onRun({
      formula: entryCondition,
      start_date: startDate,
      end_signal_date: endDate,
      initial_cash: parseFloat(cash),
      universe_type: universeType,
      index_id: universeType === 'index' ? (indexId.trim() || '000300') : undefined,
      min_listing_days: Math.max(0, parseInt(minListingDays, 10) || 0),
      exclude_st: excludeSt,
      historical_fees: feeMode === 'historical',
      benchmark: { enabled: benchmarkEnabled, type: 'index', index_id: benchmarkIndex.trim() || '000300' },
      fee_policy: {
        mode: feeMode,
        ...(feeMode === 'fixed' ? {
          commission_rate: parseFloat(commissionRate),
          commission_min: parseFloat(commissionMin),
          stamp_tax_rate: parseFloat(stampTaxRate),
          transfer_fee_rate: parseFloat(transferFeeRate),
        } : {}),
      },
      strategy,
    });
  };

  const selectClass = 'w-full px-2 py-2 text-sm border border-gray-200 rounded-lg';
  const inputClass = 'w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono';

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Entry 条件（必填）</label>
        <input
          type="text"
          value={formula}
          onChange={(e) => setFormula(e.target.value)}
          placeholder="CLOSE > MA(CLOSE, 20)"
          className={inputClass}
          required
        />
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Entry 触发</label>
          <select value={entryTrigger} onChange={(e) => setEntryTrigger(e.target.value as typeof entryTrigger)} className={selectClass}>
            {TRIGGERS.map(v => <option key={v} value={v}>{triggerLabel(v)}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">Entry 信号周期</label>
          <select value={entryTimeframe} onChange={(e) => setEntryTimeframe(e.target.value as typeof entryTimeframe)} className={selectClass}>
            {TIMEFRAMES.map(v => <option key={v} value={v}>{v === 'D' ? '日' : v === 'W' ? '周' : '月'}</option>)}
          </select>
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">Exit 条件（可选；事件驱动必填）</label>
        <input
          type="text"
          value={exitFormula}
          onChange={(e) => setExitFormula(e.target.value)}
          placeholder={mode === 'event_driven' ? '例如：MA(CLOSE,5) < MA(CLOSE,20)' : '留空则按目标组合调仓退出'}
          className={inputClass}
          required={mode === 'event_driven'}
        />
      </div>

      {exitFormula.trim() && (
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Exit 触发</label>
            <select value={exitTrigger} onChange={(e) => setExitTrigger(e.target.value as typeof exitTrigger)} className={selectClass}>
              {TRIGGERS.map(v => <option key={v} value={v}>{triggerLabel(v)}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">Exit 信号周期</label>
            <select value={exitTimeframe} onChange={(e) => setExitTimeframe(e.target.value as typeof exitTimeframe)} className={selectClass}>
              {TIMEFRAMES.map(v => <option key={v} value={v}>{v === 'D' ? '日' : v === 'W' ? '周' : '月'}</option>)}
            </select>
          </div>
        </div>
      )}

      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-[11px] text-slate-500">
        Entry：{triggerLabel(entryTrigger)} · {entryTimeframe}　
        {exitFormula.trim() ? `Exit：${triggerLabel(exitTrigger)} · ${exitTimeframe}` : 'Exit：未设置'}
        {' · '}模式：{mode === 'event_driven' ? '事件驱动' : '目标组合'}
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">股票池</label>
          <select value={universeType} onChange={(e) => setUniverseType(e.target.value as typeof universeType)} className={selectClass}>
            <option value="all_a">全 A</option>
            <option value="index">指数成分股</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">策略模式</label>
          <select value={mode} onChange={(e) => setMode(e.target.value as typeof mode)} className={selectClass}>
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
          className={inputClass}
          required
        />
      )}

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">仓位方式</label>
          <select value={sizingMethod} onChange={(e) => setSizingMethod(e.target.value as typeof sizingMethod)} className={selectClass}>
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
            className={selectClass + ' disabled:bg-gray-50'}
          />
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">调仓频率</label>
        <select value={rebalanceFreq} onChange={(e) => setRebalanceFreq(e.target.value as typeof rebalanceFreq)} className={selectClass}>
          <option value="daily">每日</option>
          <option value="weekly">每周</option>
        </select>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">起始日期</label>
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg" required />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">结束日期</label>
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg" required />
        </div>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">初始资金</label>
        <input
          type="text" value={cash}
          onChange={(e) => setCash(e.target.value.replace(/[^0-9.]/g, ''))}
          className={inputClass}
          required
        />
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">最少上市天数</label>
          <input type="number" min="0" value={minListingDays} onChange={(e) => setMinListingDays(e.target.value)} className="w-full px-2 py-2 text-sm border border-gray-200 rounded-lg" />
        </div>
        <label className="flex items-center gap-2 text-xs text-gray-600 pt-6">
          <input type="checkbox" checked={excludeSt} onChange={(e) => setExcludeSt(e.target.checked)} />
          排除 ST
        </label>
      </div>

      <div>
        <label className="flex items-center gap-2 text-xs text-gray-600">
          <input type="checkbox" checked={benchmarkEnabled} onChange={(e) => setBenchmarkEnabled(e.target.checked)} />
          启用基准指数
        </label>
      </div>

      {benchmarkEnabled && (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">基准指数</label>
          <input
            value={benchmarkIndex}
            onChange={(e) => setBenchmarkIndex(e.target.value)}
            placeholder="例如 000300（沪深300）"
            className={inputClass}
          />
        </div>
      )}

      <div>
        <label className="block text-xs font-medium text-gray-500 mb-1">费率方案</label>
        <select value={feeMode} onChange={(e) => setFeeMode(e.target.value as typeof feeMode)} className={selectClass}>
          <option value="historical">历史真实费率</option>
          <option value="fixed">固定研究费率</option>
        </select>
      </div>

      {feeMode === 'fixed' && (
        <div className="grid grid-cols-2 gap-2 rounded-lg border border-slate-200 bg-slate-50 p-2">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">佣金率</label>
            <input type="number" min="0" step="0.00001" value={commissionRate} onChange={(e) => setCommissionRate(e.target.value)} className={selectClass} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">最低佣金</label>
            <input type="number" min="0" step="0.01" value={commissionMin} onChange={(e) => setCommissionMin(e.target.value)} className={selectClass} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">卖出印花税率</label>
            <input type="number" min="0" step="0.0001" value={stampTaxRate} onChange={(e) => setStampTaxRate(e.target.value)} className={selectClass} />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">过户费率</label>
            <input type="number" min="0" step="0.000001" value={transferFeeRate} onChange={(e) => setTransferFeeRate(e.target.value)} className={selectClass} />
          </div>
        </div>
      )}

      <BacktestStrategyTemplates
        currentConfig={templateConfig}
        onLoad={applyTemplate}
        onTemplateSelected={onTemplateSelected}
      />

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

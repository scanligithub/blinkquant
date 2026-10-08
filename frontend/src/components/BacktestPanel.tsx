'use client';

import { useEffect, useState } from 'react';
import BacktestStrategyTemplates, { type BacktestTemplateConfig, type SelectionStrategySource } from './BacktestStrategyTemplates';

export interface BacktestParams {
  formula: string;
  source_selection_strategy?: SelectionStrategySource;
  start_date: string;
  end_signal_date: string;
  initial_cash: number;
  universe_type: 'all_a' | 'index' | 'watchlist';
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
    universe: { type: 'all_a' | 'index' | 'watchlist'; index_id?: string; watchlist_id?: number };
    entry: { condition: string; trigger: 'condition' | 'cross_above' | 'cross_below'; timeframe: string };
    exit?: { condition: string; trigger: 'condition' | 'cross_above' | 'cross_below'; timeframe: string };
    sizing: { method: 'equal_weight' | 'top_n_equal_weight'; max_positions?: number };
    rebalance: { frequency: 'daily' | 'weekly' };
    mode: 'target_portfolio' | 'event_driven';
  };
}

interface WatchlistOption { id: number; name: string; item_count: number; is_default: boolean }

interface BacktestPanelProps {
  initialFormula?: string;
  onRun: (params: BacktestParams) => void;
  loading: boolean;
  onTemplateSelected?: (templateId: number | null) => void;
}

const TRIGGERS = ['condition', 'cross_above', 'cross_below'] as const;
const TIMEFRAMES = ['D', 'W', 'M'] as const;

// 与 stockA 当前生产数据源的 37 个 TARGET_INDEXES 保持一致。
// 指数成分股只允许选择数据源实际提供历史成分数据的指数。
const INDEX_OPTIONS = [
  { id: '000001', name: '上证指数' },
  { id: '399001', name: '深证成指' },
  { id: '399006', name: '创业板指' },
  { id: '000688', name: '科创50' },
  { id: '899050', name: '北证50' },
  { id: '000016', name: '上证50' },
  { id: '000300', name: '沪深300' },
  { id: '000905', name: '中证500' },
  { id: '000852', name: '中证1000' },
  { id: '000851', name: '中证2000' },
  { id: '399303', name: '国证2000' },
  { id: '399330', name: '深证100' },
  { id: '000010', name: '上证180' },
  { id: '399324', name: '深证红利' },
  { id: '000015', name: '红利指数' },
  { id: '000904', name: '中证中盘200' },
  { id: '399311', name: '国证1000' },
  { id: '930713', name: '中证人工智能主题' },
  { id: '980017', name: '国证芯片' },
  { id: '399354', name: '分析师指数' },
  { id: '399673', name: '创业板50' },
  { id: '399412', name: '国证新能源' },
  { id: '399005', name: '中小100' },
  { id: '399994', name: '中证信息安全' },
  { id: '399975', name: '证券公司' },
  { id: '399986', name: '中证银行' },
  { id: '399932', name: '中证消费' },
  { id: '399933', name: '中证医药' },
  { id: '399967', name: '中证军工' },
  { id: '399989', name: '中证医疗' },
  { id: '399971', name: '中证传媒' },
  { id: '399997', name: '中证白酒' },
  { id: '000928', name: '中证能源' },
  { id: '000929', name: '中证原材料' },
  { id: '399990', name: '煤炭等权' },
  { id: '930708', name: '中证有色' },
  { id: '399974', name: '国证国企' },
] as const;

export default function BacktestPanel({ initialFormula = '', onRun, loading, onTemplateSelected }: BacktestPanelProps) {
  const [formula, setFormula] = useState(initialFormula);
  const [exitFormula, setExitFormula] = useState('');
  const [entryTrigger, setEntryTrigger] = useState<typeof TRIGGERS[number]>('condition');
  const [exitTrigger, setExitTrigger] = useState<typeof TRIGGERS[number]>('condition');
  const [mode, setMode] = useState<'target_portfolio' | 'event_driven'>('target_portfolio');
  const [universeType, setUniverseType] = useState<'all_a' | 'index' | 'watchlist'>('all_a');
  const [watchlists, setWatchlists] = useState<WatchlistOption[]>([]);
  const [watchlistId, setWatchlistId] = useState('');
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
  const [sourceSelectionStrategy, setSourceSelectionStrategy] = useState<SelectionStrategySource | undefined>();

  useEffect(() => {
    if (universeType !== 'watchlist') return;
    let active = true;
    fetch('/api/watchlists', { cache: 'no-store' }).then(async (res) => {
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || '加载自选股列表失败');
      if (!active) return;
      const next = Array.isArray(json.watchlists) ? json.watchlists : [];
      setWatchlists(next);
      if (!watchlistId && next.length) setWatchlistId(String(next[0].id));
    }).catch((error) => { if (active) console.error(error); });
    return () => { active = false; };
  }, [universeType, watchlistId]);

  const templateConfig: BacktestTemplateConfig = {
    strategy: {
      universe: universeType === 'index'
        ? { type: 'index', index_id: indexId.trim() || '000300' }
        : universeType === 'watchlist'
          ? { type: 'watchlist', watchlist_id: Number(watchlistId) }
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
    ...(sourceSelectionStrategy ? { source_selection_strategy: sourceSelectionStrategy } : {}),
  };

  const applyTemplate = (config: BacktestTemplateConfig) => {
    const s = config.strategy || {};
    const u = s.universe || {};
    const entry = s.entry || {};
    const exit = s.exit || {};
    const sizing = s.sizing || {};
    const fee = config.fee_policy || {};
    const benchmark = config.benchmark || {};
    setUniverseType(u.type === 'index' ? 'index' : u.type === 'watchlist' ? 'watchlist' : 'all_a');
    setWatchlistId(String(u.watchlist_id || ''));
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
    const source = config.source_selection_strategy;
    if (source && Number.isInteger(Number(source.id)) && Number(source.id) > 0) {
      setSourceSelectionStrategy({
        id: Number(source.id),
        version_no: Number(source.version_no) || 1,
        name: String(source.name || ''),
        formula: String(source.formula || ''),
        timeframe: source.timeframe === 'W' || source.timeframe === 'M' ? source.timeframe : 'D',
      });
    } else {
      setSourceSelectionStrategy(undefined);
    }
  };

  const handleSourceSelectionChanged = (source: SelectionStrategySource | undefined) => {
    setSourceSelectionStrategy(source);
    if (source) {
      setFormula(source.formula);
      setEntryTimeframe(source.timeframe);
    }
  };

  const triggerLabel = (value: string) => ({
    condition: '条件成立',
    cross_above: '上穿',
    cross_below: '下穿',
  } as Record<string, string>)[value] || value;

  // 从策略库进入回测工作台时，先恢复模板，再应用“从选股策略创建”来源快照。
  // 这样即使浏览器里残留旧的模板 pending key，也不会覆盖新选择的源策略。
  useEffect(() => {
    let active = true;

    const rawSource = sessionStorage.getItem('bq-pending-backtest-from-selection');
    let pendingSource: SelectionStrategySource | null = null;
    if (rawSource) {
      try {
        const parsed = JSON.parse(rawSource) as SelectionStrategySource;
        if (parsed && Number(parsed.id) > 0 && String(parsed.formula || '').trim()) {
          pendingSource = {
            id: Number(parsed.id),
            version_no: Number(parsed.version_no) || 1,
            name: String(parsed.name || ''),
            formula: String(parsed.formula).trim(),
            timeframe: parsed.timeframe === 'W' || parsed.timeframe === 'M' ? parsed.timeframe : 'D',
          };
        }
      } catch {
        // Ignore malformed browser state and keep defaults.
      }
      sessionStorage.removeItem('bq-pending-backtest-from-selection');
    }

    const applySource = (source: SelectionStrategySource | null) => {
      if (!source || !active) return;
      setSourceSelectionStrategy(source);
      setFormula(source.formula);
      setEntryTimeframe(source.timeframe);
    };

    const rawTemplate = sessionStorage.getItem('bq-pending-backtest-template');
    if (!rawTemplate) {
      applySource(pendingSource);
      return () => { active = false; };
    }

    let pending: { id?: number; config?: BacktestTemplateConfig } = {};
    try {
      pending = JSON.parse(rawTemplate);
    } catch {
      applySource(pendingSource);
      return () => { active = false; };
    }

    if (pending.config && typeof pending.config === 'object') {
      applyTemplate(pending.config);
      applySource(pendingSource);
      onTemplateSelected?.(null);
      sessionStorage.removeItem('bq-pending-backtest-template');
      return () => { active = false; };
    }

    if (!Number.isInteger(pending.id) || Number(pending.id) <= 0) {
      applySource(pendingSource);
      return () => { active = false; };
    }

    (async () => {
      try {
        const res = await fetch('/api/backtest-strategy-templates', { cache: 'no-store' });
        const json = await res.json();
        if (!res.ok || !active) return;
        const template = (json.templates || []).find((item: any) => Number(item.id) === Number(pending.id));
        if (template?.config) {
          applyTemplate(template.config);
          applySource(pendingSource);
          onTemplateSelected?.(Number(template.id));
          sessionStorage.removeItem('bq-pending-backtest-template');
        } else {
          applySource(pendingSource);
        }
      } catch {
        applySource(pendingSource);
        // 回测工作台仍可继续使用当前默认参数。
      }
    })();

    return () => { active = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const entryCondition = formula.trim();
    const exitCondition = exitFormula.trim();

    if (!entryCondition || (mode === 'event_driven' && !exitCondition)) return;
    if (universeType === 'watchlist' && (!Number.isInteger(Number(watchlistId)) || Number(watchlistId) <= 0)) return;

    const maxPositions = Math.max(1, parseInt(topN, 10) || 20);
    const strategy: BacktestParams['strategy'] = {
      universe: universeType === 'index'
        ? { type: 'index', index_id: indexId.trim() || '000300' }
        : universeType === 'watchlist'
          ? { type: 'watchlist', watchlist_id: Number(watchlistId) }
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
      ...(sourceSelectionStrategy ? { source_selection_strategy: sourceSelectionStrategy } : {}),
    });
  };

  const selectClass = 'w-full px-2 py-2 text-sm border border-gray-200 rounded-lg';
  const inputClass = 'w-full px-3 py-2 text-sm border border-gray-200 rounded-lg font-mono';

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      {sourceSelectionStrategy && (
        <div className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2">
          <div className="text-xs font-semibold text-blue-700">来源选股策略</div>
          <div className="mt-0.5 text-xs text-blue-800">
            {sourceSelectionStrategy.name || ('选股策略 #' + sourceSelectionStrategy.id)} · v{sourceSelectionStrategy.version_no}
          </div>
          <div className="mt-0.5 text-[10px] font-mono text-blue-600 break-all">
            {sourceSelectionStrategy.formula} · {sourceSelectionStrategy.timeframe}
          </div>
          <div className="mt-0.5 text-[10px] text-slate-500">仅保存版本快照；修改 Entry 不会修改源选股策略。</div>
        </div>
      )}
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
            <option value="watchlist">自选股列表</option>
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
        <select value={indexId} onChange={(e) => setIndexId(e.target.value)} className={selectClass} required>
          {INDEX_OPTIONS.map((index) => (
            <option key={index.id} value={index.id}>
              {index.id} · {index.name}
            </option>
          ))}
        </select>
      )}

      {universeType === 'watchlist' && (
        <select value={watchlistId} onChange={(e) => setWatchlistId(e.target.value)} className={selectClass} required>
          <option value="">选择自选股列表</option>
          {watchlists.map((list) => <option key={list.id} value={list.id}>{list.name} · {list.item_count} 只股票</option>)}
        </select>
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
        onSourceSelectionChanged={handleSourceSelectionChanged}
      />

      <button
        type="submit"
        disabled={loading || !formula.trim() || (mode === 'event_driven' && !exitFormula.trim()) || (universeType === 'watchlist' && !watchlistId)}
        className="w-full px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {loading ? '回测中...' : '运行回测'}
      </button>
    </form>
  );
}

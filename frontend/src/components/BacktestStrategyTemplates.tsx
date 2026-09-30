'use client';
import { useCallback, useEffect, useState } from 'react';

export interface BacktestTemplateConfig {
  strategy: any;
  fee_policy: any;
  benchmark: any;
  min_listing_days: number;
  exclude_st: boolean;
}
export interface BacktestStrategyTemplate {
  id: number;
  name: string;
  description?: string | null;
  config: BacktestTemplateConfig;
  updated_at: string;
}
// P4.1/P4.2: templates are persisted by Node1 Scheduler SQLite; Vercel only proxies authenticated requests.
interface Props {
  currentConfig: BacktestTemplateConfig;
  onLoad: (config: BacktestTemplateConfig) => void;
  onTemplateSelected?: (templateId: number | null) => void;
}

export const BUILT_IN_TEMPLATES: Array<{ id: string; name: string; description: string; config: BacktestTemplateConfig }> = [
  {
    id: 'ma20-trend',
    name: 'MA20 趋势',
    description: '全 A · CLOSE > MA20 · 每日调仓 · Top-N 20',
    config: {
      strategy: {
        universe: { type: 'all_a' },
        entry: { condition: 'CLOSE > MA(CLOSE,20)', trigger: 'condition', timeframe: 'D' },
        sizing: { method: 'top_n_equal_weight', max_positions: 20 },
        rebalance: { frequency: 'daily' },
        mode: 'target_portfolio',
      },
      fee_policy: { mode: 'historical' },
      benchmark: { enabled: true, type: 'index', index_id: '000300' },
      min_listing_days: 0,
      exclude_st: false,
    },
  },
  {
    id: 'ma5-ma20-trend',
    name: 'MA5/MA20 趋势',
    description: '全 A · MA5 > MA20 · 每日调仓 · Top-N 20',
    config: {
      strategy: {
        universe: { type: 'all_a' },
        entry: { condition: 'MA(CLOSE,5) > MA(CLOSE,20)', trigger: 'condition', timeframe: 'D' },
        sizing: { method: 'top_n_equal_weight', max_positions: 20 },
        rebalance: { frequency: 'daily' },
        mode: 'target_portfolio',
      },
      fee_policy: { mode: 'historical' },
      benchmark: { enabled: true, type: 'index', index_id: '000300' },
      min_listing_days: 0,
      exclude_st: false,
    },
  },
  {
    id: 'ma5-ma60-trend',
    name: 'MA5/MA60 趋势',
    description: '全 A · MA5 > MA60 · 每周调仓 · Top-N 20',
    config: {
      strategy: {
        universe: { type: 'all_a' },
        entry: { condition: 'MA(CLOSE,5) > MA(CLOSE,60)', trigger: 'condition', timeframe: 'D' },
        sizing: { method: 'top_n_equal_weight', max_positions: 20 },
        rebalance: { frequency: 'weekly' },
        mode: 'target_portfolio',
      },
      fee_policy: { mode: 'historical' },
      benchmark: { enabled: true, type: 'index', index_id: '000300' },
      min_listing_days: 30,
      exclude_st: true,
    },
  },
  {
    id: 'csi300-weekly',
    name: '沪深300 周调仓',
    description: '000300 成分股 · CLOSE > MA20 · 每周调仓 · Top-N 20',
    config: {
      strategy: {
        universe: { type: 'index', index_id: '000300' },
        entry: { condition: 'CLOSE > MA(CLOSE,20)', trigger: 'condition', timeframe: 'D' },
        sizing: { method: 'top_n_equal_weight', max_positions: 20 },
        rebalance: { frequency: 'weekly' },
        mode: 'target_portfolio',
      },
      fee_policy: { mode: 'historical' },
      benchmark: { enabled: true, type: 'index', index_id: '000300' },
      min_listing_days: 30,
      exclude_st: true,
    },
  },
];

export default function BacktestStrategyTemplates({ currentConfig, onLoad, onTemplateSelected }: Props) {
  const [open, setOpen] = useState(false);
  const [templates, setTemplates] = useState<BacktestStrategyTemplate[]>([]);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [editingId, setEditingId] = useState<number | null>(null);
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    setError('');
    const res = await fetch('/api/backtest-strategy-templates', { cache: 'no-store' });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || '加载模板失败');
    setTemplates(json.templates || []);
  }, []);

  useEffect(() => {
    if (open) refresh().catch(e => setError(e.message));
  }, [open, refresh]);

  const selectTemplate = (id: number | null) => {
    setSelectedTemplateId(id);
    onTemplateSelected?.(id);
  };

  const resetEditor = () => {
    setName('');
    setDescription('');
    setEditingId(null);
    setError('');
  };

  const save = async () => {
    const trimmedName = name.trim();
    if (!trimmedName) return;

    setLoading(true);
    setError('');
    try {
      const url = editingId == null
        ? '/api/backtest-strategy-templates'
        : `/api/backtest-strategy-templates?id=${editingId}`;
      const res = await fetch(url, {
        method: editingId == null ? 'POST' : 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: trimmedName,
          description: description.trim() || null,
          config: currentConfig,
        }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || (editingId == null ? '保存失败' : '更新失败'));
      const savedTemplateId = json?.template?.id;
      resetEditor();
      if (typeof savedTemplateId === 'number') selectTemplate(savedTemplateId);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const edit = (template: BacktestStrategyTemplate) => {
    selectTemplate(template.id);
    setEditingId(template.id);
    setName(template.name);
    setDescription(template.description || '');
    setError('');
    onLoad(template.config);
  };

  const copy = (template: BacktestStrategyTemplate) => {
    selectTemplate(null);
    setEditingId(null);
    setName(`${template.name} 副本`);
    setDescription(template.description || '');
    setError('');
    onLoad(template.config);
  };

  const remove = async (id: number) => {
    if (!confirm('确定删除该回测策略模板？')) return;
    setLoading(true);
    setError('');
    try {
      const res = await fetch(`/api/backtest-strategy-templates?id=${id}`, { method: 'DELETE' });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || '删除失败');
      if (editingId === id) resetEditor();
      if (selectedTemplateId === id) selectTemplate(null);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return <div className='space-y-2'>
    <button
      type='button'
      onClick={() => setOpen(v => !v)}
      className='w-full px-3 py-2 text-xs font-semibold border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-600'
    >
      {open ? '收起' : '策略模板'}
    </button>

    {open && <div className='rounded-lg border border-slate-200 bg-slate-50 p-3 space-y-3'>
      <div className='space-y-2'>
        <div className='flex gap-2'>
          <input
            value={name}
            onChange={e => setName(e.target.value)}
            placeholder='模板名称，例如：MA20 周线趋势'
            className='flex-1 px-3 py-2 text-xs border border-slate-200 rounded-lg bg-white'
          />
          <button
            type='button'
            disabled={loading || !name.trim()}
            onClick={save}
            className='px-3 py-2 text-xs font-semibold text-white bg-blue-600 rounded-lg disabled:opacity-50'
          >
            {editingId == null ? '保存' : '更新'}
          </button>
          {editingId != null && (
            <button
              type='button'
              disabled={loading}
              onClick={resetEditor}
              className='px-3 py-2 text-xs font-semibold text-slate-600 bg-white border border-slate-200 rounded-lg disabled:opacity-50'
            >
              取消
            </button>
          )}
        </div>

        <input
          value={description}
          onChange={e => setDescription(e.target.value)}
          placeholder='模板说明（可选）'
          className='w-full px-3 py-2 text-xs border border-slate-200 rounded-lg bg-white'
        />

        {editingId != null && (
          <div className='text-[10px] text-blue-600'>
            正在编辑模板 #{editingId}：先在上方回测面板修改参数，再点击“更新”保存。
          </div>
        )}
      </div>

      {error && <div className='text-xs text-red-600'>{error}</div>}

      {templates.length === 0
        ? <div className='text-xs text-slate-400'>暂无回测策略模板</div>
        : <div className='space-y-2'>
            {templates.map(t => <div key={t.id} className='bg-white border border-slate-200 rounded-lg p-2 space-y-2'>
              <div className='flex items-center gap-2'>
                <div className='flex-1 min-w-0'>
                  <div className='text-xs font-semibold text-slate-700'>{t.name}</div>
                  <div className='text-[10px] text-slate-400 truncate'>
                    {t.description || t.config?.strategy?.entry?.condition || '未命名策略'}
                  </div>
                </div>
                <button
                  type='button'
                  disabled={loading}
                  onClick={() => { selectTemplate(t.id); onLoad(t.config); }}
                  className='px-2.5 py-1 text-[11px] text-white bg-blue-600 rounded disabled:opacity-50'
                >
                  应用
                </button>
                <button
                  type='button'
                  disabled={loading}
                  onClick={() => edit(t)}
                  className='px-2.5 py-1 text-[11px] text-slate-600 border border-slate-200 rounded disabled:opacity-50'
                >
                  编辑
                </button>
                <button
                  type='button'
                  disabled={loading}
                  onClick={() => copy(t)}
                  className='px-2.5 py-1 text-[11px] text-slate-600 border border-slate-200 rounded disabled:opacity-50'
                >
                  复制
                </button>
                <button
                  type='button'
                  disabled={loading}
                  onClick={() => remove(t.id)}
                  className='px-2.5 py-1 text-[11px] text-red-500 border border-red-200 rounded disabled:opacity-50'
                >
                  删除
                </button>
              </div>
            </div>)}
          </div>}

      <div className='pt-2 border-t border-slate-200 space-y-2'>
        <div className='text-[11px] font-semibold text-slate-500'>内置策略模板</div>
        <div className='space-y-2'>
          {BUILT_IN_TEMPLATES.map(t => (
            <div key={t.id} className='flex items-center gap-2 bg-white border border-slate-200 rounded-lg p-2'>
              <div className='flex-1 min-w-0'>
                <div className='text-xs font-semibold text-slate-700'>{t.name}</div>
                <div className='text-[10px] text-slate-400 truncate'>{t.description}</div>
              </div>
              <button
                type='button'
                disabled={loading}
                onClick={() => { selectTemplate(null); onLoad(t.config); }}
                className='px-2.5 py-1 text-[11px] text-blue-600 border border-blue-200 rounded disabled:opacity-50'
              >
                使用
              </button>
            </div>
          ))}
        </div>
        <div className='text-[10px] text-slate-400'>内置模板只读，不写入数据库；使用后可通过上方“保存”另存为你的模板。</div>
      </div>
    </div>}
  </div>;
}

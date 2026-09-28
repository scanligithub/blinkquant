'use client';
import { useCallback, useEffect, useState } from 'react';

export interface BacktestTemplateConfig {
  strategy: any;
  fee_policy: any;
  benchmark: any;
  min_listing_days: number;
  exclude_st: boolean;
}
interface Template { id:number; name:string; description?:string|null; config:BacktestTemplateConfig; updated_at:string; }
// P4.1: templates are persisted by Node1 Scheduler SQLite; Vercel only proxies authenticated requests.
interface Props { currentConfig: BacktestTemplateConfig; onLoad:(config:BacktestTemplateConfig)=>void; }

export default function BacktestStrategyTemplates({ currentConfig, onLoad }: Props) {
  const [open,setOpen]=useState(false);
  const [templates,setTemplates]=useState<Template[]>([]);
  const [name,setName]=useState('');
  const [loading,setLoading]=useState(false);
  const [error,setError]=useState('');

  const refresh=useCallback(async()=>{
    const res=await fetch('/api/backtest-strategy-templates',{cache:'no-store'});
    const json=await res.json();
    if(!res.ok) throw new Error(json.error||'加载模板失败');
    setTemplates(json.templates||[]);
  },[]);

  useEffect(()=>{ if(open) refresh().catch(e=>setError(e.message)); },[open,refresh]);

  const save=async()=>{
    if(!name.trim()) return;
    setLoading(true); setError('');
    try {
      const res=await fetch('/api/backtest-strategy-templates',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name.trim(),config:currentConfig})});
      const json=await res.json();
      if(!res.ok) throw new Error(json.error||'保存失败');
      setName(''); await refresh();
    } catch(e:any){ setError(e.message); } finally { setLoading(false); }
  };

  const remove=async(id:number)=>{
    if(!confirm('确定删除该回测策略模板？')) return;
    const res=await fetch(`/api/backtest-strategy-templates?id=${id}`,{method:'DELETE'});
    if(res.ok) setTemplates(v=>v.filter(x=>x.id!==id));
  };

  return <div className='space-y-2'>
    <button type='button' onClick={()=>setOpen(v=>!v)} className='w-full px-3 py-2 text-xs font-semibold border border-slate-200 rounded-lg hover:bg-slate-50 text-slate-600'>
      {open?'收起':'策略模板'}
    </button>
    {open && <div className='rounded-lg border border-slate-200 bg-slate-50 p-3 space-y-3'>
      <div className='flex gap-2'>
        <input value={name} onChange={e=>setName(e.target.value)} placeholder='模板名称，例如：MA20 周线趋势' className='flex-1 px-3 py-2 text-xs border border-slate-200 rounded-lg bg-white' />
        <button type='button' disabled={loading||!name.trim()} onClick={save} className='px-3 py-2 text-xs font-semibold text-white bg-blue-600 rounded-lg disabled:opacity-50'>保存</button>
      </div>
      {error && <div className='text-xs text-red-600'>{error}</div>}
      {templates.length===0 ? <div className='text-xs text-slate-400'>暂无回测策略模板</div> :
        <div className='space-y-2'>{templates.map(t=><div key={t.id} className='flex items-center gap-2 bg-white border border-slate-200 rounded-lg p-2'>
          <div className='flex-1 min-w-0'><div className='text-xs font-semibold text-slate-700'>{t.name}</div><div className='text-[10px] text-slate-400 truncate'>{t.config?.strategy?.entry?.condition || '未命名策略'}</div></div>
          <button type='button' onClick={()=>{onLoad(t.config);setOpen(false)}} className='px-2.5 py-1 text-[11px] text-white bg-blue-600 rounded'>应用</button>
          <button type='button' onClick={()=>remove(t.id)} className='px-2.5 py-1 text-[11px] text-red-500 border border-red-200 rounded'>删除</button>
        </div>)}</div>}
    </div>}
  </div>;
}

// P4.1 CI trigger: keep template UI changes independently build-validated.

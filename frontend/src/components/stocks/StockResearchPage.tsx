'use client';

import { useCallback, useEffect, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import dynamic from 'next/dynamic';
import AppShell from '../app/AppShell';
import StockSearch from '../StockSearch';
import useStockResearch from '@/hooks/useStockResearch';

const KLineChart = dynamic(() => import('../KLineChart'), {
  ssr: false,
  loading: () => <div className="h-[500px] flex items-center justify-center bg-slate-100 rounded-xl animate-pulse text-slate-400">加载图表引擎...</div>,
});

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

const ADJUST_OPTIONS = [
  { label: '不复权', value: 'none' as const },
  { label: '前复权', value: 'qfq' as const },
  { label: '后复权', value: 'hfq' as const },
];

const SECTOR_GROUPS = [
  { type: '行业板块', label: '行业' },
  { type: '概念板块', label: '概念' },
  { type: '地域板块', label: '地域' },
];

export default function StockResearchPage() {
  const params = useParams<{ symbol?: string | string[] }>();
  const router = useRouter();
  const symbol = Array.isArray(params?.symbol) ? params.symbol[0] : params?.symbol;

  const [user, setUser] = useState<{ id: string; email: string; role: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [watchlistCodes, setWatchlistCodes] = useState<string[]>([]);
  const [adjustMenuOpen, setAdjustMenuOpen] = useState(false);

  const research = useStockResearch();

  useEffect(() => {
    let mounted = true;
    const loadSession = async () => {
      try {
        const res = await fetch('/api/auth/session', { cache: 'no-store' });
        const json = await res.json();
        if (!mounted) return;
        if (json.user) {
          setUser(json.user);
          setAuthLoading(false);
        } else {
          router.replace('/login');
        }
      } catch {
        if (mounted) router.replace('/login');
      }
    };
    void loadSession();
    return () => { mounted = false; };
  }, [router]);

  const refreshWatchlist = useCallback(async () => {
    try {
      const res = await fetch('/api/watchlist', { cache: 'no-store' });
      if (res.ok) {
        const json = await res.json();
        setWatchlistCodes(json.codes || []);
      }
    } catch (e) {
      console.error('Failed to load watchlist', e);
    }
  }, []);

  useEffect(() => {
    if (user) void refreshWatchlist();
  }, [user, refreshWatchlist]);

  useEffect(() => {
    if (symbol && research.stockList.length > 0) {
      void research.viewStock(decodeURIComponent(symbol));
    }
  }, [symbol, research.stockList.length, research.viewStock]);

  useEffect(() => {
    if (!adjustMenuOpen) return;
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      const el = document.getElementById('stock-research-adjust-menu');
      if (el && !el.contains(target)) setAdjustMenuOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [adjustMenuOpen]);

  const toggleWatchlist = useCallback(async (code: string) => {
    const exists = watchlistCodes.includes(code);
    try {
      const res = exists
        ? await fetch(`/api/watchlist?code=${encodeURIComponent(code)}`, { method: 'DELETE' })
        : await fetch('/api/watchlist', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code }),
          });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setWatchlistCodes((prev) => exists ? prev.filter((c) => c !== code) : [...prev, code]);
    } catch (e) {
      console.error('Failed to toggle watchlist', e);
    }
  }, [watchlistCodes]);

  const handleLogout = useCallback(async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
    } catch (e) {
      console.error('Logout failed', e);
    }
    router.replace('/login');
  }, [router]);

  if (authLoading) {
    return <main className="min-h-screen flex items-center justify-center bg-slate-50"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></main>;
  }

  const selected = research.selectedStock;
  const adjustLabel = ADJUST_OPTIONS.find((item) => item.value === research.adjustMode)?.label || '不复权';

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="min-h-screen p-4 md:p-8 bg-slate-50 text-slate-900">
        <div className="max-w-7xl mx-auto space-y-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => router.push('/select')}
                  className="text-sm font-semibold text-slate-500 hover:text-blue-600"
                >
                  ← 选股
                </button>
                <span className="text-slate-300">/</span>
                <h1 className="text-2xl font-black">股票研究</h1>
              </div>
              <p className="text-sm text-slate-500 mt-1">以单只股票为中心查看行情、技术指标和板块信息</p>
            </div>
            <StockSearch
              stockList={research.stockList}
              onSelect={(code) => {
                router.push(`/stocks/${encodeURIComponent(code)}`);
                void research.viewStock(code);
              }}
            />
          </div>

          <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100 flex flex-wrap justify-between items-center gap-3">
              <div>
                {selected ? (
                  <div className="flex items-baseline gap-2">
                    <span className="text-xl font-bold">{selected.code}</span>
                    <span className="text-base text-slate-500">{selected.name}</span>
                  </div>
                ) : (
                  <span className="text-sm text-slate-400">请选择股票开始研究</span>
                )}
              </div>
              {selected?.kind === 'stock' && (
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    onClick={() => void toggleWatchlist(selected.code)}
                    className="px-3 py-1.5 text-xs font-bold text-amber-600 border border-amber-200 bg-amber-50 rounded-lg hover:bg-amber-100"
                  >
                    {watchlistCodes.includes(selected.code) ? '★ 已自选' : '☆ 加自选'}
                  </button>
                  <div id="stock-research-adjust-menu" className="relative">
                    <button
                      onClick={() => setAdjustMenuOpen((v) => !v)}
                      className="px-3 py-1.5 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-lg hover:bg-slate-50"
                    >
                      {adjustLabel} ▼
                    </button>
                    {adjustMenuOpen && (
                      <div className="absolute right-0 top-full mt-1 z-20 min-w-[120px] bg-white rounded-lg shadow-lg border border-slate-200 py-1">
                        {ADJUST_OPTIONS.map((option) => (
                          <button
                            key={option.value}
                            onClick={() => { research.changeAdjustMode(option.value); setAdjustMenuOpen(false); }}
                            className={`w-full text-left px-3 py-1.5 text-xs ${research.adjustMode === option.value ? 'bg-blue-50 text-blue-600' : 'text-slate-600 hover:bg-slate-100'}`}
                          >
                            {option.label}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center bg-slate-50 rounded-lg p-1 border border-slate-200">
                    {TIMEFRAMES.map((tf) => (
                      <button
                        key={tf.value}
                        onClick={() => research.changeChartTimeframe(tf.value)}
                        className={`px-3 py-1 text-xs font-bold rounded-md ${research.chartTimeframe === tf.value ? 'bg-blue-600 text-white shadow' : 'text-slate-500 hover:bg-slate-200/50'}`}
                      >
                        {tf.label}
                      </button>
                    ))}
                  </div>
                  <button
                    onClick={() => void research.toggleFullscreen()}
                    className="px-3 py-1.5 text-xs font-bold text-slate-600 border border-slate-200 bg-white rounded-lg hover:bg-slate-50"
                  >
                    {research.isFullScreen ? '退出全屏' : '全屏'}
                  </button>
                </div>
              )}
            </div>

            {selected?.kind === 'stock' && research.sectors.length > 0 && (
              <div className="px-4 py-3 border-b border-slate-100 space-y-1">
                {SECTOR_GROUPS.map((group) => {
                  const items = research.sectors.filter((item) => item.type === group.type);
                  if (!items.length) return null;
                  const expanded = !!research.expandedSectors[group.type];
                  const shown = expanded ? items : items.slice(0, 5);
                  const hidden = items.length - shown.length;
                  return (
                    <div key={group.type} className="flex flex-wrap items-center gap-1.5">
                      <span className="text-[10px] font-bold text-slate-400 w-8">{group.label}</span>
                      {shown.map((item) => (
                        <button key={item.code} onClick={() => void research.viewSector(item.code, item.name)} className="text-xs px-2 py-1 rounded border border-slate-200 text-slate-600 hover:bg-slate-50">
                          {item.name}
                        </button>
                      ))}
                      {hidden > 0 && (
                        <button onClick={() => research.setExpandedSectors((prev) => ({ ...prev, [group.type]: true }))} className="text-xs px-2 py-1 rounded border border-dashed border-slate-300 text-slate-400">+{hidden}</button>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            <div ref={research.chartWrapperRef} className="relative h-[620px] p-1">
              {research.chartLoading && <div className="absolute inset-0 z-10 bg-white/60 backdrop-blur-sm flex items-center justify-center"><div className="w-8 h-8 border-4 border-blue-500/20 border-t-blue-600 rounded-full animate-spin" /></div>}
              {selected ? (
                <KLineChart
                  code={selected.code}
                  data={selected.data}
                  subChartType={research.subChartType}
                  onSubChartTypeChange={research.setSubChartType}
                  mainChartType={research.mainChartType}
                  onMainChartTypeChange={research.setMainChartType}
                />
              ) : (
                <div className="h-full flex items-center justify-center text-slate-400 bg-slate-50 rounded-xl">从上方搜索框选择股票</div>
              )}
            </div>
          </section>
        </div>
      </main>
    </AppShell>
  );
}

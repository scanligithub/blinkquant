'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import AppShell from '@/components/app/AppShell';
import BacktestPanel from '@/components/BacktestPanel';
import BacktestResults from '@/components/BacktestResults';
import useBacktest from '@/hooks/useBacktest';
import { useCluster } from '@/hooks/useCluster';

interface User {
  id: string;
  email: string;
  role: string;
}

export default function BacktestsPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [strategyTemplateId, setStrategyTemplateId] = useState<number | null>(null);
  const { submitTask } = useCluster();
  const {
    backtestResult,
    backtestLoading,
    handleBacktest,
    clearBacktestState,
  } = useBacktest({ submitTask, strategyTemplateId });

  useEffect(() => {
    let mounted = true;
    (async () => {
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
    })();
    return () => { mounted = false; };
  }, [router]);

  const handleLogout = async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST' });
    } finally {
      clearBacktestState();
      router.replace('/login');
    }
  };

  if (authLoading || !user) {
    return <div className="min-h-screen flex items-center justify-center text-sm text-slate-500">正在加载…</div>;
  }

  const result = backtestResult?.legacy ?? null;
  const taskId = backtestResult?.taskId ?? null;
  const summary = backtestResult?.summary ?? null;

  return (
    <AppShell user={user} onLogout={handleLogout}>
      <main className="mx-auto max-w-7xl px-4 py-6 md:px-6 space-y-6">
        <header className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wider text-blue-600">Research</p>
            <h1 className="mt-1 text-2xl font-bold text-slate-900">回测研究</h1>
            <p className="mt-2 text-sm text-slate-600">
              配置策略、股票池与回测参数，提交任务后在这里查看结果；完整执行过程见任务中心。
            </p>
          </div>
          <Link href="/tasks" className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50">
            查看任务中心 →
          </Link>
        </header>

        <section className="rounded-2xl border border-slate-200 bg-white p-4 md:p-6">
          <div className="mb-4">
            <h2 className="text-base font-semibold text-slate-900">新建回测</h2>
            <p className="mt-1 text-xs text-slate-500">回测仍通过统一任务 API 提交，由 Node1 Scheduler 调度；此页面不直接调用计算节点。</p>
          </div>
          <BacktestPanel
            loading={backtestLoading}
            onRun={handleBacktest}
            onTemplateSelected={setStrategyTemplateId}
          />
        </section>

        {(backtestResult || backtestLoading) && (
          <section className="rounded-2xl border border-slate-200 bg-white p-4 md:p-6">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 className="text-base font-semibold text-slate-900">本次回测结果</h2>
                {taskId && <p className="mt-1 text-xs text-slate-500">任务 #{taskId}</p>}
              </div>
              {taskId && (
                <Link href={`/tasks/${taskId}`} className="text-sm font-medium text-blue-700 hover:underline">
                  查看任务详情 →
                </Link>
              )}
            </div>
            {backtestLoading && !backtestResult ? (
              <div className="rounded-lg bg-slate-50 p-6 text-sm text-slate-600">回测任务正在执行。你可以前往任务中心查看进度。</div>
            ) : (
              <BacktestResults result={result} taskId={taskId} summary={summary} />
            )}
          </section>
        )}
      </main>
    </AppShell>
  );
}

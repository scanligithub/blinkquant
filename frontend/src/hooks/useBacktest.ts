'use client';

import { useCallback, useEffect, useState } from 'react';
import type { BacktestParams } from '@/components/BacktestPanel';

type SubmitBacktestTask = (
  taskType: 'backtest',
  payload: BacktestParams,
  options?: { strategyTemplateId?: number | null }
) => Promise<number>;

interface UseBacktestOptions {
  submitTask: SubmitBacktestTask;
  strategyTemplateId: number | null;
}

export default function useBacktest({
  submitTask,
  strategyTemplateId,
}: UseBacktestOptions) {
  const [backtestResult, setBacktestResult] = useState<any>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);

  const clearBacktestState = useCallback(() => {
    localStorage.removeItem('backtestJobId');
    localStorage.removeItem('backtestNode');
    localStorage.removeItem('backtestTime');
  }, []);

  const pollTaskStatus = useCallback(async (taskId: number) => {
    while (true) {
      await new Promise((resolve) => setTimeout(resolve, 3000));
      try {
        const res = await fetch('/api/v1/tasks/' + taskId, { cache: 'no-store' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const task = await res.json();

        if (task.status === 'done') {
          if (task.result) {
            setBacktestResult({ legacy: task.result });
          } else {
            setBacktestResult({
              taskId: task.id,
              summary: task.result_summary,
            });
          }
          return;
        }
        if (task.status === 'failed') {
          alert('回测失败: ' + (task.error || '未知错误'));
          return;
        }
        if (task.status === 'cancelled') {
          alert('回测任务已取消');
          return;
        }
        if (task.status === 'preempted') {
          alert('回测被选股任务抢占，已自动重新排队');
          return;
        }
      } catch (e) {
        console.error('Poll error:', e);
      }
    }
  }, []);

  useEffect(() => {
    const savedJobId = localStorage.getItem('backtestJobId');
    const savedNode = localStorage.getItem('backtestNode');
    const savedTime = localStorage.getItem('backtestTime');

    if (savedTime && Date.now() - parseInt(savedTime, 10) > 3600000) {
      clearBacktestState();
      return;
    }

    if (savedJobId && savedNode) {
      const pollSavedJob = async () => {
        setBacktestLoading(true);
        let retries = 0;
        const maxRetries = 20;
        try {
          while (retries < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, 3000));
            retries++;
            try {
              const pollRes = await fetch(
                savedNode + '/api/v1/backtest/async/' + savedJobId,
                { signal: AbortSignal.timeout(60000) }
              );
              if (!pollRes.ok) {
                const text = await pollRes.text();
                throw new Error('HTTP ' + pollRes.status + ': ' + text.slice(0, 200));
              }
              const result = await pollRes.json();
              if (result.status === 'done') {
                setBacktestResult(result.data);
                return;
              }
              if (result.status === 'failed' || result.status === 'cancelled' || result.status === 'expired') {
                return;
              }
            } catch (e) {
              console.error('Poll error (retry ' + retries + '):', e);
            }
          }
          alert('恢复回测超时，请重新运行');
        } catch (e) {
          console.error('Failed to poll saved job:', e);
        } finally {
          clearBacktestState();
          setBacktestLoading(false);
        }
      };
      void pollSavedJob();
    }
  }, [clearBacktestState]);

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail;
      if (detail?.taskId) {
        setBacktestResult({ taskId: detail.taskId, summary: detail.summary });
      }
    };
    window.addEventListener('openBacktestResult', handler);
    return () => window.removeEventListener('openBacktestResult', handler);
  }, []);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (backtestLoading) {
      timer = setTimeout(() => {
        console.warn('backtestLoading stuck >2min, force reset');
        setBacktestLoading(false);
        clearBacktestState();
      }, 120000);
    }
    return () => {
      if (timer) clearTimeout(timer);
    };
  }, [backtestLoading, clearBacktestState]);

  const handleBacktest = useCallback(async (params: BacktestParams) => {
    setBacktestLoading(true);
    setBacktestResult(null);
    try {
      const taskId = await submitTask('backtest', params, {
        strategyTemplateId,
      });
      await pollTaskStatus(taskId);
    } catch (e: any) {
      alert('回测失败: ' + e.message);
    } finally {
      setBacktestLoading(false);
    }
  }, [pollTaskStatus, strategyTemplateId, submitTask]);

  return {
    backtestResult,
    backtestLoading,
    handleBacktest,
    setBacktestResult,
    clearBacktestState,
  };
}

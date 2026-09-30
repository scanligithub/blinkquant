'use client';

import { useCallback, useState } from 'react';

type SubmitSelectionTask = (
  taskType: 'selection',
  payload: any
) => Promise<number>;

interface UseSelectionOptions {
  submitTask: SubmitSelectionTask;
  onClearSelectedStock?: () => void;
}

export default function useSelection({
  submitTask,
  onClearSelectedStock,
}: UseSelectionOptions) {
  const [formula, setFormula] = useState('CLOSE > MA(CLOSE, 20)');
  const [selectDate, setSelectDate] = useState('');
  const [timeframe, setTimeframe] = useState('D');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectMeta, setSelectMeta] = useState<{ date?: string | null; degraded?: boolean } | null>(null);

  const clearResults = useCallback(() => {
    setResults([]);
    setSelectMeta(null);
    onClearSelectedStock?.();
  }, [onClearSelectedStock]);

  const handleSelect = useCallback(
    async (overrides?: { formula?: string; timeframe?: string; date?: string }) => {
      setLoading(true);
      clearResults();

      const f = overrides?.formula ?? formula;
      const t = overrides?.timeframe ?? timeframe;
      const d = overrides?.date;

      try {
        const taskId = await submitTask(
          'selection',
          { formula: f, timeframe: t, ...(d ? { date: d } : {}) }
        );

        while (true) {
          await new Promise((resolve) => setTimeout(resolve, 2000));

          try {
            const res = await fetch('/api/v1/tasks/' + taskId, { cache: 'no-store' });
            if (!res.ok) throw new Error('HTTP ' + res.status);

            const task = await res.json();

            if (task.status === 'done') {
              let codes: string[] | null = null;
              let signalDate: string | null = null;
              let degraded = false;

              if (Array.isArray(task.result?.codes)) {
                codes = task.result.codes;
                signalDate = task.result.nodes?.node1?.signal_date ?? null;
              } else if (task.result?.success && Array.isArray(task.result?.data)) {
                codes = task.result.data;
                signalDate = task.result.date ?? null;
                degraded = !!task.result.meta?.degraded;
              }

              if (codes) {
                setResults(codes);
                setSelectMeta({ date: signalDate, degraded });
              } else {
                alert('Selection failed: ' + (task.result?.error || '未知错误'));
              }
              return;
            }

            if (task.status === 'failed') {
              alert('Selection failed: ' + (task.error || '未知错误'));
              return;
            }

            if (task.status === 'cancelled') {
              alert('选股任务已取消');
              return;
            }
          } catch (e: any) {
            console.error('Selection poll error:', e);
          }
        }
      } catch {
        alert('Gateway connection failed');
      } finally {
        setLoading(false);
      }
    },
    [clearResults, formula, submitTask, timeframe]
  );

  return {
    formula,
    setFormula,
    selectDate,
    setSelectDate,
    timeframe,
    setTimeframe,
    results,
    loading,
    selectMeta,
    handleSelect,
    clearResults,
  };
}

'use client';

interface SelectionControlsProps {
  formula: string;
  selectDate: string;
  loading: boolean;
  onFormulaChange: (value: string) => void;
  onDateChange: (value: string) => void;
  onRun: () => void;
  onAISelect: () => void;
  onSaveStrategy: () => void;
}

export default function SelectionControls({
  formula,
  selectDate,
  loading,
  onFormulaChange,
  onDateChange,
  onRun,
  onAISelect,
  onSaveStrategy,
}: SelectionControlsProps) {
  return (
    <section
      aria-label="选股控制"
      className="bg-white p-4 md:p-6 rounded-2xl border border-slate-200 shadow-sm"
    >
      <div className="flex flex-col gap-3">
        <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">
          策略公式
        </label>

        <div className="flex flex-col sm:flex-row gap-3">
          <input
            className="flex-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
            placeholder="例如：CLOSE > MA(CLOSE, 20)"
            value={formula}
            onChange={(e) => onFormulaChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') onRun();
            }}
          />
          <input
            type="date"
            title="选股日期（留空 = 最新交易日）"
            className="bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm text-slate-600 focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 outline-none"
            value={selectDate}
            onChange={(e) => onDateChange(e.target.value)}
          />
          <button
            type="button"
            onClick={onRun}
            disabled={loading}
            className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 rounded-xl font-bold flex items-center justify-center gap-2 min-w-[160px]"
          >
            {loading ? (
              <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            ) : (
              '运行选股'
            )}
          </button>
          <button
            type="button"
            onClick={onAISelect}
            disabled={loading}
            className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-xl font-bold"
          >
            AI 选股
          </button>
          <button
            type="button"
            onClick={onSaveStrategy}
            disabled={loading}
            className="bg-white border border-slate-200 hover:bg-slate-50 text-slate-600 px-4 py-2 rounded-xl font-bold"
          >
            保存为策略
          </button>
        </div>
      </div>
    </section>
  );
}

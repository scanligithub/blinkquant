'use client';

import BacktestResults, { type Summary } from '@/components/BacktestResults';

export default function ArtifactDataViewer({
  artifactId,
  resultUri,
  summary,
}: {
  artifactId: number;
  resultUri?: string | null;
  summary?: Summary | null;
}) {
  return (
    <section className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
      <div className="mb-4">
        <h2 className="font-bold text-slate-800">Artifact 数据</h2>
        <p className="mt-1 text-xs text-slate-400">
          与“我的任务 → 查看结果”使用相同的权益曲线、明细表格和翻页交互；成交与持仓每页最多加载 100 条。
        </p>
      </div>
      {resultUri && (
        <div className="mb-3 text-xs font-mono text-slate-400 break-all">引用：{resultUri}</div>
      )}
      <BacktestResults artifactId={artifactId} summary={summary} showSummaryMetrics={false} />
    </section>
  );
}

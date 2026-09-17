"use client";

import { useEffect, useRef, useState } from "react";
import { useCluster, Task } from "@/hooks/useCluster";

const STATUS_COLORS: Record<string, string> = {
  pending: "bg-gray-100 text-gray-700",
  queued: "bg-blue-100 text-blue-700",
  running: "bg-green-100 text-green-700",
  done: "bg-emerald-100 text-emerald-700",
  failed: "bg-red-100 text-red-700",
  cancelled: "bg-gray-100 text-gray-500",
  preempted: "bg-orange-100 text-orange-700",
};

const STATUS_LABELS: Record<string, string> = {
  pending: "等待中",
  queued: "排队中",
  running: "运行中",
  done: "完成",
  failed: "失败",
  cancelled: "已取消",
  preempted: "被抢占",
};

const TASK_TYPE_LABELS: Record<string, string> = {
  selection: "选股",
  backtest: "回测",
};

function DownloadMenu({ taskId }: { taskId: number }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        className="text-xs text-gray-500 hover:text-gray-700"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        下载
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 bg-white border rounded-lg shadow-lg z-20 py-1 min-w-[140px]">
          {(['equity_curve', 'trades', 'positions_daily'] as const).map((name) => (
            <button
              key={name}
              type="button"
              className="block w-full text-left px-3 py-1.5 text-xs hover:bg-gray-50"
              onClick={async () => {
                const { downloadArtifact } = await import('@/lib/backtestArtifacts');
                await downloadArtifact(taskId, name);
                setOpen(false);
              }}
            >
              {name === 'equity_curve'
                ? '权益曲线.parquet'
                : name === 'trades'
                  ? '成交.parquet'
                  : '持仓.parquet'}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export function TaskList() {
  const { myTasks, cancelTask } = useCluster();

  if (myTasks.length === 0) {
    return (
      <div className="text-center text-gray-400 text-sm py-8">
        暂无任务记录
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs font-medium text-gray-500 mb-2">
        <span>我的任务</span>
        <span className="text-gray-400">共 {myTasks.length} 个</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-white">
            <tr className="border-b border-gray-100">
              <th className="text-left py-2 px-2 font-medium text-gray-500 w-20">类型</th>
              <th className="text-left py-2 px-2 font-medium text-gray-500">状态</th>
              <th className="text-left py-2 px-2 font-medium text-gray-500">节点</th>
              <th className="text-left py-2 px-2 font-medium text-gray-500">创建时间</th>
              <th className="text-right py-2 px-2 font-medium text-gray-500 w-24">操作</th>
            </tr>
          </thead>
          <tbody>
            {myTasks.map((task) => (
              <tr key={task.id} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="py-2 px-2 font-mono text-gray-600">
                  {TASK_TYPE_LABELS[task.task_type] || task.task_type}
                </td>
                <td className="py-2 px-2">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                    STATUS_COLORS[task.status] || "bg-gray-100 text-gray-700"
                  }`}>
                    {STATUS_LABELS[task.status] || task.status}
                  </span>
                  {task.status === "preempted" && (
                    <span className="ml-1 text-[10px] text-orange-600">(已自动重排队)</span>
                  )}
                  {task.status === "running" && (
                    <div className="mt-1 w-full max-w-xs">
                      <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
                        <span>
                          {task.progress?.current_date
                            ? `算至 ${task.progress.current_date}`
                            : task.progress?.stage === "loading_data"
                              ? "加载数据…"
                              : "计算中…"}
                        </span>
                        <span>
                          {typeof task.progress_pct === "number"
                            ? `${task.progress_pct.toFixed(0)}%`
                            : "—"}
                        </span>
                      </div>
                      <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-blue-500 transition-all duration-500"
                          style={{
                            width: `${Math.min(100, Math.max(0, task.progress_pct ?? 0))}%`,
                          }}
                        />
                      </div>
                    </div>
                  )}
                  {task.task_type === 'backtest' && task.status === 'done' && task.result_summary && (
                    <span className="ml-2 text-xs text-muted-foreground">
                      {task.result_summary.total_return != null && (
                        <>收益 {(task.result_summary.total_return * 100).toFixed(2)}%</>
                      )}
                      {task.result_summary.max_drawdown != null && (
                        <> | 回撤 {(task.result_summary.max_drawdown * 100).toFixed(2)}%</>
                      )}
                      {task.result_summary.n_trades != null && (
                        <> | {task.result_summary.n_trades} 笔</>
                      )}
                    </span>
                  )}
                </td>
                <td className="py-2 px-2 text-gray-500 font-mono">
                  {task.assigned_node || "-"}
                </td>
                <td className="py-2 px-2 text-gray-500">
                  {task.created_at ? new Date(task.created_at).toLocaleString() : "-"}
                </td>
                <td className="py-2 px-2 text-right">
                  {task.status === "running" || task.status === "queued" || task.status === "pending" ? (
                    <button
                      onClick={() => cancelTask(task.id)}
                      className="text-xs text-red-600 hover:text-red-800 underline"
                    >
                      取消
                    </button>
                  ) : task.status === "done" && task.task_type === "backtest" && task.result_uri ? (
                    <div className="flex items-center gap-1 justify-end">
                      <button
                        onClick={() => {
                          window.dispatchEvent(new CustomEvent('openBacktestResult', { detail: { taskId: task.id, summary: task.result_summary } }));
                        }}
                        className="text-xs text-blue-600 hover:text-blue-800 underline"
                      >
                        查看结果
                      </button>
                      <span className="text-gray-300">|</span>
                      <DownloadMenu taskId={task.id} />
                    </div>
                  ) : (
                    <span className="text-gray-400">-</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

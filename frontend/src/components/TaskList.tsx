"use client";

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
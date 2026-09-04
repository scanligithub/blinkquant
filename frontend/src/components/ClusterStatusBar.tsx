"use client";

import { useCluster, NodeStatus } from "@/hooks/useCluster";

const STATUS_COLORS: Record<string, string> = {
  idle: "bg-green-100 text-green-700",
  running: "bg-blue-100 text-blue-700",
  draining: "bg-yellow-100 text-yellow-700",
  unhealthy: "bg-red-100 text-red-700",
  maintenance: "bg-gray-100 text-gray-500",
};

const STATUS_LABELS: Record<string, string> = {
  idle: "空闲",
  running: "运行中",
  draining: "让路中",
  unhealthy: "异常",
  maintenance: "维护",
};

const TASK_TYPE_LABELS: Record<string, string> = {
  selection: "选股",
  backtest: "回测",
};

export function ClusterStatusBar() {
  const { nodes, queueStats, canRunSelection, canRunBacktest, idleNodeCount } = useCluster();

  return (
    <div className="flex flex-wrap items-center gap-2 p-3 bg-gray-50 rounded-lg border">
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-xs font-medium text-gray-500">集群:</span>
        {nodes.map((node) => (
          <NodeBadge key={node.node_id} node={node} />
        ))}
      </div>

      <div className="flex items-center gap-2 ml-auto flex-wrap">
        <div className="text-xs text-gray-500 flex items-center gap-2">
          <span className={`px-2 py-0.5 rounded ${queueStats.pending_selection > 0 ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-400'}`}>
            选股队列: {queueStats.pending_selection}
          </span>
          <span className={`px-2 py-0.5 rounded ${queueStats.pending_backtest > 0 ? 'bg-purple-100 text-purple-700' : 'bg-gray-100 text-gray-400'}`}>
            回测队列: {queueStats.pending_backtest}
          </span>
          <span className={`px-2 py-0.5 rounded ${queueStats.running > 0 ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-400'}`}>
            运行中: {queueStats.running}
          </span>
        </div>

        <div className="flex gap-1">
          <button
            disabled={!canRunSelection}
            title={canRunSelection ? "" : `需要 3 个节点全部空闲 (当前 ${idleNodeCount}/3 空闲)`}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${
              canRunSelection
                ? "bg-blue-600 text-white hover:bg-blue-700"
                : "bg-gray-200 text-gray-400 cursor-not-allowed"
            }`}
          >
            运行选股
          </button>
          <button
            disabled={!canRunBacktest}
            title={canRunBacktest ? "" : `需要至少 1 个节点空闲 (当前 ${idleNodeCount}/3 空闲)`}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${
              canRunBacktest
                ? "bg-purple-600 text-white hover:bg-purple-700"
                : "bg-gray-200 text-gray-400 cursor-not-allowed"
            }`}
          >
            运行回测
          </button>
        </div>
      </div>
    </div>
  );
}

function NodeBadge({ node }: { node: NodeStatus }) {
  const colorClass = STATUS_COLORS[node.status] || "bg-gray-100 text-gray-700";
  const label = STATUS_LABELS[node.status] || node.status;

  return (
    <div className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-mono ${colorClass}`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current opacity-70" />
      <span>{node.name}</span>
      <span>{label}</span>
      {node.status === "running" && node.task_type && (
        <>
          <span className="opacity-60">·</span>
          <span>{TASK_TYPE_LABELS[node.task_type] || node.task_type}</span>
          {node.current_task_id && <span className="opacity-60">#{node.current_task_id}</span>}
        </>
      )}
    </div>
  );
}
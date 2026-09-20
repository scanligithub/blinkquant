"use client";

import { useCluster, NodeStatus, QueueItem } from "@/hooks/useCluster";

const STATUS_COLORS: Record<string, string> = {
  idle: "bg-green-100 text-green-700",
  running: "bg-blue-100 text-blue-700",
  draining: "bg-yellow-100 text-yellow-700",
  unhealthy: "bg-red-100 text-red-700",
  maintenance: "bg-gray-100 text-gray-500",
};

export function ClusterStatusBar() {
  const { nodes, queues, queueStats, canRunSelection, canRunBacktest, idleNodeCount } = useCluster();

  const slotQueue = {
    node1: queues?.selection,
    node2: queues?.backtest,
    node3: queues?.running,
  };

  return (
    <div className="space-y-2 p-3 bg-gray-50 rounded-lg border">
      <div className="flex flex-wrap gap-2">
        {nodes.map((node) => (
          <NodeCard
            key={node.node_id}
            node={node}
            queue={slotQueue[node.node_id as keyof typeof slotQueue]}
          />
        ))}
      </div>

      <div className="flex items-center gap-2 flex-wrap text-xs">
        <div className="flex items-center gap-2">
          <span className={`px-2 py-0.5 rounded ${queueStats.pending_selection > 0 ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-400'}`}>
            选股: {queueStats.pending_selection}
          </span>
          <span className={`px-2 py-0.5 rounded ${queueStats.pending_backtest > 0 ? 'bg-purple-100 text-purple-700' : 'bg-gray-100 text-gray-400'}`}>
            回测: {queueStats.pending_backtest}
          </span>
          <span className={`px-2 py-0.5 rounded ${queueStats.running > 0 ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-400'}`}>
            运行中: {queueStats.running}
          </span>
        </div>

        <div className="flex gap-1 ml-auto">
          <button
            disabled={!canRunSelection}
            title={canRunSelection ? "" : `需要 3 个节点全部空闲 (当前 ${idleNodeCount}/3)`}
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
            title={canRunBacktest ? "" : `需要至少 1 个节点空闲 (当前 ${idleNodeCount}/3)`}
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

function NodeCard({ node, queue }: { node: NodeStatus; queue?: { title: string; count: number; items: QueueItem[] } }) {
  const colorClass = STATUS_COLORS[node.status] || "bg-gray-100 text-gray-700";

  return (
    <div className={`flex-1 min-w-[200px] rounded-lg border p-2.5 text-xs ${colorClass}`}>
      <div className="flex items-center gap-1.5 font-medium mb-1">
        <span className="w-1.5 h-1.5 rounded-full bg-current opacity-70" />
        <span>{node.name}</span>
        <span className="opacity-70">·</span>
        <span>{node.display_label || node.status}</span>
      </div>

      {queue && queue.count > 0 && (
        <div className="mt-1.5 space-y-0.5">
          <div className="font-medium opacity-70">{queue.title} {queue.count}</div>
          {queue.items.map((it) => (
            <div key={it.id} className="pl-2 opacity-80 truncate">
              {itemLine(it)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function itemLine(it: QueueItem): string {
  if (it.status === "running") {
    const pct = it.progress_pct != null ? ` ${Math.round(it.progress_pct)}%` : "";
    const where = it.assigned_node ? `@${it.assigned_node}` : "";
    return `#${it.id} ${it.task_type_zh}${where}${pct}`;
  }
  const extra = it.date_range || it.formula_preview || "";
  return `#${it.id} ${extra}`.trim();
}
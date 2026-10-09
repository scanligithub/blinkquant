"use client";

import { useState, useEffect, useCallback } from "react";

export interface NodeStatus {
  node_id: string;
  name: string;
  status: "idle" | "running" | "draining" | "unhealthy" | "maintenance";
  status_zh: string;
  current_task_id: number | null;
  task_type: "selection" | "backtest" | null;
  task_type_zh: string;
  display_label: string;
  heartbeat_at: string | null;
  last_error: string | null;
  generation: number | null;
}

export interface QueueItem {
  id: number;
  task_type: string;
  task_type_zh: string;
  status: string;
  user_id: string | null;
  assigned_node: string | null;
  priority: number | null;
  progress_pct: number | null;
  formula_preview: string | null;
  date_range: string | null;
  created_at: string | null;
  queued_at: string | null;
  started_at: string | null;
}

export interface QueueSection {
  title: string;
  count: number;
  items: QueueItem[];
}

export interface QueueStats {
  pending_selection: number;
  pending_backtest: number;
  running: number;
}

export interface ClusterState {
  nodes: NodeStatus[];
  queueStats: QueueStats;
  queues?: {
    selection: QueueSection;
    backtest: QueueSection;
    running: QueueSection;
  };
}

export interface Task {
  id: number;
  task_type: "selection" | "backtest";
  status: "pending" | "queued" | "running" | "done" | "failed" | "cancelled" | "preempted";
  assigned_node: string | null;
  result: any;
  result_summary?: {
    total_return?: number;
    max_drawdown?: number;
    cagr?: number;
    sharpe?: number;
    sortino?: number;
    calmar?: number;
    drawdown_duration?: number;
    turnover?: number;
    total_fees?: number;
    buy_count?: number;
    sell_count?: number;
    n_trades?: number;
    n_positions?: number;
    final_equity?: number;
    initial_cash?: number;
    total_days?: number;
  } | null;
  result_uri?: string | null;
  strategy_template_id?: number | null;
  strategy_template_name?: string | null;
  strategy_template_updated_at?: string | null;
  strategy_template_version?: number | null;
  source_task_id?: number | null;
  artifact_id?: number | null;
  title?: string;
  updated_at?: string | null;
  task_exists?: boolean;
  task_id?: number | null;
  progress_pct?: number | null;
  progress?: {
    pct?: number;
    done_days?: number;
    total_days?: number;
    current_date?: string;
    stage?: string;
    updated_at?: string;
  } | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export function useCluster() {
  const [clusterState, setClusterState] = useState<ClusterState>({
    nodes: [],
    queueStats: { pending_selection: 0, pending_backtest: 0, running: 0 }
  });
  const [myTasks, setMyTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let mounted = true;
    const pollCluster = async () => {
      try {
        const res = await fetch('/api/v1/cluster/status', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          if (mounted) setClusterState(data);
        }
      } catch (e) {
        console.error('Cluster status poll error:', e);
      }
    };
    pollCluster();
    const interval = setInterval(pollCluster, 2000);
    return () => { mounted = false; clearInterval(interval); };
  }, []);

  useEffect(() => {
    let mounted = true;
    const pollTasks = async () => {
      try {
        const res = await fetch('/api/v1/tasks', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          const list = Array.isArray(data) ? data : (Array.isArray(data?.tasks) ? data.tasks : []);
          if (mounted) setMyTasks(list);
        }
      } catch (e) {
        console.error('Tasks poll error:', e);
      }
    };
    pollTasks();
    const interval = setInterval(pollTasks, 3000);
    return () => { mounted = false; clearInterval(interval); };
  }, []);

  const nodes = Array.isArray(clusterState.nodes) ? clusterState.nodes : [];
  const canRunSelection = nodes.every(n => n.status === "idle");
  const canRunBacktest = nodes.some(n => n.status === "idle");
  const idleNodeCount = nodes.filter(n => n.status === "idle").length;

  const submitTask = useCallback(async (
    taskType: "selection" | "backtest",
    payload: any,
    options?: { strategyTemplateId?: number | null },
  ) => {
    setLoading(true);
    try {
      const res = await fetch('/api/v1/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          task_type: taskType,
          payload,
          ...(options?.strategyTemplateId != null
            ? { strategy_template_id: options.strategyTemplateId }
            : {}),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data.task_id;
    } finally {
      setLoading(false);
    }
  }, []);

  const cancelTask = useCallback(async (taskId: number) => {
    try {
      const res = await fetch(`/api/v1/tasks/${taskId}`, { method: 'POST' });
      if (!res.ok) throw new Error('Cancel failed');
    } catch (e) {
      alert('取消失败: ' + e);
    }
  }, []);

  const deleteTask = useCallback(async (taskId: number) => {
    try {
      const res = await fetch(`/api/v1/tasks/${taskId}`, { method: 'DELETE' });
      if (!res.ok) throw new Error('Delete failed');
    } catch (e) {
      alert('删除失败: ' + e);
    }
  }, []);

  const rerunTask = useCallback(async (taskId: number) => {
    const res = await fetch(`/api/v1/tasks/${taskId}/rerun`, { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    return data.task_id as number;
  }, []);

  return {
    nodes: clusterState.nodes,
    queueStats: clusterState.queueStats,
    queues: clusterState.queues,
    myTasks,
    loading,
    canRunSelection,
    canRunBacktest,
    idleNodeCount,
    submitTask,
    cancelTask,
    deleteTask,
    rerunTask,
  };
}
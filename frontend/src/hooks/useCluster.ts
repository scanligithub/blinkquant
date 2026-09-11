"use client";

import { useState, useEffect, useCallback } from "react";

export interface NodeStatus {
  node_id: string;
  name: string;
  status: "idle" | "running" | "draining" | "unhealthy" | "maintenance";
  current_task_id: number | null;
  task_type: "selection" | "backtest" | null;
  load: number;
  heartbeat_at: string | null;
}

export interface QueueStats {
  pending_selection: number;
  pending_backtest: number;
  running: number;
}

export interface ClusterState {
  nodes: NodeStatus[];
  queueStats: QueueStats;
}

export interface Task {
  id: number;
  task_type: "selection" | "backtest";
  status: "pending" | "queued" | "running" | "done" | "failed" | "cancelled" | "preempted";
  assigned_node: string | null;
  result: any;
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

  // 轮询集群状态
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

  // 轮询我的任务
  useEffect(() => {
    let mounted = true;
    const pollTasks = async () => {
      try {
        const res = await fetch('/api/v1/tasks', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          // Node1 返回 { tasks: [...] }；兼容万一仍是数组
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

  const submitTask = useCallback(async (taskType: "selection" | "backtest", payload: any) => {
    setLoading(true);
    try {
      const res = await fetch('/api/v1/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_type: taskType, payload }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      return data.task_id;
    } finally {
      setLoading(false);
    }
  }, []);

  const cancelTask = useCallback(async (taskId: number) => {
    try {
      const res = await fetch(`/api/v1/tasks/${taskId}`, { method: 'DELETE' });
      if (!res.ok) throw new Error('Cancel failed');
    } catch (e) {
      alert('取消失败: ' + e);
    }
  }, []);

  return {
    nodes: clusterState.nodes,
    queueStats: clusterState.queueStats,
    myTasks,
    loading,
    canRunSelection,
    canRunBacktest,
    idleNodeCount,
    submitTask,
    cancelTask,
  };
}
'use client';

import { useEffect, useState } from 'react';
import { useCluster } from '@/hooks/useCluster';

function nodeIdFromHealthCard(node: any, idx: number): 'node1' | 'node2' | 'node3' {
  const n = node?.node;
  if (n === 0 || n === '0') return 'node1';
  if (n === 1 || n === '1') return 'node2';
  if (n === 2 || n === '2') return 'node3';
  return (['node1', 'node2', 'node3'] as const)[idx] ?? 'node1';
}

function queueForSlot(
  queues: ReturnType<typeof import('@/hooks/useCluster').useCluster>['queues'],
  slot: 'node1' | 'node2' | 'node3'
) {
  if (!queues) return null;
  if (slot === 'node1') return queues.selection;
  if (slot === 'node2') return queues.backtest;
  return queues.running;
}

function queueItemLine(it: {
  id: number;
  status: string;
  task_type_zh?: string;
  assigned_node?: string | null;
  progress_pct?: number | null;
  date_range?: string | null;
  formula_preview?: string | null;
}): string {
  if (it.status === 'running') {
    const pct = it.progress_pct != null ? ` ${Math.round(it.progress_pct)}%` : '';
    const where = it.assigned_node ? `@${it.assigned_node}` : '';
    return `#${it.id} ${it.task_type_zh || ''}${where}${pct}`;
  }
  const extra = it.date_range || it.formula_preview || '';
  return `#${it.id} ${extra}`.trim();
}

export default function NodeHealthCards() {
  const { nodes: clusterNodes, queues } = useCluster();
  const [clusterStatus, setClusterStatus] = useState<any>(null);

  useEffect(() => {
    let mounted = true;
    const fetchStatus = async () => {
      try {
        const res = await fetch('/api/status', { cache: 'no-store' });
        const json = await res.json();
        if (mounted) setClusterStatus(json);
      } catch (e) {
        console.error('Monitor failed', e);
      }
    };
    void fetchStatus();
    const timer = window.setInterval(fetchStatus, 5000);
    return () => {
      mounted = false;
      window.clearInterval(timer);
    };
  }, []);

  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap justify-center gap-3">
          {clusterStatus?.nodes?.map((node: any, idx: number) => {
            const slot = nodeIdFromHealthCard(node, idx);
            const role = (clusterNodes || []).find((n) => n.node_id === slot);
            const q = queueForSlot(queues, slot);

            return (
              <div
                key={idx}
                className={`text-xs md:text-sm font-mono px-3 py-2 rounded-lg border shadow-sm min-w-[200px] max-w-[280px] ${
                  node.online ? 'bg-white border-slate-200' : 'bg-red-50 border-red-200'
                }`}
              >
                <div className="flex items-center gap-2 flex-wrap">
                  <div className={`w-2.5 h-2.5 md:w-3 md:h-3 rounded-full ${node.online ? 'bg-green-500' : 'bg-red-500'}`} />
                  <span className="font-bold text-slate-700 text-sm md:text-base">Node {node.node ?? idx}</span>
                  <span className={`text-xs uppercase font-bold px-2 py-0.5 rounded-full ${
                    node.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
                  }`}>
                    {node.status || 'OFFLINE'}
                  </span>
                </div>

                <div className="mt-1.5 text-sm font-semibold text-slate-800">
                  {role?.display_label || (node.online ? '—' : '离线')}
                </div>

                {node.online ? (
                  <div className="mt-2 space-y-1">
                    <div className="flex justify-between gap-3">
                      <span className="text-slate-500 text-xs">进程内存</span>
                      <span className="font-mono font-medium text-slate-900">{node.process_memory_gb} GB</span>
                    </div>
                    <div className="flex justify-between gap-3">
                      <span className="text-slate-500 text-xs">系统空闲</span>
                      <span className="font-mono font-bold text-blue-600">{node.system_memory_free_gb ?? node.system_memory_available_gb} GB</span>
                    </div>
                    <div className="flex justify-between gap-3">
                      <span className="text-slate-500 text-xs">磁盘空闲</span>
                      <span className="font-mono text-slate-900">{node.disk_free_gb} GB</span>
                    </div>
                    <div className="flex justify-between gap-3">
                      <span className="text-slate-500 text-xs">数据行</span>
                      <span className="font-mono text-slate-500">{node.rows_daily?.toLocaleString()}</span>
                    </div>
                  </div>
                ) : null}

                {q && (
                  <div className="mt-2 pt-2 border-t border-slate-100">
                    <div className="font-medium text-slate-600 text-xs">
                      {q.title}{' '}
                      <span className={q.count > 0 ? 'text-slate-900' : 'text-slate-400'}>{q.count}</span>
                    </div>
                    {q.count > 0 && (
                      <div className="mt-1 space-y-0.5 max-h-24 overflow-y-auto">
                        {q.items.map((it) => (
                          <div key={it.id} className="pl-1 text-slate-600 truncate text-xs">
                            {queueItemLine(it)}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}

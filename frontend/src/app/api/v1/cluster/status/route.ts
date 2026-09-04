import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { sql } from '@/lib/db';

export const runtime = 'nodejs';

export async function GET(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  try {
    const [nodesResult, queueResult] = await Promise.all([
      sql`
        SELECT node_id, name, endpoint, weight, status,
               current_task_id, task_type, heartbeat_at, last_error
        FROM cluster_nodes
        ORDER BY node_id
      `,
      sql`
        SELECT 
          COUNT(*) FILTER (WHERE status = 'pending' AND task_type = 'selection') as pending_selection,
          COUNT(*) FILTER (WHERE status = 'pending' AND task_type = 'backtest') as pending_backtest,
          COUNT(*) FILTER (WHERE status = 'running') as running
        FROM task_queue
      `
    );
    
    const nodes = nodesResult.rows.map(row => ({
      node_id: row.node_id,
      name: row.name,
      status: row.status,
      current_task_id: row.current_task_id,
      task_type: row.task_type,
      load: 0.0, // TODO: 从心跳获取
      heartbeat_at: row.heartbeat_at,
    }));
    
    return NextResponse.json({
      nodes,
      queue_stats: queueResult.rows[0],
    });
  } catch (e) {
    console.error('Cluster status error:', e);
    return NextResponse.json({ error: 'Failed to get cluster status' }, { status: 500 });
  }
}
import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { sql } from '@/lib/db';

export const runtime = 'nodejs';

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const { id } = await params;
  const taskId = parseInt(id);
  if (isNaN(taskId)) {
    return NextResponse.json({ error: 'Invalid task ID' }, { status: 400 });
  }

  try {
    const result = await sql`
      SELECT id, user_id, task_type, payload, priority, status,
             assigned_node, cluster_job_id, result, error,
             created_at, queued_at, started_at, finished_at,
             retry_count, max_retries, preempted_by
      FROM task_queue
      WHERE id = ${taskId}
    `;
    
    if (result.rows.length === 0) {
      return NextResponse.json({ error: 'Task not found' }, { status: 404 });
    }
    
    return NextResponse.json(result.rows[0]);
  } catch (e) {
    console.error('Get task error:', e);
    return NextResponse.json({ error: 'Failed to get task' }, { status: 500 });
  }
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const { id } = await params;
  const taskId = parseInt(id);
  if (isNaN(taskId)) {
    return NextResponse.json({ error: 'Invalid task ID' }, { status: 400 });
  }

  try {
    // 只能取消自己的任务
    const auth = await requireAuth(req);
    const userId = auth.user?.id;
    
    const result = await sql`
      UPDATE task_queue
      SET status = 'cancelled', finished_at = now()
      WHERE id = ${taskId} AND user_id = ${userId}
      AND status IN ('pending', 'queued', 'running')
      RETURNING id
    `;
    
    if (result.rowCount === 0) {
      return NextResponse.json({ error: 'Task not found or cannot be cancelled' }, { status: 404 });
    }
    
    return NextResponse.json({ cancelled: true });
  } catch (e) {
    console.error('Cancel task error:', e);
    return NextResponse.json({ error: 'Failed to cancel task' }, { status: 500 });
  }
}
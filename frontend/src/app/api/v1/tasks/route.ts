import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';
import { sql } from '@/lib/db';

export const runtime = 'nodejs';

export async function POST(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const session = await requireAuth(req);
  const userId = session.user?.id;
  if (!userId) {
    return NextResponse.json({ error: 'User not found' }, { status: 401 });
  }

  const body = await req.json();
  const { task_type, payload, priority = 0 } = body;

  if (!task_type || !payload) {
    return NextResponse.json({ error: 'task_type and payload are required' }, { status: 400 });
  }

  if (!['selection', 'backtest'].includes(task_type)) {
    return NextResponse.json({ error: 'Invalid task_type' }, { status: 400 });
  }

  try {
    const result = await sql`
      INSERT INTO task_queue (user_id, task_type, payload, priority, status)
      VALUES (${userId}, ${task_type}, ${JSON.stringify(payload)}::jsonb, ${priority}, 'pending')
      RETURNING id, status, created_at
    `;
    
    const task = result.rows[0];
    
    // 获取队列位置
    const positionResult = await sql`
      SELECT COUNT(*) as position FROM task_queue
      WHERE status = 'pending' AND task_type = ${task_type}
      AND (priority > (SELECT priority FROM task_queue WHERE id = ${task.id})
           OR (priority = (SELECT priority FROM task_queue WHERE id = ${task.id}) AND created_at < (SELECT created_at FROM task_queue WHERE id = ${task.id})))
    `;
    
    return NextResponse.json({
      task_id: task.id,
      status: task.status,
      position: parseInt(positionResult.rows[0].position) + 1,
      estimated_wait_sec: (parseInt(positionResult.rows[0].position) + 1) * 30,
    }, { status: 202 });
  } catch (e) {
    console.error('Submit task error:', e);
    return NextResponse.json({ error: 'Failed to submit task' }, { status: 500 });
  }
}

export async function GET(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const session = await requireAuth(req);
  const userId = session.user?.id;
  if (!userId) {
    return NextResponse.json({ error: 'User not found' }, { status: 401 });
  }

  try {
    const tasks = await sql`
      SELECT id, task_type, status, assigned_node, result, error,
             created_at, started_at, finished_at
      FROM task_queue
      WHERE user_id = ${userId}
      ORDER BY created_at DESC
      LIMIT 50
    `;
    
    return NextResponse.json(tasks.rows);
  } catch (e) {
    console.error('Get tasks error:', e);
    return NextResponse.json({ error: 'Failed to get tasks' }, { status: 500 });
  }
}
import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'nodejs';

const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';

async function forwardToNode1(path: string, options: RequestInit) {
  const url = `${NODE1_URL}/internal${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${INTERNAL_TOKEN}`,
      ...options.headers,
    },
  });
  
  const data = await response.json().catch(() => ({}));
  return NextResponse.json(data, { status: response.status });
}

export async function POST(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const auth = await requireAuth(req);
  const userId = auth.user?.userId;
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

  // Forward to Node1 internal API
  const node1Body = {
    user_id: userId,
    task_type,
    payload,
    priority,
  };

  return forwardToNode1('/tasks', {
    method: 'POST',
    body: JSON.stringify(node1Body),
  });
}

export async function GET(req: NextRequest) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const auth = await requireAuth(req);
  const userId = auth.user?.userId;
  if (!userId) {
    return NextResponse.json({ error: 'User not found' }, { status: 401 });
  }

  // Forward to Node1 with query params
  const searchParams = new URLSearchParams();
  searchParams.set('user_id', userId);
  
  return forwardToNode1(`/tasks?${searchParams.toString()}`, {
    method: 'GET',
  });
}
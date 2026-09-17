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

  const user_id = authErr.user?.userId;
  const role = authErr.user?.role;
  const qs = new URLSearchParams();
  if (user_id) qs.set('user_id', user_id);
  if (role) qs.set('role', role);

  return forwardToNode1(`/tasks/${taskId}?${qs}`, {
    method: 'GET',
  });
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const user_id = authErr.user?.userId;
  const role = authErr.user?.role;

  const { id } = await params;
  const taskId = parseInt(id);
  if (isNaN(taskId)) {
    return NextResponse.json({ error: 'Invalid task ID' }, { status: 400 });
  }

  const qs = new URLSearchParams();
  if (user_id) qs.set('user_id', user_id);
  if (role) qs.set('role', role);

  return forwardToNode1(`/tasks/${taskId}?${qs}`, {
    method: 'DELETE',
  });
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const authErr = await requireAuth(req);
  if (authErr.status !== 200) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: authErr.status });
  }

  const user_id = authErr.user?.userId;
  const role = authErr.user?.role;

  const { id } = await params;
  const taskId = parseInt(id);
  if (isNaN(taskId)) {
    return NextResponse.json({ error: 'Invalid task ID' }, { status: 400 });
  }

  const qs = new URLSearchParams();
  if (user_id) qs.set('user_id', user_id);
  if (role) qs.set('role', role);

  return forwardToNode1(`/tasks/${taskId}/cancel?${qs}`, {
    method: 'POST',
  });
}

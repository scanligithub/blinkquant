import { NextRequest, NextResponse } from 'next/server';
import { requireAuth } from '@/lib/auth';

export const runtime = 'edge';

export async function GET(req: NextRequest) {
  const result = await requireAuth(req);
  if (!result.user) {
    return NextResponse.json({ user: null });
  }
  const { user } = result;
  return NextResponse.json({ user: { id: user.userId, email: user.email, role: user.role } });
}

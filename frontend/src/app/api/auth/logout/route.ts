import { NextResponse } from 'next/server';
import { clearAuthCookie } from '@/lib/auth';

export const runtime = 'edge';

export async function POST() {
  const res = NextResponse.json({ success: true });
  return clearAuthCookie(res);
}

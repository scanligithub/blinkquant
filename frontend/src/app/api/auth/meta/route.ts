import { NextResponse } from 'next/server';
import { parseInviteCodes } from '@/lib/invite';

export const runtime = 'edge';

export async function GET() {
  const inviteCodes = parseInviteCodes(process.env.AUTH_INVITE_CODE);
  return NextResponse.json({ requireInvite: inviteCodes.length > 0 });
}

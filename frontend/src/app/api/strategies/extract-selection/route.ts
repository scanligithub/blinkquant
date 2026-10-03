import { NextRequest } from 'next/server';
import { POST as extractFromBacktest } from '@/app/api/strategies/extract-from-backtest/route';
export const runtime = 'nodejs';
export async function POST(req: NextRequest) { return extractFromBacktest(req); }

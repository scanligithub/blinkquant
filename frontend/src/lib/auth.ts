import { SignJWT, jwtVerify } from 'jose';
import { NextRequest, NextResponse } from 'next/server';
import { sql } from './db';

export const COOKIE_NAME = '__auth_token';
const TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 天

export interface SessionUser {
  userId: string;
  email: string;
  role: string;
}

export interface AuthResult {
  user: SessionUser | null;
  status: number;
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function getSecret(): Uint8Array {
  const secret = process.env.AUTH_SECRET;
  if (!secret) {
    if (process.env.NODE_ENV === 'production') {
      throw new Error('AUTH_SECRET is required in production environment');
    }
    console.warn('[auth] AUTH_SECRET missing, falling back to insecure dev secret');
    return new TextEncoder().encode('blinkquant-dev-insecure-secret');
  }
  return new TextEncoder().encode(secret);
}

export function isValidEmail(email: string): boolean {
  return typeof email === 'string' && email.length <= 254 && EMAIL_REGEX.test(email);
}

export function isValidPassword(password: string): boolean {
  return typeof password === 'string' && password.length >= 8 && password.length <= 128;
}

export async function signToken(user: SessionUser): Promise<string> {
  return new SignJWT({ email: user.email, role: user.role })
    .setProtectedHeader({ alg: 'HS256' })
    .setSubject(user.userId)
    .setIssuedAt()
    .setExpirationTime(Math.floor(Date.now() / 1000) + TOKEN_TTL_SECONDS)
    .sign(getSecret());
}

export async function verifyToken(token: string): Promise<SessionUser | null> {
  try {
    const { payload } = await jwtVerify(token, getSecret(), { algorithms: ['HS256'] });
    if (!payload.sub || typeof payload.email !== 'string' || typeof payload.role !== 'string') {
      return null;
    }
    return { userId: payload.sub, email: payload.email, role: payload.role };
  } catch {
    return null;
  }
}

export function getTokenFromRequest(req: NextRequest): string | null {
  return req.cookies.get(COOKIE_NAME)?.value ?? null;
}

export async function requireAuth(req: NextRequest): Promise<AuthResult> {
  const token = getTokenFromRequest(req);
  if (!token) return { user: null, status: 401 };
  const user = await verifyToken(token);
  if (!user) return { user: null, status: 401 };
  return { user, status: 200 };
}

export async function requireAdmin(req: NextRequest): Promise<AuthResult> {
  const result = await requireAuth(req);
  if (!result.user) return result;
  if (result.user.role !== 'admin') return { user: null, status: 403 };
  return result;
}

export function setAuthCookie(res: NextResponse, token: string): NextResponse {
  res.cookies.set({
    name: COOKIE_NAME,
    value: token,
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    path: '/',
    maxAge: TOKEN_TTL_SECONDS,
  });
  return res;
}

export function clearAuthCookie(res: NextResponse): NextResponse {
  res.cookies.set({
    name: COOKIE_NAME,
    value: '',
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    path: '/',
    maxAge: 0,
  });
  return res;
}

export async function ensureAdmin(): Promise<void> {
  const email = process.env.AUTH_ADMIN_EMAIL;
  const password = process.env.AUTH_ADMIN_PASSWORD;
  if (!email || !password) return;

  const bcrypt = (await import('bcryptjs')).default;
  try {
    const normalized = email.trim().toLowerCase();
    const existing = await sql`SELECT id FROM users WHERE email = ${normalized} LIMIT 1`;
    if (existing.rows.length > 0) return;

    const hash = await bcrypt.hash(password, 10);
    await sql`
      INSERT INTO users (id, email, password_hash, role, status)
      VALUES (gen_random_uuid(), ${normalized}, ${hash}, 'admin', 'active')
      ON CONFLICT (email) DO NOTHING
    `;
    console.log(`[auth] Admin seed created for ${normalized}`);
  } catch (e) {
    console.error('[auth] ensureAdmin failed:', e);
  }
}

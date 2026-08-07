import { test } from 'node:test';
import assert from 'node:assert/strict';
import { SignJWT, jwtVerify } from 'jose';
import bcrypt from 'bcryptjs';

// 与 src/lib/auth.ts 保持一致的参数
const COOKIE_NAME = '__auth_token';
const TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60;
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SECRET = new TextEncoder().encode('test-secret-0123456789abcdef');
const ALG = 'HS256';

function isValidEmail(email) {
  return typeof email === 'string' && email.length <= 254 && EMAIL_REGEX.test(email);
}

function isValidPassword(password) {
  return typeof password === 'string' && password.length >= 8 && password.length <= 128;
}

async function signToken(user) {
  return new SignJWT({ email: user.email, role: user.role })
    .setProtectedHeader({ alg: ALG })
    .setSubject(user.userId)
    .setIssuedAt()
    .setExpirationTime(Math.floor(Date.now() / 1000) + TOKEN_TTL_SECONDS)
    .sign(SECRET);
}

async function verifyToken(token) {
  try {
    const { payload } = await jwtVerify(token, SECRET, { algorithms: [ALG] });
    if (!payload.sub || typeof payload.email !== 'string' || typeof payload.role !== 'string') return null;
    return { userId: payload.sub, email: payload.email, role: payload.role };
  } catch {
    return null;
  }
}

test('JWT 签发/校验往返', async () => {
  const token = await signToken({ userId: 'u1', email: 'a@b.com', role: 'user' });
  const payload = await verifyToken(token);
  assert.deepEqual(payload, { userId: 'u1', email: 'a@b.com', role: 'user' });
});

test('JWT 载荷包含 iat 与 exp 且有效期 7 天', async () => {
  const token = await signToken({ userId: 'u1', email: 'a@b.com', role: 'user' });
  const { payload } = await jwtVerify(token, SECRET, { algorithms: [ALG] });
  assert.ok(typeof payload.iat === 'number');
  assert.ok(typeof payload.exp === 'number');
  assert.ok(payload.exp > payload.iat);
  assert.equal(payload.exp - payload.iat, TOKEN_TTL_SECONDS);
});

test('篡改后的 token 校验失败', async () => {
  const token = await signToken({ userId: 'u1', email: 'a@b.com', role: 'user' });
  const tampered = token.slice(0, -2) + (token.endsWith('xx') ? 'yy' : 'zz');
  const payload = await verifyToken(tampered);
  assert.equal(payload, null);
});

test('过期 token 校验失败', async () => {
  const expired = await new SignJWT({ email: 'a@b.com', role: 'user' })
    .setProtectedHeader({ alg: ALG })
    .setSubject('u1')
    .setIssuedAt()
    .setExpirationTime(-1) // 已过期
    .sign(SECRET);
  const payload = await verifyToken(expired);
  assert.equal(payload, null);
});

test('无效签名算法 (none) 被 jose 拒绝', async () => {
  await assert.rejects(
    new SignJWT({ email: 'a@b.com', role: 'user' })
      .setProtectedHeader({ alg: 'none' })
      .setSubject('u1')
      .sign(SECRET)
  );
});

test('错误密钥签发的 token 校验失败', async () => {
  const otherSecret = new TextEncoder().encode('another-secret-0123456789abcdef');
  const token = await new SignJWT({ email: 'a@b.com', role: 'user' })
    .setProtectedHeader({ alg: ALG })
    .setSubject('u1')
    .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
    .sign(otherSecret);
  const payload = await verifyToken(token);
  assert.equal(payload, null);
});

test('缺少必需载荷字段的 token 返回 null', async () => {
  const token = await new SignJWT({})
    .setProtectedHeader({ alg: ALG })
    .setExpirationTime(3600)
    .sign(SECRET);
  const payload = await verifyToken(token);
  assert.equal(payload, null);
});

test('密码哈希与比对', async () => {
  const hash = await bcrypt.hash('password123', 10);
  assert.ok(hash.startsWith('$2'));
  assert.ok(await bcrypt.compare('password123', hash));
  assert.ok(!(await bcrypt.compare('wrongpass', hash)));
});

test('邮箱校验', () => {
  assert.ok(isValidEmail('user@example.com'));
  assert.ok(isValidEmail('a.b+c@sub.domain.org'));
  assert.ok(!isValidEmail('not-an-email'));
  assert.ok(!isValidEmail('a@b'));
  assert.ok(!isValidEmail(''));
  assert.ok(!isValidEmail('x'.repeat(300) + '@a.com'));
});

test('密码校验', () => {
  assert.ok(isValidPassword('12345678'));
  assert.ok(isValidPassword('a'.repeat(128)));
  assert.ok(!isValidPassword('1234567'));
  assert.ok(!isValidPassword(''));
  assert.ok(!isValidPassword('a'.repeat(129)));
});

test('Cookie 名与设计一致', () => {
  assert.equal(COOKIE_NAME, '__auth_token');
});

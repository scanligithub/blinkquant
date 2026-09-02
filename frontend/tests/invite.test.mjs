// frontend/tests/invite.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// 与 src/lib/invite.ts 保持一致（复制实现以绕过 TS import）
function parseInviteCodes(envValue) {
  if (!envValue) return [];
  return envValue.split(',').map((s) => s.trim()).filter(Boolean);
}

function isValidInviteCode(inviteCodes, code) {
  if (inviteCodes.length === 0) return true;
  return typeof code === 'string' && inviteCodes.includes(code);
}

test('parseInviteCodes: undefined 返回空数组', () => {
  assert.deepEqual(parseInviteCodes(undefined), []);
});

test('parseInviteCodes: 空串返回空数组', () => {
  assert.deepEqual(parseInviteCodes(''), []);
});

test('parseInviteCodes: 纯空格串返回空数组', () => {
  assert.deepEqual(parseInviteCodes('   '), []);
});

test('parseInviteCodes: 单码', () => {
  assert.deepEqual(parseInviteCodes('abc123'), ['abc123']);
});

test('parseInviteCodes: 多码逗号分隔并 trim', () => {
  assert.deepEqual(parseInviteCodes('abc, def , ghi'), ['abc', 'def', 'ghi']);
});

test('parseInviteCodes: 过滤中间空项', () => {
  assert.deepEqual(parseInviteCodes('abc,,def'), ['abc', 'def']);
});

test('isValidInviteCode: 未启用（空列表）恒通过', () => {
  assert.equal(isValidInviteCode([], 'anything'), true);
  assert.equal(isValidInviteCode([], undefined), true);
  assert.equal(isValidInviteCode([], null), true);
});

test('isValidInviteCode: 缺失码拒绝', () => {
  assert.equal(isValidInviteCode(['abc'], undefined), false);
  assert.equal(isValidInviteCode(['abc'], null), false);
  assert.equal(isValidInviteCode(['abc'], ''), false);
});

test('isValidInviteCode: 错误码拒绝', () => {
  assert.equal(isValidInviteCode(['abc'], 'xyz'), false);
});

test('isValidInviteCode: 正确码通过', () => {
  assert.equal(isValidInviteCode(['abc'], 'abc'), true);
});

test('isValidInviteCode: 大小写敏感', () => {
  assert.equal(isValidInviteCode(['AbC'], 'abc'), false);
  assert.equal(isValidInviteCode(['abc'], 'AbC'), false);
});

test('isValidInviteCode: 多码之一匹配即通过', () => {
  assert.equal(isValidInviteCode(['abc', 'def'], 'def'), true);
});

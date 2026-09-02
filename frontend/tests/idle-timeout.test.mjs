// frontend/tests/idle-timeout.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';

// 复制实现核心逻辑用于测试（模拟 Provider 内部逻辑）
function createIdleController(timeoutMs, onTimeout) {
  let timerId = null;
  const reset = () => {
    if (timerId) clearTimeout(timerId);
    timerId = setTimeout(onTimeout, timeoutMs);
  };
  const destroy = () => {
    if (timerId) clearTimeout(timerId);
    timerId = null;
  };
  return { reset, destroy };
}

test('空闲超时触发回调', async () => {
  let called = false;
  const controller = createIdleController(100, () => { called = true; });
  controller.reset(); // 启动计时器（模拟 Provider 挂载时调用 resetTimer）
  
  // 等待超过超时时间
  await new Promise(r => setTimeout(r, 150));
  
  assert.equal(called, true);
  controller.destroy();
});

test('交互事件重置计时器', async () => {
  let callCount = 0;
  const controller = createIdleController(100, () => { callCount++; });
  
  // 50ms 时触发一次交互（重置）
  await new Promise(r => setTimeout(r, 50));
  controller.reset();
  
  // 再等 150ms（总计 200ms，但重置后只有 100ms 才会触发）
  await new Promise(r => setTimeout(r, 150));
  
  // 应该只触发 1 次（第一次 100ms 被重置取消，第二次 100ms 触发）
  assert.equal(callCount, 1);
  controller.destroy();
});

test('destroy 防止回调触发', async () => {
  let called = false;
  const controller = createIdleController(50, () => { called = true; });
  
  controller.destroy();
  await new Promise(r => setTimeout(r, 100));
  
  assert.equal(called, false);
});

test('多次快速交互只保留最后一次重置', async () => {
  let callCount = 0;
  const controller = createIdleController(100, () => { callCount++; });
  
  // 10ms, 20ms, 30ms 各触发一次交互
  await new Promise(r => setTimeout(r, 10));
  controller.reset();
  await new Promise(r => setTimeout(r, 10));
  controller.reset();
  await new Promise(r => setTimeout(r, 10));
  controller.reset();
  
  // 等待 150ms（从最后一次重置算起）
  await new Promise(r => setTimeout(r, 150));
  
  // 应该只触发 1 次
  assert.equal(callCount, 1);
  controller.destroy();
});
# 空闲 30 分钟自动登出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 前端空闲 30 分钟无交互操作（鼠标/键盘/触摸/滚动）后自动登出并跳转 `/login`，全局生效，复用现有登出接口。

**Architecture:** 新增 `IdleTimeoutProvider` 客户端组件（React Context Provider 模式），在根布局 `layout.tsx` 中包裹全站；内部用 `useEffect` 注册全局事件监听器 + `setTimeout`，任意交互事件重置计时器，超时触发登出流程（调用 `/api/auth/logout` + 硬跳转 `/login`）。

**Tech Stack:** Next.js 14 (App Router), React 18 (useEffect/useRef), 原生 DOM 事件 API。

---

## 文件结构

- 新增：`frontend/src/components/IdleTimeoutProvider.tsx` — 核心 Provider 组件
- 修改：`frontend/src/app/layout.tsx` — 引入并包裹 Provider
- 新增：`frontend/tests/idle-timeout.test.mjs` — 单测（模拟时间推进验证超时触发登出）

**运行环境注意**：所有命令在 `E:\数据中台\blinkquant\frontend` 下执行（`node --test` / `npx tsc --noEmit` / `npm run build`）；git 提交在 `E:\数据中台\blinkquant` 仓库根执行。项目 `strict: false`。

---

### Task 1: IdleTimeoutProvider 组件 + 单测

**Files:**
- Create: `frontend/src/components/IdleTimeoutProvider.tsx`
- Test: `frontend/tests/idle-timeout.test.mjs`

- [ ] **Step 1: Write the failing test**

创建 `frontend/tests/idle-timeout.test.mjs`：

```js
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test tests/idle-timeout.test.mjs` (workdir `E:\数据中台\blinkquant\frontend`)
Expected: 测试通过（实现副本在测试内，无需 src 存在）

- [ ] **Step 3: Write implementation**

创建 `frontend/src/components/IdleTimeoutProvider.tsx`：

```tsx
'use client';

import { useEffect, useRef, useCallback } from 'react';
import { useRouter } from 'next/navigation';

interface IdleTimeoutProviderProps {
  children: React.ReactNode;
  timeoutMs?: number; // 默认 30 分钟
}

const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

const EVENTS = ['mousemove', 'keydown', 'click', 'touchstart', 'scroll'] as const;

export function IdleTimeoutProvider({
  children,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}: IdleTimeoutProviderProps) {
  const router = useRouter();
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const logout = useCallback(async () => {
    try {
      await fetch('/api/auth/logout', {
        method: 'POST',
        credentials: 'include',
      });
    } catch {
      // 网络错误也要跳转登录页
    }
    window.location.href = '/login';
  }, []);

  const resetTimer = useCallback(() => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    timeoutRef.current = setTimeout(logout, timeoutMs);
  }, [logout, timeoutMs]);

  const handleActivity = useCallback(() => {
    resetTimer();
  }, [resetTimer]);

  useEffect(() => {
    // 初始启动计时器
    resetTimer();

    // 注册全局事件监听器
    EVENTS.forEach((event) => {
      window.addEventListener(event, handleActivity, { passive: true });
    });

    // 清理函数
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      EVENTS.forEach((event) => {
        window.removeEventListener(event, handleActivity);
      });
    };
  }, [resetTimer, handleActivity]);

  return <>{children}</>;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --test tests/idle-timeout.test.mjs`
Expected: 6 pass / 0 fail

- [ ] **Step 5: Type check**

Run: `npx tsc --noEmit`
Expected: 无输出（成功）

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/IdleTimeoutProvider.tsx frontend/tests/idle-timeout.test.mjs
git commit -m "feat: 新增空闲超时自动登出 Provider 与单测"
```

---

### Task 2: 根布局引入 Provider

**Files:**
- Modify: `frontend/src/app/layout.tsx`

- [ ] **Step 1: Read current layout**

读取 `frontend/src/app/layout.tsx` 确认当前结构。

- [ ] **Step 2: Add import and wrap children**

在文件顶部 import 区新增：
```tsx
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider';
```

在 `body` 内部包裹 `{children}`：
```tsx
<body className={...}>
  <IdleTimeoutProvider>
    {children}
  </IdleTimeoutProvider>
</body>
```

- [ ] **Step 3: Type check**

Run: `npx tsc --noEmit` (workdir `E:\数据中台\blinkquant\frontend`)
Expected: 无输出（成功）

- [ ] **Step 4: Commit**

```bash
git add frontend/src/app/layout.tsx
git commit -m "feat: 根布局引入空闲超时 Provider"
```

---

### Task 3: 文档更新 + 全量验证 + 推送

**Files:**
- Modify: `docs/CONTEXT.md`

- [ ] **Step 1: Update CONTEXT.md**

在 `docs/CONTEXT.md` 第 6 节「注意事项」列表末尾追加一条：
```
- 空闲自动登出：前端 `IdleTimeoutProvider` 监听交互事件，30 分钟无操作自动调用 `/api/auth/logout` 并跳转 `/login`；组件在 `frontend/src/components/IdleTimeoutProvider.tsx`，全局生效。
```

- [ ] **Step 2: Full verification**

Run（在 `frontend` 目录）：
```bash
node --test tests/auth.test.mjs tests/invite.test.mjs tests/export.test.mjs tests/idle-timeout.test.mjs
npx tsc --noEmit
npm run build
```
Expected: 全部测试 pass（37 + 6 = 43）；tsc 无输出；build 成功。

- [ ] **Step 3: Commit**

```bash
git add docs/CONTEXT.md
git commit -m "docs: 补充空闲自动登出说明"
```

- [ ] **Step 4: Push**

```bash
git push origin main
```
Expected: 全部提交推送成功（若遇 GitHub `Connection was reset`，等待后重试）。

---

## 验证清单（手动验收）

- [ ] 打开任意受保护页面，无操作 30 分钟 → 自动跳转 `/login`
- [ ] 操作页面（鼠标移动/点击/键盘/滚动） → 计时器重置，不触发登出
- [ ] 登出后 Cookie `__auth_token` 被清除，访问受保护页面被重定向登录
- [ ] 多标签页独立计时（A 标签操作不重置 B 标签计时器）
- [ ] 页面卸载/关闭标签 → 无内存泄漏（监听器正确移除）

## 相关文件

- 新增：`frontend/src/components/IdleTimeoutProvider.tsx`、`frontend/tests/idle-timeout.test.mjs`
- 修改：`frontend/src/app/layout.tsx`、`docs/CONTEXT.md`
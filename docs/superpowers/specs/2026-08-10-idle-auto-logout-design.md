# 设计文档：前端空闲 30 分钟自动登出

日期：2026-08-10
状态：已认可，待实施

## 背景

当前认证基于 JWT Cookie（`__auth_token`，7 天有效期），用户关闭浏览器或手动登出前一直保持登录。为安全起见，需在用户连续 30 分钟无任何交互操作后自动登出。

## 目标

- 用户 30 分钟无鼠标移动、点击、键盘输入、触摸、滚动等交互 → 自动登出并跳转 `/login`
- 全局生效：所有受保护页面自动受保护，无需逐页改造
- 登出流程复用现有 `/api/auth/logout`（清除 Cookie + 服务端会话清理）

## 非目标（YAGNI）

- 不做跨标签页同步计时器（各标签独立）
- 不做超时前倒计时警告/续期弹窗
- 不做 Page Visibility API 暂停计时（页面隐藏仍计时）
- 不做服务端会话刷新/滑动窗口（JWT 固定 7 天过期，仅前端强制登出）

## 方案

### 1. 新增 IdleTimeoutProvider 组件

文件：`frontend/src/components/IdleTimeoutProvider.tsx`

- `'use client'` 组件
- Props：`children: React.ReactNode`、`timeoutMs?: number`（默认 30 * 60 * 1000）
- 内部 `useEffect`：
  - 注册全局事件监听：`mousemove`、`keydown`、`click`、`touchstart`、`scroll`（`passive: true`）
  - `setTimeout(logout, timeoutMs)`
  - 任意事件触发 → `clearTimeout` + 重新 `setTimeout`
  - 清理函数：`clearTimeout` + 移除所有监听器
- 登出函数：
  ```ts
  const logout = async () => {
    await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' });
    window.location.href = '/login';
  };
  ```

### 2. 根布局包裹 Provider

文件：`frontend/src/app/layout.tsx`

```tsx
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider';

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>
        <IdleTimeoutProvider>{children}</IdleTimeoutProvider>
      </body>
    </html>
  );
}
```

### 3. 行为细节

- **触发事件**：`mousemove`、`keydown`、`click`、`touchstart`、`scroll`（覆盖桌面/移动端常见交互）
- **无警告/弹窗**：超时直接跳转 `/login`（硬跳转 `window.location.href`，避免 Next.js 客户端路由残留状态）
- **全局生效**：根布局包裹 `{children}`，所有页面自动受保护
- **多标签页**：各标签独立计时，互不干扰（符合「当前页面无操作」语义）
- **页面隐藏**：切标签/最小化时计时器继续运行（用户未关闭页面，只是暂离）

### 4. 复用现有登出接口

- `POST /api/auth/logout`：现有实现调用 `clearAuthCookie` 清除 Cookie，返回 200
- 前端 `fetch('/api/auth/logout', { method: 'POST', credentials: 'include' })` 后硬跳转 `/login`

## 数据流

```
用户交互事件 → clearTimeout + 重置 setTimeout
30 分钟无事件 → logout()
  → fetch('/api/auth/logout', { method: 'POST', credentials: 'include' })
  → 服务端 clearAuthCookie（清除 __auth_token）
  → window.location.href = '/login'（硬跳转）
```

## 错误处理

- `/api/auth/logout` 失败（网络/5xx）：仍执行 `window.location.href = '/login'`（前端以登出态呈现，Cookie 若未清由浏览器自然过期）
- 组件卸载/路由跳转：`useEffect` 清理函数自动 `clearTimeout` + 移除监听器，防内存泄漏

## 测试

- 单元测试：`frontend/tests/idle-timeout.test.mjs`（或复用 `node --test` 模式），模拟时间推进验证 30 分钟触发 `logout` 调用
- 手动验证：打开页面无操作 30 分钟 → 跳登录页；操作页面 → 计时器重置

## 相关文件

- 新增：`frontend/src/components/IdleTimeoutProvider.tsx`
- 修改：`frontend/src/app/layout.tsx`
- 无需改动：`frontend/src/lib/auth.ts`、`frontend/src/app/api/auth/logout/route.ts`、`frontend/src/app/page.tsx`
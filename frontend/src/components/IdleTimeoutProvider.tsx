'use client';

import { useEffect, useRef, useCallback } from 'react';

interface IdleTimeoutProviderProps {
  children: React.ReactNode;
  timeoutMs?: number; // 默认 5 分钟
}

const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;

const EVENTS = ['mousemove', 'keydown', 'click', 'touchstart', 'scroll'] as const;

export function IdleTimeoutProvider({
  children,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}: IdleTimeoutProviderProps) {
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
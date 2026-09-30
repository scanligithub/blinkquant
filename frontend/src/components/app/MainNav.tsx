'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { useState } from 'react';
import { downloadFromResponse } from '@/lib/download';

export interface NavUser {
  id: string;
  email: string;
  role: string;
}

interface MainNavProps {
  user: NavUser | null;
  onLogout: () => void;
  onOpenWatchlist?: () => void;
  onOpenStrategies?: () => void;
}

const NAV_ITEMS = [
  { label: '选股', href: '/select', enabled: true },
  { label: '股票研究', href: '/stocks', enabled: true },
  { label: '自选股', href: '/watchlists', enabled: true },
  { label: '策略库', href: '/strategies', enabled: false },
  { label: '回测研究', href: '/backtests', enabled: false },
  { label: '成果库', href: '/artifacts', enabled: false },
  { label: '任务中心', href: '/tasks', enabled: false },
  { label: '系统状态', href: '/system', enabled: false },
] as const;

export default function MainNav({
  user,
  onLogout,
  onOpenWatchlist,
  onOpenStrategies,
}: MainNavProps) {
  const pathname = usePathname();
  const router = useRouter();
  const [menuOpen, setMenuOpen] = useState(false);

  const renderNavItem = (item: typeof NAV_ITEMS[number]) => {
    if (item.enabled) {
      const active = pathname === item.href || pathname.startsWith(item.href + '/');
      return (
        <Link
          key={item.href}
          href={item.href}
          className={`px-2.5 py-1.5 rounded-lg text-xs font-semibold ${active ? 'text-blue-700 bg-blue-50' : 'text-slate-600 hover:bg-slate-50'}`}
        >
          {item.label}
        </Link>
      );
    }
    return (
      <span
        key={item.href}
        aria-disabled="true"
        title="下一阶段开放"
        className="px-2.5 py-1.5 rounded-lg text-xs font-semibold text-slate-300 cursor-default"
      >
        {item.label}
      </span>
      );
  };

  return (
    <nav className="sticky top-0 z-50 border-b border-slate-200 bg-white/95 backdrop-blur">
      <div className="max-w-7xl mx-auto px-4 md:px-8 h-14 flex items-center gap-4">
        <Link href="/select" className="shrink-0 flex items-center gap-2">
          <span className="text-lg md:text-xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
            BlinkQuant
          </span>
          <span className="hidden md:inline text-xs text-slate-400">量化研究</span>
        </Link>

        <div className="hidden lg:flex items-center gap-1 min-w-0 flex-1">
          {NAV_ITEMS.map(renderNavItem)}
        </div>

        {user && (
          <div className="relative shrink-0">
            <button
              type="button"
              onClick={() => setMenuOpen((v) => !v)}
              className="flex items-center gap-2 bg-white border border-slate-200 rounded-xl px-3 py-2 shadow-sm hover:bg-slate-50 transition-colors"
              aria-expanded={menuOpen}
              aria-haspopup="menu"
            >
              <span className="text-sm font-medium text-slate-700 max-w-[160px] truncate">{user.email}</span>
              {user.role === 'admin' && (
                <span className="text-xs font-bold bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full">管理员</span>
              )}
              <svg className="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" aria-hidden="true">
                <path stroke="currentColor" strokeWidth="2" d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {menuOpen && (
              <div className="absolute right-0 mt-2 w-48 bg-white border border-slate-200 rounded-xl shadow-lg overflow-hidden" role="menu">
                <button
                  type="button"
                  onClick={() => {
                    setMenuOpen(false);
                    router.push('/watchlists');
                  }}
                  className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  自选股
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setMenuOpen(false);
                    onOpenStrategies?.();
                  }}
                  className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  我的策略
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setMenuOpen(false);
                    downloadFromResponse('/api/me/export');
                  }}
                  className="w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  导出我的数据
                </button>
                {user.role === 'admin' && (
                  <Link
                    href="/admin"
                    onClick={() => setMenuOpen(false)}
                    className="block w-full text-left px-4 py-2.5 text-sm text-slate-700 hover:bg-slate-50"
                    role="menuitem"
                  >
                    管理后台
                  </Link>
                )}
                <button
                  type="button"
                  onClick={() => {
                    setMenuOpen(false);
                    onLogout();
                  }}
                  className="w-full text-left px-4 py-2.5 text-sm text-red-600 border-t border-slate-100 hover:bg-red-50"
                >
                  退出登录
                </button>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="lg:hidden border-t border-slate-100 overflow-x-auto">
        <div className="max-w-7xl mx-auto px-4 py-2 flex gap-1 min-w-max">
          {NAV_ITEMS.map(renderNavItem)}
        </div>
      </div>
    </nav>
  );
}

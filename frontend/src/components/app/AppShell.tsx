'use client';

import type { ReactNode } from 'react';
import MainNav, { type NavUser } from './MainNav';

interface AppShellProps {
  user: NavUser | null;
  onLogout: () => void;
  onOpenWatchlist?: () => void;
  onOpenStrategies?: () => void;
  children: ReactNode;
}

export default function AppShell({
  user,
  onLogout,
  onOpenWatchlist,
  onOpenStrategies,
  children,
}: AppShellProps) {
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <MainNav
        user={user}
        onLogout={onLogout}
        onOpenWatchlist={onOpenWatchlist}
        onOpenStrategies={onOpenStrategies}
      />
      {children}
    </div>
  );
}

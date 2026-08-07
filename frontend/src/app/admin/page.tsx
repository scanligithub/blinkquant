'use client';
import { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';

interface AdminUser {
  id: string;
  email: string;
  role: string;
  status: string;
  created_at: string;
  last_login_at: string | null;
}

export default function AdminPage() {
  const router = useRouter();
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [keyword, setKeyword] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notAdmin, setNotAdmin] = useState(false);

  const fetchUsers = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
      if (keyword) params.set('keyword', keyword);
      if (statusFilter) params.set('status', statusFilter);
      const res = await fetch(`/api/admin/users?${params}`, { cache: 'no-store' });
      if (res.status === 401) {
        router.replace('/login');
        return;
      }
      if (res.status === 403) {
        setNotAdmin(true);
        setLoading(false);
        return;
      }
      const json = await res.json();
      if (!res.ok) {
        setError(json.error || '加载失败');
        return;
      }
      setUsers(json.users || []);
      setTotal(json.total || 0);
    } catch (e) {
      setError('网络错误');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, keyword, statusFilter, router]);

  useEffect(() => {
    fetchUsers();
  }, [fetchUsers]);

  const updateUser = async (id: string, patch: { role?: string; status?: string }) => {
    try {
      const res = await fetch(`/api/admin/users/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      });
      const json = await res.json();
      if (!res.ok) {
        alert(json.error || '操作失败');
        return;
      }
      setUsers((prev) => prev.map((u) => (u.id === id ? json.user : u)));
    } catch (e) {
      alert('网络错误');
    }
  };

  const deleteUser = async (id: string, email: string) => {
    if (!confirm(`确定删除用户 ${email}？该用户的自选股和策略将一并删除。`)) return;
    try {
      const res = await fetch(`/api/admin/users/${id}`, { method: 'DELETE' });
      const json = await res.json();
      if (!res.ok) {
        alert(json.error || '删除失败');
        return;
      }
      setUsers((prev) => prev.filter((u) => u.id !== id));
      setTotal((t) => Math.max(0, t - 1));
    } catch (e) {
      alert('网络错误');
    }
  };

  if (notAdmin) {
    return (
      <main className="min-h-screen flex items-center justify-center bg-slate-50 text-slate-900 font-sans">
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8 text-center">
          <p className="font-bold text-slate-700 mb-2">无权限访问</p>
          <Link href="/" className="text-blue-600 text-sm hover:underline">返回首页</Link>
        </div>
      </main>
    );
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <main className="min-h-screen p-4 md:p-8 font-sans bg-slate-50 text-slate-900">
      <div className="max-w-5xl mx-auto space-y-6">
        <header className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-black bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">用户管理</h1>
            <p className="text-slate-500 text-sm mt-1">共 {total} 个用户</p>
          </div>
          <Link href="/" className="text-sm text-blue-600 hover:underline">← 返回首页</Link>
        </header>

        {error && <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-xl px-3 py-2">{error}</div>}

        <div className="flex flex-col sm:flex-row gap-3">
          <input
            value={keyword}
            onChange={(e) => { setKeyword(e.target.value); setPage(1); }}
            placeholder="搜索邮箱..."
            className="flex-1 bg-white border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
          />
          <select
            value={statusFilter}
            onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
            className="bg-white border border-slate-200 rounded-xl px-3 py-2 text-sm"
          >
            <option value="">全部状态</option>
            <option value="active">启用</option>
            <option value="disabled">禁用</option>
          </select>
        </div>

        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-slate-50/50 text-left text-xs text-slate-500 uppercase tracking-wider">
                <th className="px-4 py-3">邮箱</th>
                <th className="px-4 py-3">角色</th>
                <th className="px-4 py-3">状态</th>
                <th className="px-4 py-3">注册时间</th>
                <th className="px-4 py-3">最近登录</th>
                <th className="px-4 py-3">操作</th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr><td colSpan={6} className="px-4 py-8 text-center text-slate-400">加载中...</td></tr>
              )}
              {!loading && users.length === 0 && (
                <tr><td colSpan={6} className="px-4 py-8 text-center text-slate-400">无匹配用户</td></tr>
              )}
              {users.map((u) => (
                <tr key={u.id} className="border-t border-slate-100">
                  <td className="px-4 py-3 font-medium">{u.email}</td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => updateUser(u.id, { role: u.role === 'admin' ? 'user' : 'admin' })}
                      className={`text-xs font-bold px-2 py-0.5 rounded-full ${u.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-slate-100 text-slate-600'}`}
                    >
                      {u.role === 'admin' ? '管理员' : '用户'}
                    </button>
                  </td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => updateUser(u.id, { status: u.status === 'active' ? 'disabled' : 'active' })}
                      className={`text-xs font-bold px-2 py-0.5 rounded-full ${u.status === 'active' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}
                    >
                      {u.status === 'active' ? '启用' : '禁用'}
                    </button>
                  </td>
                  <td className="px-4 py-3 text-slate-500">{u.created_at ? new Date(u.created_at).toLocaleDateString() : '-'}</td>
                  <td className="px-4 py-3 text-slate-500">{u.last_login_at ? new Date(u.last_login_at).toLocaleString() : '-'}</td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => deleteUser(u.id, u.email)}
                      className="text-xs text-red-500 border border-red-200 hover:bg-red-50 rounded-lg px-2.5 py-1"
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="flex justify-between items-center">
          <button
            disabled={page <= 1}
            onClick={() => setPage((p) => p - 1)}
            className="text-sm px-3 py-1.5 border border-slate-200 rounded-lg disabled:opacity-40 bg-white"
          >
            上一页
          </button>
          <span className="text-sm text-slate-500">第 {page} / {totalPages} 页</span>
          <button
            disabled={page >= totalPages}
            onClick={() => setPage((p) => p + 1)}
            className="text-sm px-3 py-1.5 border border-slate-200 rounded-lg disabled:opacity-40 bg-white"
          >
            下一页
          </button>
        </div>
      </div>
    </main>
  );
}

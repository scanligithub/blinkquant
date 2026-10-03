import { node1Json, node1Path } from '@/lib/node1Internal';

export const DEFAULT_WATCHLIST_NAME = '默认自选';
export const MAX_WATCHLIST_NAME_LENGTH = 40;

export function parseWatchlistId(value: string | null | undefined): number | null {
  if (!value) return null;
  const id = Number(value);
  return Number.isInteger(id) && id > 0 ? id : null;
}

export function validateWatchlistName(value: unknown): string {
  const name = String(value ?? '').trim();
  if (!name) throw new Error('自选股列表名称不能为空');
  if (name.length > MAX_WATCHLIST_NAME_LENGTH) throw new Error('自选股列表名称过长');
  return name;
}

export async function ensureDefaultWatchlist(userId: string) {
  const { response, data } = await node1Json(node1Path('/user-assets/watchlists', { user_id: userId }));
  if (!response.ok) throw new Error(String(data?.detail || data?.error || '加载自选股失败'));
  const list = Array.isArray(data?.watchlists) ? data.watchlists.find((item: any) => item.is_default) : null;
  return list ?? null;
}

export async function getOwnedWatchlist(userId: string, id: number) {
  const { response, data } = await node1Json(node1Path('/user-assets/watchlists/' + id, { user_id: userId }));
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(String(data?.detail || data?.error || '加载自选股失败'));
  return data?.watchlist ?? null;
}

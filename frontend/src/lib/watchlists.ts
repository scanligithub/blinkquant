import { sql } from '@/lib/db';

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
  const existing = await sql`
    SELECT id, user_id, name, is_default, created_at, updated_at
    FROM watchlists
    WHERE user_id = ${userId} AND is_default = TRUE
    ORDER BY created_at ASC
    LIMIT 1
  `;
  if (existing.rows[0]) return existing.rows[0];

  await sql`
    INSERT INTO watchlists (user_id, name, is_default)
    VALUES (${userId}, ${DEFAULT_WATCHLIST_NAME}, TRUE)
    ON CONFLICT DO NOTHING
  `;
  const created = await sql`
    SELECT id, user_id, name, is_default, created_at, updated_at
    FROM watchlists
    WHERE user_id = ${userId} AND is_default = TRUE
    ORDER BY created_at ASC
    LIMIT 1
  `;
  return created.rows[0] ?? null;
}

export async function getOwnedWatchlist(userId: string, id: number) {
  const result = await sql`
    SELECT id, user_id, name, is_default, created_at, updated_at
    FROM watchlists
    WHERE id = ${id} AND user_id = ${userId}
    LIMIT 1
  `;
  return result.rows[0] ?? null;
}

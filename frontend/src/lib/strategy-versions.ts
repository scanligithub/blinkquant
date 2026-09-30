import { sql } from '@/lib/db';

let ensurePromise: Promise<void> | null = null;

/**
 * Keep the selection-strategy API compatible with deployments where the
 * incremental strategy_versions migration has not been run yet.
 *
 * This is intentionally idempotent: the explicit SQL migration remains the
 * preferred deployment path, while API traffic can self-heal a missing table.
 */
export function ensureSelectionStrategyVersions(): Promise<void> {
  if (ensurePromise) return ensurePromise;

  ensurePromise = (async () => {
    await sql`
      CREATE TABLE IF NOT EXISTS strategy_versions (
        id BIGSERIAL PRIMARY KEY,
        strategy_id BIGINT NOT NULL REFERENCES strategies (id) ON DELETE CASCADE,
        version_no INTEGER NOT NULL,
        name TEXT NOT NULL,
        formula TEXT NOT NULL,
        timeframe TEXT NOT NULL DEFAULT 'D',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (strategy_id, version_no)
      )
    `;
    await sql`
      CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy
      ON strategy_versions (strategy_id, version_no DESC)
    `;
    await sql`
      INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe, created_at)
      SELECT id, 1, name, formula, timeframe, COALESCE(created_at, NOW())
      FROM strategies s
      WHERE NOT EXISTS (
        SELECT 1 FROM strategy_versions v WHERE v.strategy_id = s.id
      )
    `;
  })().catch((error) => {
    ensurePromise = null;
    throw error;
  });

  return ensurePromise;
}

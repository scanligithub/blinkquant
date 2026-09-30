import { sql } from '@/lib/db';

let ensurePromise: Promise<void> | null = null;

/**
 * Idempotently ensures the selection-strategy version schema exists.
 * The migration SQL remains the canonical deployment mechanism; this helper
 * keeps already-running production deployments compatible until that SQL is run.
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
        source_backtest_strategy_id BIGINT,
        source_backtest_strategy_version INTEGER,
        source_backtest_strategy_name TEXT,
        source_backtest_strategy_trigger TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (strategy_id, version_no)
      )
    `;
    await sql`
      ALTER TABLE strategies
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_id BIGINT,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_version INTEGER,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_name TEXT,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_trigger TEXT
    `;
    await sql`
      ALTER TABLE strategy_versions
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_id BIGINT,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_version INTEGER,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_name TEXT,
        ADD COLUMN IF NOT EXISTS source_backtest_strategy_trigger TEXT
    `;
    await sql`
      CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy
      ON strategy_versions (strategy_id, version_no DESC)
    `;
    await sql`
      INSERT INTO strategy_versions (
        strategy_id, version_no, name, formula, timeframe,
        source_backtest_strategy_id, source_backtest_strategy_version,
        source_backtest_strategy_name, source_backtest_strategy_trigger,
        created_at
      )
      SELECT
        id, 1, name, formula, timeframe,
        source_backtest_strategy_id, source_backtest_strategy_version,
        source_backtest_strategy_name, source_backtest_strategy_trigger,
        COALESCE(created_at, NOW())
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

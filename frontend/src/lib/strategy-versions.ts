/**
 * Selection-strategy versions are stored in Node1 SQLite.
 * The Node1 scheduler initializes and migrates this schema from schema.sql,
 * so the former Neon DDL/backfill helper is intentionally a no-op.
 *
 * Kept for source compatibility with older callers.
 */
let ensurePromise: Promise<void> | null = null;

export function ensureSelectionStrategyVersions(): Promise<void> {
  if (!ensurePromise) {
    ensurePromise = Promise.resolve();
  }
  return ensurePromise;
}

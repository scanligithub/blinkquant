# BlinkQuant User Assets v1 Contract

## Purpose

IA5.10 upgrades the existing “导出我的数据” capability into a portable user-asset bundle while preserving the legacy JSON endpoint.

## Bundle

The bundle is a ZIP with format `blinkquant-user-assets-v1`:

- `manifest.json`
- `strategies.json`
- `watchlists.json`
- `artifacts/0001.zip`, `artifacts/0002.zip`, ...

The strategy and watchlist payloads reuse the IA5.7/IA5.8 formats. Each artifact entry reuses the IA5.9 `blinkquant-artifact-v1` bundle.

## Portability and ownership

- Export is authenticated and scoped to the current user.
- Portable payloads do not carry database owner IDs, task IDs, artifact IDs, watchlist IDs, strategy IDs, node assignments, or other internal ownership identifiers.
- Semantic version numbers and descriptive source names remain portable metadata.
- Import always re-enters the existing authenticated import endpoints, so imported objects belong to the current user.
- The legacy `/api/me/export` JSON endpoint remains available for backward compatibility.

## Import semantics

The unified import is an orchestration layer rather than a cross-database transaction:

1. validate the ZIP structure and declared file set;
2. import selection/backtest strategies through `/api/strategies/import`;
3. import watchlists through `/api/watchlists/import`;
4. import each artifact ZIP through `/api/artifacts/import`.

A partial result uses HTTP 207 and reports imported/skipped counts plus errors. Existing per-object import idempotency and validation rules remain authoritative.

## Limits

- Maximum 100 artifacts per bundle.
- Maximum 128 MiB for the complete bundle.
- Maximum 32 MiB per embedded artifact ZIP.
- Maximum 4 MiB per JSON asset file.

These are protection limits for the unified orchestration layer; individual asset APIs retain their own validation and quota rules.

## Non-goals

IA5.10 does not change the Scheduler, task scheduling, three-node compute topology, backtest semantics, SignalTrace semantics, database schema, or the existing single-asset API contracts.

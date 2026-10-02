# Watchlist Assets v1 Contract

## Purpose

Define the portable JSON asset format used to export and import a user's watchlist collections.

## Format

Top-level object contains `format`, `exported_at`, and `watchlists`. Each watchlist contains `name`, optional descriptive timestamps, `is_default`, and `codes`.

Example fields:
- format: `blinkquant-watchlists-v1`
- watchlists[].name: current list name
- watchlists[].is_default: descriptive export metadata only
- watchlists[].codes: array of stock codes

## Semantics

- Export is authenticated and contains only the current user's watchlists and items.
- Database IDs, user IDs and internal foreign keys are never exported.
- `is_default`, `created_at` and `updated_at` are descriptive export metadata.
- Import always creates a non-default watchlist. The incoming `is_default` value is not trusted as an instruction to change the user's default list.
- A name collision with an existing list owned by the current user causes that imported list to be skipped.
- Duplicate codes inside one imported list are de-duplicated.
- Empty watchlists are valid.
- Maximum import size is 100 lists per request and 5000 codes per list.
- Imported values are inserted under the authenticated user ID only.
- Each list plus its items is written through one SQL statement; a failed statement does not leave a partially-created imported list.
- No strategy, artifact, task or node provenance is part of this contract.

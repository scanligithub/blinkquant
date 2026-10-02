# Artifact Assets v1 Contract

## Purpose

Define the portable ZIP format for moving completed selection and backtest artifacts between BlinkQuant installations or accounts.

## Format

The bundle contains `manifest.json`, plus either a selection `result.json` or persisted backtest result parts under `parts/`.

`manifest.json` uses format `blinkquant-artifact-v1` and records the artifact type, title, descriptive timestamps, portable metadata, summary, and an exact list of included files.

## Semantics

- Export is authenticated and restricted to the current user's artifact.
- Owner identifiers and internal task/strategy IDs are stripped from portable metadata.
- Import always creates a new completed task and Artifact under the authenticated user.
- Imported `source_task_id`, strategy template IDs, assigned node and other internal ownership fields are not trusted.
- Selection results are stored as JSON; backtest result parts remain Parquet files and optional `signal_trace.json`.
- ZIP members are limited to the declared v1 file set; duplicate members and manifest/file-list mismatches are rejected.
- Parquet payloads are schema-validated before registration.
- Import is rejected when the resulting artifact would exceed the user's artifact quota.
- Existing artifact and task APIs continue to enforce the authenticated user's ownership.

# ADR 0003: 14-day lookback window and an append-only raw layer

Date: 2026-10-17. Status: accepted.

## Context

NOAA revises recent observations: late reports arrive for several days and quality flags change. A run that only loaded "yesterday" would miss those revisions. Runs also fail and get retried, and backfills re-run past days, so the same data may be loaded more than once.

## Decision

- Every run for logical date D reads the window [D - 14 days, D] (never earlier than the pipeline start date) for the configured station prefix and writes it as one bronze Parquet file.
- The raw tables are append-only, stamped with `run_id` and `loaded_at`. Loading a `run_id` deletes that run's previous rows first, inside a transaction, so a retry or a re-backfill of a day leaves the table as if it had run once.
- Deduplication across runs (the same station-date-element loaded by several overlapping windows) happens in dbt staging: the row with the latest `loaded_at` wins. The incremental fact model merges on the natural key.

## Why

- Correctness without coordination: idempotent per run, order-independent across runs.
- Auditability: raw keeps every version NOAA published, so a changed value can be traced to the run that brought it.
- Cheap: a 14-day US window is about 660K rows and under 2 MB compressed.

## Consequences

- Raw grows by about 15x the daily increment (each day is re-read 15 times). At US scale this is roughly 10 million rows a month, acceptable for DuckDB and a 2X-Small warehouse; a retention job that drops superseded raw rows older than 60 days is planned if growth becomes a problem.
- Revisions older than 14 days are not picked up. The README states this; a monthly full-year reconcile is a possible extension.

# ADR 0002: DuckDB first, hosted warehouses as additional dbt targets

Date: 2026-10-17. Status: accepted.

## Context

Target jobs ask for Snowflake and Databricks experience. Snowflake's trial lasts 30 days; Databricks Free Edition is persistent but serverless-only with fair-use limits and may be deleted when inactive. CI must run on every push at no cost, and reviewers must be able to run the project on a laptop.

## Decision

DuckDB is the default target for local runs, tests and CI. Databricks Free Edition is the persistent hosted target. A Snowflake trial is proven as a third target during its 30-day window. The dbt project stays adapter-neutral: built-in incremental strategies and snapshots only, no warehouse-specific SQL outside small macros.

## Why

- The same models and tests run in three places; the hosted targets prove the project on real warehouses without making the project depend on them.
- CI stays free and fast (the DuckDB build on a fixture day takes seconds).
- When the Snowflake trial ends, nothing breaks: the profile remains in the repo with its measured credits-per-run, and Databricks keeps running.

## Consequences

- Raw loading differs per target (DuckDB reads the bronze file directly; Databricks and Snowflake need a staged upload). The Python loader has one implementation per target behind the same function signature.
- Adapter quirks (Databricks' Delta merge, Snowflake's case-folding) are handled in staging, with tests that run on all three.

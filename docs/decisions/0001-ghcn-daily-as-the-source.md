# ADR 0001: NOAA GHCN-Daily as the source dataset

Date: 2026-10-17. Status: accepted.

## Context

The pipeline needs a public dataset that updates daily, is large enough to need incremental logic, has a dimension whose attributes change over time, and has a licence that allows redistribution of derived tables and screenshots. Candidates: FAA/BTS on-time flights, City of Tampa open data, Florida tourism figures, NOAA weather.

## Decision

Use GHCN-Daily from the AWS Open Data mirror (`s3://noaa-ghcn-pds`), restricted to stations whose id starts with `US`.

## Why

- Published under CC0 1.0; NOAA asks only for a citation.
- Rewritten every night (the current-year partition's timestamp moves daily), so "N days of real runs" is a real measurement.
- Hive-partitioned Parquet by year and element lets DuckDB read a two-week window with range requests instead of downloading the year (measured: a 14-day US window is about 660K rows and under 2 seconds of extract time on a home connection).
- About 20 million US rows per year and 30K reporting stations: enough to need incremental merges and to make a free warehouse tier do real work.
- The station file changes over time (names, coordinates, elevation, network flags), which is a natural SCD Type 2 case.

## Alternatives rejected

- BTS on-time performance: monthly releases, so no daily cadence.
- City of Tampa portal: 27 datasets, licence text not findable, small volumes.
- Florida tourism: no open feed.

## Consequences

- Units arrive as integers in tenths (degrees C, millimetres); conversion happens in dbt staging.
- NOAA revises recent days, which forces the lookback design in ADR 0003.
- A Florida-focused mart keeps the local angle without restricting the pipeline to Florida.

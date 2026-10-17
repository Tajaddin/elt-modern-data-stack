# ADR 0004: Airflow 3 with Cosmos-rendered dbt tasks and a single-writer pool for DuckDB

Date: 2026-10-17. Status: accepted.

## Context

The pipeline needs a scheduler with retries, backfills, visible task history and alerting. The data jobs this project targets ask for Airflow by name (31% of postings) and dbt (27%). dbt has to run under Airflow in a way that keeps per-model visibility, and the local target is DuckDB, which allows only one writing process at a time.

## Decision

- Apache Airflow 3.3 in Docker Compose with the LocalExecutor, Postgres metadata database, api-server, scheduler, dag-processor and triggerer, and the simple auth manager with a password seeded from the environment.
- One DAG, `ghcn_daily`, scheduled at 07:00 UTC with `catchup=False`; backfills are explicit.
- dbt runs through astronomer-cosmos' `DbtTaskGroup`, so every model, snapshot and test is its own Airflow task with its own retries and logs, and the Airflow graph mirrors dbt's lineage.
- dbt lives in its own virtualenv inside the image (`/opt/dbt-venv`); Cosmos calls it by path. Airflow's dependency tree and dbt's never share a resolver.
- When the target is DuckDB, every Cosmos task is assigned to a one-slot pool (`duckdb_writer`), serialising the models. Databricks and Snowflake targets run Cosmos tasks in parallel.
- Metrics go out over StatsD to a statsd-exporter scraped by Prometheus; traces go over OpenTelemetry to a collector and Tempo; Grafana reads both. Alert rules live in the repo and have promtool unit tests.

## Why

- Cosmos over a single `dbt build` BashOperator: per-model retries and timings, and reviewers see the model graph in Airflow without opening dbt docs. The cost is more tasks per run (about 50), which the pool keeps orderly on DuckDB.
- A separate dbt venv avoids the classic "dbt pinned my Airflow" problem; both can upgrade independently.
- StatsD rather than scraping Airflow directly: Airflow 3 emits StatsD natively, the exporter mapping gives clean labels, and promtool unit tests make the alerting testable in CI without running anything.

## Consequences

- The pool makes the DuckDB build sequential (minutes for 17 tasks on the laptop versus 2 seconds for an in-process `dbt build`). Acceptable for a daily job; a `dbt build` fallback operator is a one-line change if that ever matters.
- Cosmos renders the DAG with its own parser (`LoadMode.CUSTOM`), so parsing never shells out to dbt or opens the DuckDB file; DAG integrity tests still run inside the image because they import Airflow and Cosmos.

## Amendments after the first runs (2026-10-17)

- **Airflow's own OpenTelemetry tracer stays off.** With `[traces] otel_on = True` the dag-processor's parsing child blocked for about 50 seconds per parse and was killed at the timeout, so the DAG went stale (0 DAGs, 1 error) while a manual `DagBag` parse of the same file took 2.5 seconds; switching the tracer off brought the processor back to 0.5 seconds. Traces now come from the extractor itself (`ghcn_elt.tracing`, OTLP/HTTP to the collector), which also carries the row counts the Airflow spans would not have had.
- **`airflow dags test` is not the end-to-end check.** It runs every task in one process; Cosmos' subprocess handling stalled there after dbt had already finished, and the extractor originally held the DuckDB file open across tasks. The end-to-end check (`scripts/e2e_airflow_run.py`) triggers a real run over the REST API and waits for the scheduler and executor, which is also what production does.
- **FAB auth manager instead of the simple one.** The simple auth manager keeps the password in a plaintext file and logs a warning that the deployment "looks like production". The FAB manager stores a hashed password in the metadata database; the init container creates or refreshes the admin user from `AIRFLOW_ADMIN_PASSWORD`.
- The helper module next to the DAG (`dags/common/`) was importable from the CLI but not from the dag-processor's child; alerts moved into the installed package.

"""ghcn_daily: extract the lookback window from NOAA, load raw, snapshot stations, build dbt, record metrics.

Runs once a day at 07:00 UTC (NOAA rewrites the bucket around 00:05 UTC). Backfills are done with
`airflow dags backfill` or the CLI; every run re-reads its own 14-day window, so reruns are safe.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior

from ghcn_elt.alerts import notify_failure

RAW_OBSERVATIONS = Asset("duckdb://raw/ghcn_observations")
RAW_STATIONS = Asset("duckdb://raw/ghcn_stations")
MARTS = Asset("duckdb://marts/fct_station_day")

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "duckdb")

# DuckDB allows one writing process per file, so on that target every task that touches the
# warehouse (the two Python loads and each Cosmos dbt task) runs through a one-slot pool.
WRITER_POOL = {"pool": "duckdb_writer"} if DBT_TARGET == "duckdb" else {}

default_args = {
    "owner": "tajaddin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


def _run_date(context: dict[str, Any]):  # type: ignore[no-untyped-def]
    """The day this run is for.

    The end of the data interval (today, for the 07:00 schedule); the current time for manual runs.
    """
    end = context.get("data_interval_end") or context.get("logical_date") or datetime.now(UTC)
    return end.date()


@dag(
    dag_id="ghcn_daily",
    schedule="0 7 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ghcn", "elt", "dbt"],
    doc_md=__doc__,
    dagrun_timeout=timedelta(hours=2),
)
def ghcn_daily() -> None:
    @task(outlets=[RAW_OBSERVATIONS], **WRITER_POOL)
    def extract_and_load(**context: Any) -> dict[str, Any]:
        from airflow.stats import Stats

        from ghcn_elt.cli import run_extract, run_load
        from ghcn_elt.config import get_settings
        from ghcn_elt.logging import configure_logging

        configure_logging()
        settings = get_settings()
        run_date = _run_date(context)
        t0 = time.perf_counter()
        result = run_extract(settings, run_date)
        rows_loaded, load_seconds = run_load(settings, run_date, result.parquet_path)
        Stats.gauge("ghcn.rows_extracted", result.rows)
        Stats.gauge("ghcn.rows_loaded", rows_loaded)
        Stats.timing("ghcn.extract_seconds", result.seconds * 1000)
        return {
            "run_date": str(run_date),
            "window_start": str(result.window.start),
            "window_end": str(result.window.end),
            "rows_extracted": result.rows,
            "rows_loaded": rows_loaded,
            "bytes_bronze": result.bytes_written,
            "extract_seconds": result.seconds,
            "load_seconds": load_seconds,
            "elapsed": round(time.perf_counter() - t0, 2),
        }

    @task(outlets=[RAW_STATIONS], **WRITER_POOL)
    def snapshot_stations(**context: Any) -> int:
        from ghcn_elt.cli import run_stations
        from ghcn_elt.config import get_settings
        from ghcn_elt.logging import configure_logging

        configure_logging()
        return run_stations(get_settings(), _run_date(context))

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="ghcn_warehouse",
            target_name=DBT_TARGET,
            profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        # CUSTOM parses the dbt project files directly, so DAG parsing never opens the DuckDB file
        # (a running task may hold its write lock) and never shells out to dbt.
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH, load_method=LoadMode.CUSTOM),
        # DuckDB accepts one writing process at a time; the pool serialises Cosmos' per-model tasks.
        operator_args=dict(WRITER_POOL),
        default_args={"retries": 1},
    )

    @task(outlets=[MARTS])
    def record_run(load_stats: dict[str, Any], n_stations: int, **context: Any) -> None:
        from ghcn_elt.config import get_settings
        from ghcn_elt.metrics import RunMetrics, append_metrics

        settings = get_settings()
        run_date = load_stats["run_date"]
        append_metrics(
            settings.metrics_path,
            RunMetrics(
                run_id=f"ghcn_daily__{run_date}",
                run_date=run_date,
                window_start=load_stats["window_start"],
                window_end=load_stats["window_end"],
                station_prefix=settings.station_prefix,
                rows_extracted=load_stats["rows_extracted"],
                rows_loaded=load_stats["rows_loaded"],
                bytes_bronze=load_stats["bytes_bronze"],
                extract_seconds=load_stats["extract_seconds"],
                load_seconds=load_stats["load_seconds"],
                total_seconds=load_stats["elapsed"],
                stations_loaded=n_stations,
            ),
        )

    loaded = extract_and_load()
    stations = snapshot_stations()
    [loaded, stations] >> dbt_build >> record_run(loaded, stations)


ghcn_daily()

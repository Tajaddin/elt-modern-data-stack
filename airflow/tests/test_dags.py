"""DAG integrity tests. They import the DAG folder the way the dag-processor does, so they run
inside the Airflow image:  docker compose run --rm airflow-cli python -m pytest /opt/airflow/dag_tests -q
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_ghcn_daily_shape(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("ghcn_daily")
    assert dag is not None
    assert dag.timetable is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    task_ids = set(dag.task_ids)
    assert {"extract_and_load", "snapshot_stations", "record_run"} <= task_ids
    assert any(t.startswith("dbt_build.") for t in task_ids), "Cosmos must render dbt models as tasks"
    # dbt runs after both loads, and the run record after dbt
    rec = dag.get_task("record_run")
    assert any(u.startswith("dbt_build.") for u in rec.upstream_task_ids)
    for t in dag.tasks:
        assert t.retries >= 1, f"{t.task_id} has no retries"
        assert t.on_failure_callback is not None or t.task_id.startswith("dbt_build."), t.task_id


def test_duckdb_tasks_share_the_single_writer_pool(dagbag) -> None:  # type: ignore[no-untyped-def]
    if os.environ.get("DBT_TARGET", "duckdb") != "duckdb":
        pytest.skip("pool only applies to the DuckDB target")
    dag = dagbag.get_dag("ghcn_daily")
    writers = [t for t in dag.tasks if t.task_id.startswith("dbt_build.")]
    writers += [dag.get_task("extract_and_load"), dag.get_task("snapshot_stations")]
    assert len(writers) >= 3
    assert all(t.pool == "duckdb_writer" for t in writers), [(t.task_id, t.pool) for t in writers]
    assert dag.get_task("record_run").pool != "duckdb_writer"


def test_failure_message_formats_without_airflow_objects() -> None:
    from ghcn_elt.alerts import build_failure_message

    class TI:
        dag_id = "ghcn_daily"
        task_id = "extract_and_load"
        try_number = 2
        log_url = "http://airflow/log"

    msg = build_failure_message(
        {"task_instance": TI(), "run_id": "manual__1", "exception": ValueError("boom")}
    )
    assert "ghcn_daily.extract_and_load" in msg
    assert "ValueError: boom" in msg
    assert "http://airflow/log" in msg

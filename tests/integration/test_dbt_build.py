"""Build the dbt project twice against a DuckDB file loaded from the committed fixture.

Checks: every model and test passes, a second build leaves the marts identical (idempotent),
overlapping runs are de-duplicated in staging, and the snapshot opens a new version when a
station attribute changes.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from ghcn_elt.load import load_observations, load_stations, record_load_run, stations_to_arrow
from ghcn_elt.sources import Station

REPO = Path(__file__).resolve().parents[2]
DBT_DIR = REPO / "dbt"
FIXTURE = REPO / "tests" / "fixtures" / "observations_sample.parquet"

pytestmark = pytest.mark.slow


def _stations_for(con: duckdb.DuckDBPyConnection, name_suffix: str = "") -> list[Station]:
    ids = [
        r[0]
        for r in con.execute("SELECT DISTINCT station_id FROM raw.ghcn_observations ORDER BY 1").fetchall()
    ]
    return [
        Station(
            station_id=i,
            latitude=27.0 + k * 0.01,
            longitude=-81.0 - k * 0.01,
            elevation_m=10.0 + k,
            state="FL",
            name=f"STATION {k}{name_suffix}",
            gsn_flag=False,
            hcn_crn_flag=None,
            wmo_id=None,
        )
        for k, i in enumerate(ids)
    ]


def _dbt(db_path: Path, *args: str) -> None:
    env = {"DBT_TARGET": "duckdb", "GHCN_DUCKDB_PATH": str(db_path), "DBT_PROFILES_DIR": str(DBT_DIR)}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        res = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    assert res.success, f"dbt {' '.join(args)} failed: {res.exception}"
    failed = [r for r in getattr(res.result, "results", []) if r.status not in ("success", "pass")]
    assert not failed, [(r.node.name, r.status, r.message) for r in failed]


def _counts(db_path: Path) -> dict[str, int]:
    con = duckdb.connect(str(db_path))
    try:
        out = {}
        for t in [
            "main_marts.fct_station_day",
            "main_marts.dim_station",
            "main_marts.mart_florida_weather",
            "main_marts.mart_station_coverage",
            "main_marts.mart_daily_state_summary",
        ]:
            out[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        return out
    finally:
        con.close()


@pytest.mark.skipif(not FIXTURE.exists(), reason="fixture not generated")
def test_dbt_build_is_green_and_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(db))
    # Two overlapping runs load the same two days: staging must keep one version per key.
    n = load_observations(con, FIXTURE, "ghcn_daily__2026-10-02")
    load_observations(con, FIXTURE, "ghcn_daily__2026-10-03")
    record_load_run(
        con,
        run_id="ghcn_daily__2026-10-02",
        run_date=date(2026, 10, 2),
        window_start=date(2026, 9, 18),
        window_end=date(2026, 10, 2),
        rows_loaded=n,
    )
    record_load_run(
        con,
        run_id="ghcn_daily__2026-10-03",
        run_date=date(2026, 10, 3),
        window_start=date(2026, 9, 19),
        window_end=date(2026, 10, 3),
        rows_loaded=n,
    )
    stations = _stations_for(con)
    load_stations(con, stations_to_arrow(stations), "ghcn_daily__2026-10-03")
    con.close()

    _dbt(db, "build")
    first = _counts(db)
    assert first["main_marts.fct_station_day"] > 0
    assert first["main_marts.dim_station"] == len(stations)
    assert first["main_marts.mart_florida_weather"] == first["main_marts.fct_station_day"]

    con = duckdb.connect(str(db))
    raw_rows = con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0]
    stg_rows = con.execute("SELECT count(*) FROM main_staging.stg_ghcn__observations").fetchone()[0]
    assert raw_rows == 2 * stg_rows, "staging must collapse the two overlapping runs to one version per key"
    units = dict(
        con.execute(
            "SELECT element, max(unit) FROM main_staging.stg_ghcn__observations GROUP BY 1"
        ).fetchall()
    )
    assert units.get("PRCP") == "mm"
    con.close()

    # Second build: incremental merge and snapshot leave everything unchanged.
    _dbt(db, "build")
    assert _counts(db) == first

    # A station changes name: the snapshot opens a new version and dim_station gains one row.
    con = duckdb.connect(str(db))
    changed = _stations_for(con, name_suffix=" RENAMED")
    load_stations(con, stations_to_arrow(changed[:1] + stations[1:]), "ghcn_daily__2026-10-04")
    con.close()
    _dbt(db, "build")
    third = _counts(db)
    assert third["main_marts.dim_station"] == first["main_marts.dim_station"] + 1
    con = duckdb.connect(str(db))
    versions = con.execute(
        "SELECT is_current, station_name FROM main_marts.dim_station WHERE station_id = ? ORDER BY valid_from",
        [stations[0].station_id],
    ).fetchall()
    assert [v[0] for v in versions] == [False, True]
    assert versions[1][1].endswith("RENAMED")
    con.close()

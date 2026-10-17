"""Load: append bronze files into the warehouse's raw schema.

The raw layer is append-only with run metadata. Re-running a run_id first deletes that run's rows,
so a retry or a backfill of the same day leaves the table exactly as a single run would.
Deduplication across runs (NOAA revises recent days) is dbt's job in staging.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from ghcn_elt.sources import Station

OBSERVATIONS_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ghcn_observations (
    station_id VARCHAR NOT NULL,
    obs_date   DATE    NOT NULL,
    element    VARCHAR NOT NULL,
    data_value INTEGER,
    m_flag     VARCHAR,
    q_flag     VARCHAR,
    s_flag     VARCHAR,
    obs_time   VARCHAR,
    run_id     VARCHAR NOT NULL,
    loaded_at  TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.ghcn_stations (
    station_id   VARCHAR NOT NULL,
    latitude     DOUBLE,
    longitude    DOUBLE,
    elevation_m  DOUBLE,
    state        VARCHAR,
    name         VARCHAR,
    gsn_flag     BOOLEAN,
    hcn_crn_flag VARCHAR,
    wmo_id       VARCHAR,
    run_id       VARCHAR NOT NULL,
    loaded_at    TIMESTAMP NOT NULL
);
"""


def ensure_raw_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(OBSERVATIONS_DDL)


def load_observations(con: duckdb.DuckDBPyConnection, parquet_path: Path, run_id: str) -> int:
    """Replace this run's rows with the bronze file's content. Returns rows loaded."""
    ensure_raw_schema(con)
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM raw.ghcn_observations WHERE run_id = ?", [run_id])
        con.execute(
            f"""
            INSERT INTO raw.ghcn_observations
            SELECT station_id, obs_date, element, data_value, m_flag, q_flag, s_flag, obs_time, ?, ?
            FROM read_parquet('{parquet_path.as_posix()}')
            """,
            [run_id, loaded_at],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    row = con.execute("SELECT count(*) FROM raw.ghcn_observations WHERE run_id = ?", [run_id]).fetchone()
    assert row is not None
    return int(row[0])


def stations_to_arrow(stations: list[Station]) -> pa.Table:
    return pa.table(
        {
            "station_id": [s.station_id for s in stations],
            "latitude": [s.latitude for s in stations],
            "longitude": [s.longitude for s in stations],
            "elevation_m": [s.elevation_m for s in stations],
            "state": [s.state for s in stations],
            "name": [s.name for s in stations],
            "gsn_flag": [s.gsn_flag for s in stations],
            "hcn_crn_flag": [s.hcn_crn_flag for s in stations],
            "wmo_id": [s.wmo_id for s in stations],
        }
    )


def load_stations(con: duckdb.DuckDBPyConnection, stations: pa.Table, run_id: str) -> int:
    """Replace this run's station snapshot. dbt's snapshot turns these into SCD Type 2 rows."""
    ensure_raw_schema(con)
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    con.register("_stations_in", stations)
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM raw.ghcn_stations WHERE run_id = ?", [run_id])
        con.execute(
            """
            INSERT INTO raw.ghcn_stations
            SELECT station_id, latitude, longitude, elevation_m, state, name,
                   gsn_flag, hcn_crn_flag, wmo_id, ?, ?
            FROM _stations_in
            """,
            [run_id, loaded_at],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_stations_in")
    row = con.execute("SELECT count(*) FROM raw.ghcn_stations WHERE run_id = ?", [run_id]).fetchone()
    assert row is not None
    return int(row[0])

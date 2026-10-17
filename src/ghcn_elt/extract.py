"""Extract: read the lookback window from NOAA's Parquet partitions with DuckDB and write bronze files.

DuckDB's httpfs extension does range reads with predicate pushdown, so a 14-day window over the
current year costs a few hundred megabytes of transfer rather than the whole year.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from ghcn_elt.sources import Window, observation_globs


@dataclass(frozen=True)
class ExtractResult:
    parquet_path: Path
    rows: int
    bytes_written: int
    seconds: float
    window: Window


def connect(region: str = "us-east-1", database: str | Path = ":memory:") -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with anonymous S3 access configured."""
    con = duckdb.connect(str(database))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{region}'; SET s3_url_style='path';")
    con.execute("SET s3_access_key_id=''; SET s3_secret_access_key='';")
    return con


def observations_query(globs: list[str], window: Window, station_prefix: str) -> str:
    """SQL that selects the window for the station prefix and types the columns.

    DATE arrives as 'YYYYMMDD' text and DATA_VALUE as an integer in NOAA's units
    (tenths of a degree Celsius, tenths of a millimetre); unit conversion is dbt's job.
    """
    files = ", ".join(f"'{g}'" for g in globs)
    return f"""
        SELECT
            "ID"          AS station_id,
            strptime("DATE", '%Y%m%d')::DATE AS obs_date,
            "ELEMENT"     AS element,
            "DATA_VALUE"::INTEGER AS data_value,
            NULLIF("M_FLAG", '') AS m_flag,
            NULLIF("Q_FLAG", '') AS q_flag,
            NULLIF("S_FLAG", '') AS s_flag,
            NULLIF("OBS_TIME", '') AS obs_time
        FROM read_parquet([{files}], hive_partitioning = true)
        WHERE "ID" LIKE '{station_prefix}%'
          AND "DATE" >= '{window.start:%Y%m%d}'
          AND "DATE" <= '{window.end:%Y%m%d}'
    """


def bronze_path(bronze_dir: Path, dataset: str, run_date: date) -> Path:
    return bronze_dir / dataset / f"run_date={run_date:%Y-%m-%d}" / f"{dataset}.parquet"


def extract_observations(
    con: duckdb.DuckDBPyConnection,
    *,
    bucket_url: str,
    window: Window,
    station_prefix: str,
    bronze_dir: Path,
    run_date: date,
) -> ExtractResult:
    """Write one Parquet file holding every observation in the window for the station prefix."""
    out = bronze_path(bronze_dir, "observations", run_date)
    out.parent.mkdir(parents=True, exist_ok=True)
    query = observations_query(observation_globs(bucket_url, window.years), window, station_prefix)
    t0 = time.perf_counter()
    con.execute(f"COPY ({query}) TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}')").fetchone()
    assert rows is not None
    return ExtractResult(
        parquet_path=out,
        rows=int(rows[0]),
        bytes_written=out.stat().st_size,
        seconds=round(time.perf_counter() - t0, 2),
        window=window,
    )


def count_observations(
    con: duckdb.DuckDBPyConnection, *, bucket_url: str, window: Window, station_prefix: str
) -> int:
    """How many rows a run would process, without writing anything."""
    query = observations_query(observation_globs(bucket_url, window.years), window, station_prefix)
    row = con.execute(f"SELECT count(*) FROM ({query})").fetchone()
    assert row is not None
    return int(row[0])

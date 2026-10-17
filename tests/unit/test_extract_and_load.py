"""Extract SQL shape, bronze paths, and the raw loader's idempotency, all against a local fixture."""

from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ghcn_elt.extract import bronze_path, observations_query
from ghcn_elt.load import ensure_raw_schema, load_observations, load_stations, stations_to_arrow
from ghcn_elt.sources import Window, parse_station_line

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "observations_sample.parquet"


def test_observations_query_filters_prefix_and_window() -> None:
    sql = observations_query(
        ["s3://b/parquet/by_year/YEAR=2026/*/*.parquet"], Window(date(2026, 10, 3), date(2026, 10, 17)), "US"
    )
    assert "\"ID\" LIKE 'US%'" in sql
    assert "\"DATE\" >= '20261003'" in sql
    assert "\"DATE\" <= '20261017'" in sql
    assert "hive_partitioning = true" in sql


def test_bronze_path_is_partitioned_by_run_date(tmp_path: Path) -> None:
    p = bronze_path(tmp_path, "observations", date(2026, 10, 17))
    assert p == tmp_path / "observations" / "run_date=2026-10-17" / "observations.parquet"


def test_observations_query_runs_against_local_parquet_in_noaa_layout(tmp_path: Path) -> None:
    # Build a tiny file in NOAA's raw column layout (hive partition supplies ELEMENT and YEAR).
    part = tmp_path / "parquet" / "by_year" / "YEAR=2026" / "ELEMENT=TMAX"
    part.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "ID": ["USW00012842", "USW00012842", "CA001010066"],
                "DATE": ["20261010", "20261001", "20261010"],
                "DATA_VALUE": [311, 300, 150],
                "M_FLAG": ["", "", ""],
                "Q_FLAG": ["", "", ""],
                "S_FLAG": ["W", "W", "C"],
                "OBS_TIME": ["2400", "2400", ""],
            }
        ),
        part / "x.parquet",
    )
    glob = (tmp_path / "parquet" / "by_year" / "YEAR=2026" / "*" / "*.parquet").as_posix()
    con = duckdb.connect()
    rows = con.execute(
        observations_query([glob], Window(date(2026, 10, 3), date(2026, 10, 17)), "US")
    ).fetchall()
    assert rows == [("USW00012842", date(2026, 10, 10), "TMAX", 311, None, None, "W", "2400")]


@pytest.mark.skipif(not FIXTURE.exists(), reason="fixture not generated yet")
def test_load_observations_is_idempotent_per_run_id(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    n1 = load_observations(con, FIXTURE, "ghcn_daily__2026-10-17")
    n2 = load_observations(con, FIXTURE, "ghcn_daily__2026-10-17")
    total = con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0]
    assert n1 == n2 == total > 0
    n3 = load_observations(con, FIXTURE, "ghcn_daily__2026-10-18")
    assert con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0] == n1 + n3
    runs = con.execute("SELECT count(DISTINCT run_id) FROM raw.ghcn_observations").fetchone()[0]
    assert runs == 2


def test_load_observations_rolls_back_on_bad_file(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    ensure_raw_schema(con)
    bad = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"unexpected": [1]}), bad)
    with pytest.raises(duckdb.Error):
        load_observations(con, bad, "r1")
    assert con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0] == 0


def test_load_stations_snapshot_replaces_same_run(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    s = parse_station_line(
        "USW00012842  27.9633  -82.5400    1.8 FL TAMPA INTL AP                  GSN     72211"
    )
    table = stations_to_arrow([s])
    assert load_stations(con, table, "r1") == 1
    assert load_stations(con, table, "r1") == 1
    assert load_stations(con, table, "r2") == 1
    assert con.execute("SELECT count(*) FROM raw.ghcn_stations").fetchone()[0] == 2
    row = con.execute("SELECT name, state, elevation_m, wmo_id FROM raw.ghcn_stations LIMIT 1").fetchone()
    assert row == ("TAMPA INTL AP", "FL", 1.8, "72211")

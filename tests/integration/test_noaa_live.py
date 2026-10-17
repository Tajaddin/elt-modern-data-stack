"""Talks to NOAA's public bucket. Run with: pytest -m network"""

from datetime import date
from pathlib import Path

import pytest

from ghcn_elt.extract import connect, count_observations, extract_observations
from ghcn_elt.load import load_observations
from ghcn_elt.sources import Window

pytestmark = [pytest.mark.network, pytest.mark.slow]

BUCKET = "s3://noaa-ghcn-pds"


def test_two_day_window_for_florida_cocorahs_stations_extracts_and_loads(tmp_path: Path) -> None:
    con = connect()
    window = Window(date(2026, 10, 1), date(2026, 10, 2))
    expected = count_observations(con, bucket_url=BUCKET, window=window, station_prefix="US1FL")
    assert expected > 0
    result = extract_observations(
        con,
        bucket_url=BUCKET,
        window=window,
        station_prefix="US1FL",
        bronze_dir=tmp_path,
        run_date=window.end,
    )
    assert result.rows == expected
    assert result.parquet_path.exists()
    wh = connect(database=tmp_path / "w.duckdb")
    assert load_observations(wh, result.parquet_path, "it-run") == expected
    dates = wh.execute("SELECT min(obs_date), max(obs_date) FROM raw.ghcn_observations").fetchone()
    assert dates == (date(2026, 10, 1), date(2026, 10, 2))
    prefixes = wh.execute("SELECT DISTINCT left(station_id, 5) FROM raw.ghcn_observations").fetchall()
    assert prefixes == [("US1FL",)]

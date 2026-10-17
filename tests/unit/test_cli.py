"""End-to-end run of the CLI against a local copy of NOAA's layout: no network, real DuckDB, real files."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from ghcn_elt import cli
from ghcn_elt.config import Settings

TAMPA = "USW00012842  27.9633  -82.5400    1.8 FL TAMPA INTL AP                  GSN     72211"
CANADA = "CA001010066  48.8667 -123.2833    4.0    ACTIVE PASS                            71030"


def _write_noaa_layout(root: Path) -> None:
    """Two element partitions for 2026 with US and non-US rows across several days."""
    rows = {
        "TMAX": [
            ("USW00012842", "20261001", 311),
            ("USW00012842", "20261010", 300),
            ("CA001010066", "20261010", 150),
        ],
        "PRCP": [
            ("USW00012842", "20261010", 25),
            ("US1FLHL0004", "20261009", 0),
            ("USW00012842", "20260901", 10),
        ],
    }
    for element, recs in rows.items():
        part = root / "parquet" / "by_year" / "YEAR=2026" / f"ELEMENT={element}"
        part.mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "ID": [r[0] for r in recs],
                    "DATE": [r[1] for r in recs],
                    "DATA_VALUE": [r[2] for r in recs],
                    "M_FLAG": ["" for _ in recs],
                    "Q_FLAG": ["" for _ in recs],
                    "S_FLAG": ["W" for _ in recs],
                    "OBS_TIME": ["" for _ in recs],
                }
            ),
            part / "part-0.parquet",
        )


@pytest.fixture()
def local_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    bucket = tmp_path / "bucket"
    _write_noaa_layout(bucket)
    settings = Settings(
        bucket_url=bucket.as_posix(),
        http_base_url="https://stations.invalid",
        station_prefix="US",
        lookback_days=14,
        start_date=date(2024, 1, 1),
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "data" / "w.duckdb",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)

    def fake_get(url: str, **_: object) -> httpx.Response:
        assert url == "https://stations.invalid/ghcnd-stations.txt"
        return httpx.Response(200, text="\n".join([TAMPA, CANADA]) + "\n", request=httpx.Request("GET", url))

    monkeypatch.setattr(cli.httpx, "get", fake_get)
    return settings


def test_run_loads_window_records_metrics_and_is_rerunnable(local_settings: Settings) -> None:
    runner = CliRunner()
    result = runner.invoke(cli.app, ["run", "--run-date", "2026-10-10"])
    assert result.exit_code == 0, result.output

    con = duckdb.connect(str(local_settings.duckdb_path), read_only=True)
    obs = con.execute(
        "SELECT station_id, obs_date, element, data_value FROM raw.ghcn_observations ORDER BY 1, 2, 3"
    ).fetchall()
    # Window is 2026-09-26..2026-10-10: the 2026-09-01 row and the Canadian row are excluded.
    assert obs == [
        ("US1FLHL0004", date(2026, 10, 9), "PRCP", 0),
        ("USW00012842", date(2026, 10, 1), "TMAX", 311),
        ("USW00012842", date(2026, 10, 10), "PRCP", 25),
        ("USW00012842", date(2026, 10, 10), "TMAX", 300),
    ]
    stations = con.execute("SELECT station_id, name, gsn_flag FROM raw.ghcn_stations").fetchall()
    assert stations == [("USW00012842", "TAMPA INTL AP", True)]
    con.close()

    lines = local_settings.metrics_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    m = json.loads(lines[0])
    assert m["run_id"] == "ghcn_daily__2026-10-10"
    assert m["window_start"] == "2026-09-26"
    assert m["rows_extracted"] == m["rows_loaded"] == 4
    assert m["stations_loaded"] == 1
    assert m["total_seconds"] >= m["extract_seconds"]

    # A second run of the same date replaces, never duplicates.
    again = runner.invoke(cli.app, ["run", "--run-date", "2026-10-10"])
    assert again.exit_code == 0, again.output
    con = duckdb.connect(str(local_settings.duckdb_path), read_only=True)
    assert con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0] == 4
    assert con.execute("SELECT count(*) FROM raw.ghcn_stations").fetchone()[0] == 1
    assert len(local_settings.metrics_path.read_text(encoding="utf-8").strip().splitlines()) == 2


def test_measure_counts_without_writing(local_settings: Settings) -> None:
    result = CliRunner().invoke(cli.app, ["measure", "--run-date", "2026-10-10"])
    assert result.exit_code == 0, result.output
    assert '"rows": 4' in result.output
    assert not local_settings.duckdb_path.exists()
    assert not local_settings.bronze_dir.exists()


def test_extract_then_load_as_separate_commands(local_settings: Settings) -> None:
    runner = CliRunner()
    assert runner.invoke(cli.app, ["extract", "--run-date", "2026-10-10"]).exit_code == 0
    bronze = local_settings.bronze_dir / "observations" / "run_date=2026-10-10" / "observations.parquet"
    assert bronze.exists()
    assert runner.invoke(cli.app, ["load", "--run-date", "2026-10-10"]).exit_code == 0
    con = duckdb.connect(str(local_settings.duckdb_path), read_only=True)
    assert con.execute("SELECT count(*) FROM raw.ghcn_observations").fetchone()[0] == 4


def test_run_before_start_date_fails_cleanly(local_settings: Settings) -> None:
    result = CliRunner().invoke(cli.app, ["measure", "--run-date", "2023-01-01"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)

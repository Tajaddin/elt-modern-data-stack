"""Command line entry points. Airflow calls the same functions; the CLI is for humans and CI."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import httpx
import pyarrow.parquet as pq
import typer

from ghcn_elt import extract as ex
from ghcn_elt import load as ld
from ghcn_elt.config import Settings, get_settings
from ghcn_elt.logging import configure_logging, get_logger
from ghcn_elt.metrics import RunMetrics, append_metrics
from ghcn_elt.sources import lookback_window, parse_stations, stations_url

app = typer.Typer(no_args_is_help=True, add_completion=False, help="GHCN-Daily ELT: extract, load, measure.")
log = get_logger("ghcn_elt")

RunDate = Annotated[
    str | None,
    typer.Option("--run-date", help="Logical date YYYY-MM-DD (default: today, UTC)."),
]


def _run_date(value: str | None) -> date:
    return date.fromisoformat(value) if value else datetime.now(UTC).date()


def _run_id(run_date: date) -> str:
    return f"ghcn_daily__{run_date:%Y-%m-%d}"


@app.callback()
def _setup(json_logs: bool = typer.Option(True, help="JSON logs (default) or console logs.")) -> None:
    configure_logging(json_output=json_logs)


def run_extract(settings: Settings, run_date: date) -> ex.ExtractResult:
    window = lookback_window(run_date, settings.lookback_days, settings.start_date)
    con = ex.connect(settings.s3_region)
    result = ex.extract_observations(
        con,
        bucket_url=settings.bucket_url,
        window=window,
        station_prefix=settings.station_prefix,
        bronze_dir=settings.bronze_dir,
        run_date=run_date,
    )
    log.info(
        "extract.done",
        run_date=str(run_date),
        window_start=str(window.start),
        window_end=str(window.end),
        rows=result.rows,
        bytes=result.bytes_written,
        seconds=result.seconds,
        path=str(result.parquet_path),
    )
    return result


def run_load(settings: Settings, run_date: date, parquet_path: Path) -> tuple[int, float]:
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = ex.connect(settings.s3_region, settings.duckdb_path)
    t0 = time.perf_counter()
    rows = ld.load_observations(con, parquet_path, _run_id(run_date))
    seconds = round(time.perf_counter() - t0, 2)
    log.info("load.done", run_date=str(run_date), rows=rows, seconds=seconds, db=str(settings.duckdb_path))
    return rows, seconds


def run_stations(settings: Settings, run_date: date) -> int:
    url = stations_url(settings.http_base_url)
    t0 = time.perf_counter()
    text = httpx.get(url, timeout=120.0, follow_redirects=True).raise_for_status().text
    stations = parse_stations(text, prefix=settings.station_prefix)
    table = ld.stations_to_arrow(stations)
    out = ex.bronze_path(settings.bronze_dir, "stations", run_date)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    con = ex.connect(settings.s3_region, settings.duckdb_path)
    rows = ld.load_stations(con, table, _run_id(run_date))
    log.info("stations.done", run_date=str(run_date), rows=rows, seconds=round(time.perf_counter() - t0, 2))
    return rows


@app.command()
def extract(run_date: RunDate = None) -> None:
    """Read the lookback window from NOAA and write the bronze Parquet file."""
    run_extract(get_settings(), _run_date(run_date))


@app.command()
def load(run_date: RunDate = None) -> None:
    """Load an already extracted bronze file into the raw schema."""
    settings = get_settings()
    rd = _run_date(run_date)
    run_load(settings, rd, ex.bronze_path(settings.bronze_dir, "observations", rd))


@app.command()
def stations(run_date: RunDate = None) -> None:
    """Download the station metadata file and load a snapshot of it."""
    run_stations(get_settings(), _run_date(run_date))


@app.command()
def run(run_date: RunDate = None) -> None:
    """Extract, load and snapshot stations for one logical date, and record the run's metrics."""
    settings = get_settings()
    rd = _run_date(run_date)
    t0 = time.perf_counter()
    result = run_extract(settings, rd)
    rows_loaded, load_seconds = run_load(settings, rd, result.parquet_path)
    n_stations = run_stations(settings, rd)
    metrics = RunMetrics(
        run_id=_run_id(rd),
        run_date=str(rd),
        window_start=str(result.window.start),
        window_end=str(result.window.end),
        station_prefix=settings.station_prefix,
        rows_extracted=result.rows,
        rows_loaded=rows_loaded,
        bytes_bronze=result.bytes_written,
        extract_seconds=result.seconds,
        load_seconds=load_seconds,
        total_seconds=round(time.perf_counter() - t0, 2),
        stations_loaded=n_stations,
    )
    append_metrics(settings.metrics_path, metrics)
    log.info("run.done", **{k: v for k, v in metrics.__dict__.items()})


@app.command()
def measure(run_date: RunDate = None) -> None:
    """Count the rows a run would process without writing anything."""
    settings = get_settings()
    rd = _run_date(run_date)
    window = lookback_window(rd, settings.lookback_days, settings.start_date)
    con = ex.connect(settings.s3_region)
    t0 = time.perf_counter()
    n = ex.count_observations(
        con, bucket_url=settings.bucket_url, window=window, station_prefix=settings.station_prefix
    )
    log.info(
        "measure.done",
        run_date=str(rd),
        window_start=str(window.start),
        window_end=str(window.end),
        rows=n,
        seconds=round(time.perf_counter() - t0, 2),
    )


if __name__ == "__main__":
    app()

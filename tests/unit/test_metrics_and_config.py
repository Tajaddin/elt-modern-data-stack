from datetime import date
from pathlib import Path

import pytest

from ghcn_elt.config import Settings
from ghcn_elt.metrics import RunMetrics, append_metrics, read_metrics


def _metrics(run_date: str) -> RunMetrics:
    return RunMetrics(
        run_id=f"ghcn_daily__{run_date}",
        run_date=run_date,
        window_start="2026-10-03",
        window_end=run_date,
        station_prefix="US",
        rows_extracted=10,
        rows_loaded=10,
        bytes_bronze=1234,
        extract_seconds=1.5,
        load_seconds=0.2,
        total_seconds=2.0,
        stations_loaded=5,
    )


def test_metrics_round_trip_appends_lines(tmp_path: Path) -> None:
    path = tmp_path / "m" / "runs.jsonl"
    assert read_metrics(path) == []
    append_metrics(path, _metrics("2026-10-17"))
    append_metrics(path, _metrics("2026-10-18"))
    rows = read_metrics(path)
    assert [r["run_id"] for r in rows] == ["ghcn_daily__2026-10-17", "ghcn_daily__2026-10-18"]
    assert rows[0]["recorded_at"].endswith("+00:00")


def test_settings_defaults_and_env_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no .env file here
    s = Settings()
    assert s.station_prefix == "US"
    assert s.lookback_days == 14
    assert s.start_date == date(2024, 1, 1)
    assert s.bronze_dir == Path("data") / "bronze"
    assert s.metrics_path == Path("data") / "metrics" / "extract_runs.jsonl"

    monkeypatch.setenv("GHCN_STATION_PREFIX", "US1FL")
    monkeypatch.setenv("GHCN_LOOKBACK_DAYS", "3")
    monkeypatch.setenv("GHCN_START_DATE", "2025-06-01")
    monkeypatch.setenv("GHCN_DATA_DIR", "elsewhere")
    s2 = Settings()
    assert (s2.station_prefix, s2.lookback_days, s2.start_date) == ("US1FL", 3, date(2025, 6, 1))
    assert s2.bronze_dir == Path("elsewhere") / "bronze"

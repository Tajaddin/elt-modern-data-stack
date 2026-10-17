"""Runtime settings. Every value can be overridden with a GHCN_* environment variable or a .env file."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GHCN_", env_file=".env", extra="ignore")

    # NOAA GHCN-Daily on the AWS Open Data registry. Anonymous access, CC0.
    bucket_url: str = "s3://noaa-ghcn-pds"
    http_base_url: str = "https://noaa-ghcn-pds.s3.amazonaws.com"
    s3_region: str = "us-east-1"

    # Which stations to keep. GHCN ids start with a two-letter country code.
    station_prefix: str = "US"

    # NOAA revises recent days, so every run re-reads this many days and the raw layer
    # keeps the latest copy. Backfills start at start_date.
    lookback_days: int = 14
    start_date: date = date(2024, 1, 1)

    data_dir: Path = Path("data")
    duckdb_path: Path = Path("data/warehouse.duckdb")

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def metrics_path(self) -> Path:
        return self.data_dir / "metrics" / "extract_runs.jsonl"


def get_settings() -> Settings:
    return Settings()

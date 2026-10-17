"""Where the data lives and how its reference files are laid out.

GHCN-Daily publishes observations as Hive-partitioned Parquet (YEAR=.../ELEMENT=.../*.parquet)
and station metadata as a fixed-width text file. Both layouts are documented in NOAA's readme.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Window:
    """Inclusive date range one run is responsible for."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"window start {self.start} is after end {self.end}")

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def years(self) -> list[int]:
        return list(range(self.start.year, self.end.year + 1))


def lookback_window(run_date: date, lookback_days: int, start_date: date) -> Window:
    """The run for `run_date` re-reads the previous `lookback_days` days, never before `start_date`."""
    if run_date < start_date:
        raise ValueError(f"run date {run_date} is before the pipeline start date {start_date}")
    start = max(run_date - timedelta(days=lookback_days), start_date)
    return Window(start=start, end=run_date)


def observation_globs(bucket_url: str, years: list[int]) -> list[str]:
    """One glob per year partition; the element partition sits one level below."""
    return [f"{bucket_url.rstrip('/')}/parquet/by_year/YEAR={y}/*/*.parquet" for y in years]


def stations_url(http_base_url: str) -> str:
    return f"{http_base_url.rstrip('/')}/ghcnd-stations.txt"


@dataclass(frozen=True)
class Station:
    station_id: str
    latitude: float
    longitude: float
    elevation_m: float | None
    state: str | None
    name: str
    gsn_flag: bool
    hcn_crn_flag: str | None
    wmo_id: str | None


def parse_station_line(line: str) -> Station:
    """Parse one fixed-width line of ghcnd-stations.txt (column positions from NOAA's readme, section IV)."""
    if len(line) < 71:
        raise ValueError(f"station line too short ({len(line)} chars): {line!r}")
    elev = float(line[31:37])
    return Station(
        station_id=line[0:11].strip(),
        latitude=float(line[12:20]),
        longitude=float(line[21:30]),
        elevation_m=None if elev <= -999.0 else elev,
        state=line[38:40].strip() or None,
        name=line[41:71].strip(),
        gsn_flag=line[72:75].strip() == "GSN",
        hcn_crn_flag=line[76:79].strip() or None,
        wmo_id=line[80:85].strip() or None,
    )


def parse_stations(text: str, prefix: str | None = None) -> list[Station]:
    out: list[Station] = []
    for raw in text.splitlines():
        if not raw.strip():
            continue
        if prefix and not raw.startswith(prefix):
            continue
        out.append(parse_station_line(raw))
    return out

"""Per-run metrics, appended as JSON lines. The README's numbers are computed from this file."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class RunMetrics:
    run_id: str
    run_date: str
    window_start: str
    window_end: str
    station_prefix: str
    rows_extracted: int
    rows_loaded: int
    bytes_bronze: int
    extract_seconds: float
    load_seconds: float
    total_seconds: float
    stations_loaded: int | None = None
    recorded_at: str = ""

    def __post_init__(self) -> None:
        if not self.recorded_at:
            self.recorded_at = datetime.now(UTC).isoformat(timespec="seconds")


def append_metrics(path: Path, metrics: RunMetrics) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(metrics)) + "\n")


def read_metrics(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if line:
                out.append(json.loads(line))
    return out
